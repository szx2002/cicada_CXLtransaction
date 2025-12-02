#pragma once
#ifndef MICA_TRANSACTION_CXL_TABLE_IMPL_H_
#define MICA_TRANSACTION_CXL_TABLE_IMPL_H_

#include "mica/transaction/cxl_table.h"
#include "mica/transaction/context.h"
#include "mica/transaction/db.h"

namespace mica {
namespace transaction {

// 构造函数实现
template <class StaticConfig>
CXLTable<StaticConfig>::CXLTable(DB<StaticConfig>* db, uint16_t cf_count,
                                const uint64_t* data_size_hints, uint8_t cxl_numa_node)
    : Table<StaticConfig>(db, cf_count, data_size_hints), cxl_numa_node_(cxl_numa_node) {
  printf("CXLTable initialized on NUMA node %u for CXL shared memory\n", cxl_numa_node_);
}

// 析构函数
template <class StaticConfig>
CXLTable<StaticConfig>::~CXLTable() {
  printf("CXLTable on NUMA node %u destroyed\n", cxl_numa_node_);
}

// CXL行分配实现
template <class StaticConfig>
bool CXLTable<StaticConfig>::allocate_cxl_rows(Context<StaticConfig>* ctx,
                                             std::vector<uint64_t>& row_ids) {
  if (StaticConfig::kCollectProcessingStats) ctx->stats().insert_row_count++;

  // 强制从CXL NUMA节点分配内存
  char* p = nullptr;

  // 直接使用CXL NUMA节点的PagePool分配
  p = this->db_->page_pool(cxl_numa_node_)->allocate();

  if (p == nullptr) {
    printf("failed to allocate CXL memory on NUMA node %u\n", cxl_numa_node_);
    return false;
  }

  printf("allocated CXL memory page on NUMA node %u for %zu rows\n",
         cxl_numa_node_, this->second_level_width_);

  // 初始化页面结构 - 与原Table相同
  for (uint64_t i = 0; i < this->second_level_width_; i++) {
    for (uint16_t cf_id = 0; cf_id < this->cf_count_; cf_id++) {
      auto& cf = this->cf_[cf_id];
      auto h = reinterpret_cast<RowHead<StaticConfig>*>(p + i * this->total_rh_size_ +
                                                        cf.rh_offset);
      h->older_rv = nullptr;

      if (StaticConfig::kInlinedRowVersion && cf.inlining) {
        auto inlined_rv = h->inlined_rv;
        inlined_rv->status = RowVersionStatus::kInvalid;
        inlined_rv->numa_id =
            RowVersion<StaticConfig>::kInlinedRowVersionNUMAID;
        inlined_rv->size_cls = cf.inlined_rv_size_cls;
      }
    }
  }

  // 获取表锁
  while (__sync_lock_test_and_set(&this->lock_, 1) == 1) ::mica::util::pause();

  // 分配行ID
  uint64_t row_id = this->row_count_;
  if ((row_id >> this->row_id_shift_) == this->kFirstLevelWidth) {
    printf("maximum CXL table size (%" PRIu64 " rows) reached\n",
           this->kFirstLevelWidth * this->second_level_width_);
    this->db_->page_pool(cxl_numa_node_)->free(p);
    __sync_lock_release(&this->lock_);
    return false;
  }

  // 注册新页面到CXL内存
  this->root_[row_id >> this->row_id_shift_] = p;
  this->page_numa_ids_[row_id >> this->row_id_shift_] = cxl_numa_node_;

  this->row_count_ += this->second_level_width_;

  // 释放表锁
  __sync_lock_release(&this->lock_);

  // 生成行ID列表
  for (uint64_t i = 0; i < this->second_level_width_; i++) {
    row_ids.push_back(row_id + this->second_level_width_ - 1 - i);
  }

  return true;
}

}
}
#endif