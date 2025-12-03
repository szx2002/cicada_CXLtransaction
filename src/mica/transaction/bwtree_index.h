#pragma once
#ifndef MICA_TRANSACTION_BWTREE_INDEX_H_
#define MICA_TRANSACTION_BWTREE_INDEX_H_

#include "mica/common.h"
#include "mica/util/type_traits.h"
#include "mica/transaction/bwtree_index_impl/bwtree.h"

namespace mica {
namespace transaction {

// 范围查询类型枚举
enum class BTreeRangeType {
  kOpen = 0,
  kInclusive,
  kExclusive,
};

// 前向声明
template <typename StaticConfig> class Transaction;
template <typename StaticConfig> class DB;
template <typename StaticConfig> class Table;
template <typename StaticConfig> class RowAccessHandle;
template <typename StaticConfig> class RowAccessHandlePeekOnly;

// 新增：CXL内存分配器
template <class StaticConfig>
class CXLBwTreeAllocator {
public:
    DB<StaticConfig>* db_;

    CXLBwTreeAllocator(DB<StaticConfig>* db) : db_(db) {}

    template<typename T>
    T* allocate(size_t count) {
        auto cxl_pool = db_->cxl_page_pool();
        char* p = cxl_pool->allocate();
        return reinterpret_cast<T*>(p);
    }

    template<typename T>
    void deallocate(T* ptr, size_t count) {
        auto cxl_pool = db_->cxl_page_pool();
        cxl_pool->free(reinterpret_cast<char*>(ptr));
    }
};

// 新增：索引项数据结构，包含slot信息
struct BwTreeIndexEntry {
    uint64_t row_id;                    // 指向主表的行ID
    uint16_t writer_thread_id;          // 写入线程ID
    uint16_t slot_idx;                  // 关联的slot索引
    uint64_t writer_local_seq;          // 写入事务的本地序列号

    // 构造函数
    BwTreeIndexEntry(uint64_t rid, uint16_t tid, uint16_t sidx, uint64_t seq)
        : row_id(rid), writer_thread_id(tid), slot_idx(sidx), writer_local_seq(seq) {}

    // 默认构造函数（用于BwTree内部）
    BwTreeIndexEntry() : row_id(0), writer_thread_id(0), slot_idx(0), writer_local_seq(0) {}

    // 删除标记检测
    bool is_delete_marker() const {
        return row_id & 0x8000000000000000ULL;
    }

    // 获取实际row_id（去除删除标记）
    uint64_t get_actual_row_id() const {
        return row_id & ~0x8000000000000000ULL;
    }
};

template <class StaticConfig, bool HasValue, class Key, class Compare = std::less<Key>>
class BwTreeIndex {
 public:
  // 修改：使用BwTreeIndexEntry作为值类型
  using BwTreeType = wangziqi2013::bwtree::BwTree<  
    Key, BwTreeIndexEntry, Compare, std::equal_to<Key>, std::hash<Key>,  
    std::equal_to<BwTreeIndexEntry>, std::hash<BwTreeIndexEntry>,  
    CXLBwTreeAllocator<StaticConfig>>;

  typedef typename StaticConfig::Timing Timing;
  typedef ::mica::transaction::RowAccessHandle<StaticConfig> RowAccessHandle;
  typedef ::mica::transaction::RowAccessHandlePeekOnly<StaticConfig> RowAccessHandlePeekOnly;
  typedef ::mica::transaction::Transaction<StaticConfig> Transaction;

  static constexpr uint64_t kHaveToAbort = static_cast<uint64_t>(-1);

  BwTreeIndex(DB<StaticConfig>* db, Table<StaticConfig>* main_tbl,
              Table<StaticConfig>* idx_tbl, const Compare& comp = Compare());

  bool init(Transaction* tx);

  uint64_t insert(Transaction* tx, const Key& key, uint64_t value);
  uint64_t remove(Transaction* tx, const Key& key, uint64_t value);

  template <typename Func>
  uint64_t lookup(Transaction* tx, const Key& key, bool skip_validation, const Func& func);

  template <BTreeRangeType LeftRangeType, BTreeRangeType RightRangeType, bool Reversed, typename Func>
  uint64_t lookup(Transaction* tx, const Key& min_key, const Key& max_key,
                  bool skip_validation, const Func& func);

  void prefetch(Transaction* tx, const Key& key);
  bool check(Transaction* tx) const;

  Table<StaticConfig>* main_table() { return main_tbl_; }
  const Table<StaticConfig>* main_table() const { return main_tbl_; }
  Table<StaticConfig>* index_table() { return idx_tbl_; }
  const Table<StaticConfig>* index_table() const { return idx_tbl_; }

 private:
  // 新增：事务可见性判断函数
  bool is_entry_visible(const BwTreeIndexEntry& entry, const Transaction* tx) const;

  DB<StaticConfig>* db_;
  Table<StaticConfig>* main_tbl_;
  Table<StaticConfig>* idx_tbl_;
  Compare comp_;
  BwTreeType bwtree_;
};

}  // namespace transaction
}  // namespace mica

#endif