#pragma once
#ifndef MICA_TRANSACTION_CXL_TABLE_H_
#define MICA_TRANSACTION_CXL_TABLE_H_

#include "mica/transaction/table.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
class CXLTable : public Table<StaticConfig> {
 public:
  CXLTable(DB<StaticConfig>* db, uint16_t cf_count,
           const uint64_t* data_size_hints, uint8_t cxl_numa_node);
  ~CXLTable();

  // 重写内存分配方法，强制使用CXL NUMA节点
  bool allocate_cxl_rows(Context<StaticConfig>* ctx,
                         std::vector<uint64_t>& row_ids);

 private:
  uint8_t cxl_numa_node_;

  // 重写父类的内存分配逻辑
  char* allocate_cxl_page();
};
}
}
#endif