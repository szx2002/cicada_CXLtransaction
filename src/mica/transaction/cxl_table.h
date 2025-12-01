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

 private:
  uint8_t cxl_numa_node_;
};

}
}

#endif