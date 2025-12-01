#pragma once
#ifndef MICA_TRANSACTION_CXL_TABLE_IMPL_H_
#define MICA_TRANSACTION_CXL_TABLE_IMPL_H_

#include "mica/transaction/cxl_table.h"

namespace mica {
namespace transaction {

template <class StaticConfig>
CXLTable<StaticConfig>::CXLTable(DB<StaticConfig>* db, uint16_t cf_count,
                                 const uint64_t* data_size_hints,
                                 uint8_t cxl_numa_node)
    : Table<StaticConfig>(db, cf_count, data_size_hints),
      cxl_numa_node_(cxl_numa_node) {
  printf("CXLTable created on NUMA node %u\n", cxl_numa_node_);
}

template <class StaticConfig>
CXLTable<StaticConfig>::~CXLTable() {
  printf("CXLTable destroyed\n");
}

}
}

#endif