// bwtree_index.h
#pragma once
#ifndef MICA_TRANSACTION_BWTREE_INDEX_H_
#define MICA_TRANSACTION_BWTREE_INDEX_H_

#include "mica/common.h"
#include "mica/util/type_traits.h"
#include "mica/transaction/bwtree_index_impl/bwtree.h"

namespace mica {
namespace transaction {

// 添加缺失的枚举定义
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

template <class StaticConfig, bool HasValue, class Key, class Compare = std::less<Key>>
class BwTreeIndex {
 public:
  using BwTreeType = wangziqi2013::bwtree::BwTree<
    Key, uint64_t, Compare, std::equal_to<Key>, std::hash<Key>,
    std::equal_to<uint64_t>, std::hash<uint64_t>>;

  typedef typename StaticConfig::Timing Timing;
  typedef ::mica::transaction::RowAccessHandle<StaticConfig> RowAccessHandle;
  typedef ::mica::transaction::RowAccessHandlePeekOnly<StaticConfig> RowAccessHandlePeekOnly;
  typedef ::mica::transaction::Transaction<StaticConfig> Transaction;

  BwTreeIndex(DB<StaticConfig>* db, Table<StaticConfig>* main_tbl,
              Table<StaticConfig>* idx_tbl, const Compare& comp = Compare());

  bool init(Transaction* tx);

  uint64_t insert(Transaction* tx, const Key& key, uint64_t value);
  uint64_t remove(Transaction* tx, const Key& key, uint64_t value);

  template <typename Func>
  uint64_t lookup(Transaction* tx, const Key& key, bool skip_validation, const Func& func);

  // 修正范围查询的模板参数
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
  DB<StaticConfig>* db_;
  Table<StaticConfig>* main_tbl_;
  Table<StaticConfig>* idx_tbl_;
  Compare comp_;
  BwTreeType bwtree_;
};

}  // namespace transaction
}  // namespace mica

#endif