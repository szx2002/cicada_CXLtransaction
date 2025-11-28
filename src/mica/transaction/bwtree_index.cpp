#include "bwtree_index.h"
#include "mica/transaction/transaction.h"
#include "mica/transaction/db.h"
#include "mica/transaction/table.h"

namespace mica {
namespace transaction {

template <class StaticConfig, bool HasValue, class Key, class Compare>
BwTreeIndex<StaticConfig, HasValue, Key, Compare>::BwTreeIndex(
    DB<StaticConfig>* db, Table<StaticConfig>* main_tbl,
    Table<StaticConfig>* idx_tbl, const Compare& comp)
    : db_(db), main_tbl_(main_tbl), idx_tbl_(idx_tbl), comp_(comp),
      bwtree_(false, comp) {  // false表示不启动GC线程

  // 注册当前线程
  BwTreeType::RegisterThread();
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BwTreeIndex<StaticConfig, HasValue, Key, Compare>::init(Transaction* tx) {
  (void)tx;
  return true;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BwTreeIndex<StaticConfig, HasValue, Key, Compare>::insert(
    Transaction* tx, const Key& key, uint64_t value) {
  (void)tx;
  bool success = bwtree_.Insert(key, value);
  return success ? 0 : static_cast<uint64_t>(-1);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BwTreeIndex<StaticConfig, HasValue, Key, Compare>::remove(
    Transaction* tx, const Key& key, uint64_t value) {
  (void)tx;
  bool success = bwtree_.Delete(key, value);
  return success ? 0 : static_cast<uint64_t>(-1);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename Func>
uint64_t BwTreeIndex<StaticConfig, HasValue, Key, Compare>::lookup(
    Transaction* tx, const Key& key, bool skip_validation, const Func& func) {
  (void)tx;
  (void)skip_validation;

  std::vector<uint64_t> results;
  bwtree_.GetValue(key, results);

  for (const auto& value : results) {
    func(value);
  }

  return results.size();
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <BTreeRangeType LeftRangeType, BTreeRangeType RightRangeType, bool Reversed, typename Func>
uint64_t BwTreeIndex<StaticConfig, HasValue, Key, Compare>::lookup(
    Transaction* tx, const Key& min_key, const Key& max_key,
    bool skip_validation, const Func& func) {
  (void)tx;
  (void)skip_validation;
  (void)LeftRangeType;
  (void)RightRangeType;
  (void)Reversed;

  auto it = bwtree_.Begin(min_key);
  uint64_t count = 0;

  while (!it.IsEnd()) {
    auto key_value = *it;
    if (key_value.first > max_key) break;

    func(key_value.first, key_value.second);
    count++;
    it++;
  }

  return count;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
void BwTreeIndex<StaticConfig, HasValue, Key, Compare>::prefetch(
    Transaction* tx, const Key& key) {
  (void)tx;
  (void)key;
  // BwTree内部有自己的预取机制
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BwTreeIndex<StaticConfig, HasValue, Key, Compare>::check(Transaction* tx) const {
  (void)tx;
  // BwTree内部有一致性检查机制
  return true;
}

// 显式实例化常用类型
template class BwTreeIndex<StaticConfig, true, uint64_t, std::less<uint64_t>>;
template class BwTreeIndex<StaticConfig, true, std::string, std::less<std::string>>;

}  // namespace transaction
}  // namespace mica