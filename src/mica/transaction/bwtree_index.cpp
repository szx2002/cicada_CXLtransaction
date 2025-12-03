#include "mica/transaction/bwtree_index.h"
#include "mica/transaction/context.h"
#include "mica/transaction/transaction.h"

namespace mica {
namespace transaction {

template <class StaticConfig, bool HasValue, class Key, class Compare>
BwTreeIndex<StaticConfig, HasValue, Key, Compare>::BwTreeIndex(
    DB<StaticConfig>* db, Table<StaticConfig>* main_tbl,
    Table<StaticConfig>* idx_tbl, const Compare& comp)
    : db_(db), main_tbl_(main_tbl), idx_tbl_(idx_tbl), comp_(comp),
      bwtree_(CXLBwTreeAllocator<StaticConfig>(db)) {

  // 注册当前线程到BwTree系统
  bwtree_.UpdateThreadLocal(1);
  bwtree_.UpdateGCThread(0);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BwTreeIndex<StaticConfig, HasValue, Key, Compare>::init(Transaction* tx) {
  if (!tx) return false;

  // 事务性初始化：创建索引表的元数据
  RowAccessHandle rah(tx);
  if (!rah.new_row(idx_tbl_, 0, sizeof(uint64_t))) {
    return false;
  }

  uint64_t meta = 0;  // 索引元数据
  rah.write_row(&meta, sizeof(meta));
  return true;
}

// 新增：事务可见性判断实现
template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BwTreeIndex<StaticConfig, HasValue, Key, Compare>::is_entry_visible(
    const BwTreeIndexEntry& entry, const Transaction* tx) const {

    if (!tx) return false;

    // 获取写入事务的Context和Slot
    auto writer_ctx = tx->context()->db()->context(entry.writer_thread_id);
    if (!writer_ctx) return false;

    auto& slot = writer_ctx->get_slot(entry.slot_idx);

    // ABA检测：slot是否被复用
    if (slot.local_tx_seq != entry.writer_local_seq) {
        return false;  // 旧事务的索引项，不可见
    }

    // 检查slot状态
    if (slot.state != CommitSlotState::kCommitted) {
        return false;  // 未提交或已中止，不可见
    }

    // 检查提交时间戳（事务可见性）
    if (slot.commit_ts >= tx->ts()) {
        return false;  // 未来事务的提交，当前事务不可见
    }

    return true;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BwTreeIndex<StaticConfig, HasValue, Key, Compare>::insert(
    Transaction* tx, const Key& key, uint64_t value) {

    if (!tx || tx->is_peek_only()) {
        return static_cast<uint64_t>(Result::kError);
    }

    // 获取当前事务的slot信息
    auto ctx = tx->context();
    uint32_t slot_idx = tx->current_slot_idx();
    uint64_t local_seq = tx->current_local_seq();

    // 创建包含slot信息的索引项
    BwTreeIndexEntry entry(value, static_cast<uint16_t>(ctx->thread_id()),
                          static_cast<uint16_t>(slot_idx), local_seq);

    // 插入到BwTree中（此时还不可见，需要事务提交）
    bool success = bwtree_.Insert(key, entry);
    if (!success) {
        return static_cast<uint64_t>(Result::kError);
    }

    // 索引项将在事务提交时通过slot状态变为可见
    return static_cast<uint64_t>(Result::kSuccess);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
uint64_t BwTreeIndex<StaticConfig, HasValue, Key, Compare>::remove(
    Transaction* tx, const Key& key, uint64_t value) {

    if (!tx || tx->is_peek_only()) {
        return static_cast<uint64_t>(Result::kError);
    }

    // 删除操作通过插入特殊的"删除标记"索引项实现
    auto ctx = tx->context();
    uint32_t slot_idx = tx->current_slot_idx();
    uint64_t local_seq = tx->current_local_seq();

    // 使用最高位标记删除（row_id | 0x8000000000000000）
    uint64_t delete_marker = value | 0x8000000000000000ULL;
    BwTreeIndexEntry entry(delete_marker, static_cast<uint16_t>(ctx->thread_id()),
                          static_cast<uint16_t>(slot_idx), local_seq);

    bool success = bwtree_.Insert(key, entry);
    return success ? static_cast<uint64_t>(Result::kSuccess)
                   : static_cast<uint64_t>(Result::kError);
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <typename Func>
uint64_t BwTreeIndex<StaticConfig, HasValue, Key, Compare>::lookup(
    Transaction* tx, const Key& key, bool skip_validation, const Func& func) {

    if (!tx) {
        return static_cast<uint64_t>(Result::kError);
    }

    uint64_t count = 0;

    // BwTree的Value是BwTreeIndexEntry，需要过滤可见性
    std::vector<BwTreeIndexEntry> results;
    bwtree_.GetValue(key, results);

    for (const auto& entry : results) {
        // 检查索引项的可见性
        if (!skip_validation && !is_entry_visible(entry, tx)) {
            continue;  // 跳过不可见项
        }

        // 检查是否为删除标记
        if (entry.is_delete_marker()) {
            continue; // 跳过已删除项
        }

        // 调用回调函数处理可见的索引项
        func(entry.get_actual_row_id());
        count++;
    }

    return count;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
template <BTreeRangeType LeftRangeType, BTreeRangeType RightRangeType, bool Reversed, typename Func>
uint64_t BwTreeIndex<StaticConfig, HasValue, Key, Compare>::lookup(
    Transaction* tx, const Key& min_key, const Key& max_key,
    bool skip_validation, const Func& func) {

    if (!tx) {
        return static_cast<uint64_t>(Result::kError);
    }

    uint64_t count = 0;
    auto begin_iter = bwtree_.Begin(min_key);
    auto end_iter = bwtree_.End(max_key);

    for (auto it = begin_iter; it != end_iter; ++it) {
        const auto& entry = it.second;

        // 应用可见性过滤
        if (!skip_validation && !is_entry_visible(entry, tx)) {
            continue;
        }

        // 检查删除标记
        if (entry.is_delete_marker()) {
            continue;
        }

        // 边界条件检查
        if (LeftRangeType == BTreeRangeType::kExclusive && it.first <= min_key) {
            continue;
        }
        if (RightRangeType == BTreeRangeType::kExclusive && it.first >= max_key) {
            break;
        }

        func(it.first, entry.get_actual_row_id());
        count++;
    }

    return count;
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
void BwTreeIndex<StaticConfig, HasValue, Key, Compare>::prefetch(
    Transaction* tx, const Key& key) {
    if (!tx) return;

    // 基于key的预取实现
    bwtree_.GetValue(key, [](const BwTreeIndexEntry&) {
        // 预取回调，实际不处理数据
    });
}

template <class StaticConfig, bool HasValue, class Key, class Compare>
bool BwTreeIndex<StaticConfig, HasValue, Key, Compare>::check(Transaction* tx) const {
    if (!tx) return false;

    // 简化的一致性检查
    // 实际实现中可以遍历BwTree检查结构完整性
    return true;
}

// 显式实例化常用类型
template class BwTreeIndex<StaticConfig, true, uint64_t, std::less<uint64_t>>;
template class BwTreeIndex<StaticConfig, true, std::string, std::less<std::string>>;

}  // namespace transaction
}  // namespace mica