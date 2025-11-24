#pragma once
#ifndef MICA_TRANSACTION_TRANSACTION_IMPL_OPERATION_H_
#define MICA_TRANSACTION_TRANSACTION_IMPL_OPERATION_H_

namespace mica {
namespace transaction {
template <class StaticConfig>
template <class DataCopier>
bool Transaction<StaticConfig>::new_row(RAH& rah, Table<StaticConfig>* tbl,
                                        uint16_t cf_id, uint64_t row_id,
                                        bool check_dup_access,
                                        uint64_t data_size,
                                        const DataCopier& data_copier) {
  assert(began_);

  assert(!peek_only_);

  // new_row() requires explicit data sizes.
  assert(data_size != kDefaultWriteDataSize);

  Timing t(ctx_->timing_stack(), &Stats::execution_write);

  // This rah must not be in use.
  if (rah) return false;

  if (cf_id == 0) {
    if (row_id != kNewRowID) return false;

    row_id = ctx_->allocate_row(tbl);
    if (row_id == static_cast<uint64_t>(-1)) {
      // TODO: Use different stats counter.
      if (StaticConfig::kCollectExtraCommitStats) {
        abort_reason_target_count_ = &ctx_->stats().aborted_by_get_row_count;
        abort_reason_target_time_ = &ctx_->stats().aborted_by_get_row_time;
      }
      return false;
    }
  } else {
    // Non-zero column family must supply a valid row ID.
    if (row_id == kNewRowID) return false;
  }

  auto head = tbl->head(cf_id, row_id);

  auto write_rv =
      ctx_->allocate_version_for_new_row(tbl, cf_id, row_id, head, data_size);
  if (write_rv == nullptr) {
    // Not enough memory.
    if (cf_id == 0) ctx_->deallocate_row(tbl, row_id);
    return false;
  }

  //新增:设置slot引用
  write_rv->writer_thread_id = ctx_->thread_id_;
  write_rv->slot_idx = static_cast<uint16_t>(current_slot_idx_);
  write_rv->writer_local_seq = current_local_seq_;
  //新增结束

  write_rv->older_rv = nullptr;
  write_rv->wts = ts_;
  write_rv->rts.init(ts_);
  write_rv->status = RowVersionStatus::kPending;

  if (!data_copier(cf_id, write_rv, nullptr)) {
    // Copy failed.
    ctx_->deallocate_version(write_rv);
    if (cf_id == 0) ctx_->deallocate_row(tbl, row_id);
    return false;
  }

  // Prefetch the whole row because it is typically written with new data.
  // {
  //   auto addr = reinterpret_cast<const char*>(write_rv);
  //   auto max_addr = addr + version_size;
  //   for (; addr < max_addr; addr += 64)
  //     __builtin_prefetch(reinterpret_cast<const void*>(addr), 1, 0);
  // }

  uint16_t bkt_id;
  AccessBucket* bkt;
  if (check_dup_access) {
    // TODO: Factor this out because it is used later again.
    if (access_bucket_count_ == 0) {
      for (size_t i = 0; i < StaticConfig::kAccessBucketRootCount; i++) {
        access_buckets_[i].count = 0;
        access_buckets_[i].next = AccessBucket::kEmptyBucketID;
      }
      access_bucket_count_ = StaticConfig::kAccessBucketRootCount;
    }

    bkt_id = (reinterpret_cast<size_t>(tbl) / 64 + row_id) %
             StaticConfig::kAccessBucketRootCount;
    bkt = &access_buckets_[bkt_id];
    while (true) {
      if (bkt->next == AccessBucket::kEmptyBucketID) break;
      bkt_id = bkt->next;
      bkt = &access_buckets_[bkt_id];
    }

    if (bkt->count == StaticConfig::kAccessBucketSize) {
      // Allocate a new acccess bucket if needed.
      auto new_bkt_id = access_bucket_count_++;
      if (access_buckets_.size() < access_bucket_count_)
        access_buckets_.resize(access_bucket_count_);
      auto new_bkt = &access_buckets_[new_bkt_id];
      new_bkt->count = 0;
      new_bkt->next = AccessBucket::kEmptyBucketID;

      // We must refresh bkt pointer because std::vector's resize() can move
      // the buffer.
      bkt = &access_buckets_[bkt_id];
      bkt->next = new_bkt_id;
      bkt = new_bkt;
    }
    bkt->idx[bkt->count++] = access_size_;
    // printf("check_dup %" PRIu64 "\n", row_id);
  }

  // assert(access_size_ < StaticConfig::kMaxAccessSize);
  if (access_size_ >= StaticConfig::kMaxAccessSize) {
    printf("too large access\n");
    assert(false);
  }
  iset_idx_[iset_size_++] = access_size_;
  rah.access_item_ = &accesses_[access_size_];
  accesses_[access_size_] = {access_size_, 0,     RowAccessState::kNew,
                             tbl,          cf_id, row_id,
                             head,         head,  write_rv,
                             nullptr /*, ts_*/};
  access_size_++;

  return true;
}

template <class StaticConfig>
void Transaction<StaticConfig>::prefetch_row(Table<StaticConfig>* tbl,
                                             uint16_t cf_id, uint64_t row_id,
                                             uint64_t off, uint64_t len) { //对数据进行预取减少后续正式访问数据时的cache miss
  assert(began_);

  assert(row_id < tbl->row_count());

  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  auto head = tbl->head(cf_id, row_id);
  __builtin_prefetch(head, 0, 0);

  if (StaticConfig::kInlinedRowVersion && tbl->inlining(cf_id) && len > 0) {
    size_t addr =
        (reinterpret_cast<size_t>(head->inlined_rv->data) + off) & ~size_t(63);
    if ((reinterpret_cast<size_t>(head) & ~size_t(63)) == addr) addr += 64;
    size_t max_addr =
        (reinterpret_cast<size_t>(head->inlined_rv->data) + off + len - 1) |
        size_t(63);
    for (; addr <= max_addr; addr += 64)
      __builtin_prefetch(reinterpret_cast<void*>(addr), 0, 0);
  }
}

template <class StaticConfig>
bool Transaction<StaticConfig>::peek_row(RAH& rah, Table<StaticConfig>* tbl,
                                         uint16_t cf_id, uint64_t row_id,
                                         bool check_dup_access, bool read_hint,
                                         bool write_hint) {
  assert(began_);
  if (rah) return false;

  assert(row_id < tbl->row_count());

  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  // Use an access item if it already exists.
  uint16_t bkt_id;
  AccessBucket* bkt;
  if (check_dup_access) { //重复访问检测
    if (access_bucket_count_ == 0) {
      for (size_t i = 0; i < StaticConfig::kAccessBucketRootCount; i++) {
        access_buckets_[i].count = 0;
        access_buckets_[i].next = AccessBucket::kEmptyBucketID;
      }
      access_bucket_count_ = StaticConfig::kAccessBucketRootCount;
    }

    bkt_id = (reinterpret_cast<size_t>(tbl) / 64 + row_id) %
             StaticConfig::kAccessBucketRootCount;
    bkt = &access_buckets_[bkt_id];
    while (true) {
      for (auto i = 0; i < bkt->count; i++) {
        auto item = &accesses_[bkt->idx[i]];
        if (item->row_id == row_id && item->tbl == tbl &&
            item->cf_id == cf_id) {
          rah.access_item_ = item;
          return true;
        }
      }
      if (bkt->next == AccessBucket::kEmptyBucketID) break;
      bkt_id = bkt->next;
      bkt = &access_buckets_[bkt_id];
    }
  }

  auto head = tbl->head(cf_id, row_id);// 获取行头
  if (StaticConfig::kInlinedRowVersion && StaticConfig::kInlineWithAltRow &&
      tbl->inlining(cf_id)) {
    auto alt_head = tbl->alt_head(cf_id, row_id);
    (void)alt_head;
  }
  RowCommon<StaticConfig>* newer_rv = head;
  auto rv = head->older_rv;
  // auto head_older = rv;
  // auto latest_wts = rv->wts;

  switch (static_cast<int>(read_hint) * 2 + static_cast<int>(write_hint)) { //根据不同策略查找版本链
    default:
    case 0:
      locate<false, false, false>(newer_rv, rv);
      break;
    case 1:
      locate<false, true, false>(newer_rv, rv);
      break;
    case 2:
      locate<true, false, false>(newer_rv, rv);
      break;
    case 3:
      locate<true, true, false>(newer_rv, rv);
      break;
  }

  if (rv == nullptr) {
    /*
#ifndef NDEBUG
    if (!write_hint) {
      // This usually should not happen; print some debugging information.

      print_version_chain(tbl, row_id);

      assert(ctx_->db_->min_rts() <= ts_);
    }
#endif
    */

    if (StaticConfig::kReserveAfterAbort)
      reserve(tbl, cf_id, row_id, read_hint, write_hint);

    if (StaticConfig::kCollectExtraCommitStats) {
      abort_reason_target_count_ = &ctx_->stats().aborted_by_get_row_count;
      abort_reason_target_time_ = &ctx_->stats().aborted_by_get_row_time;
    }
    return false;
  }

  // if (head_older != rv) using_latest_only_ = 0;

  // assert(access_size_ < StaticConfig::kMaxAccessSize);
  if (access_size_ >= StaticConfig::kMaxAccessSize) {
    printf("too large access\n");
    assert(false);
  }
  rah.access_item_ = &accesses_[access_size_]; //rah绑定访问记录

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
  if (check_dup_access) { //重复访问检测
    if (bkt->count == StaticConfig::kAccessBucketSize) {
      // Allocate a new acccess bucket if needed.
      auto new_bkt_id = access_bucket_count_++;
      if (access_buckets_.size() < access_bucket_count_)
        access_buckets_.resize(access_bucket_count_);
      auto new_bkt = &access_buckets_[new_bkt_id];
      new_bkt->count = 0;
      new_bkt->next = AccessBucket::kEmptyBucketID;

      // We must refresh bkt pointer because std::vector's resize() can move
      // the buffer.
      bkt = &access_buckets_[bkt_id];
      bkt->next = new_bkt_id;
      bkt = new_bkt;
    }
    bkt->idx[bkt->count++] = access_size_;
  }
#pragma GCC diagnostic pop

  accesses_[access_size_] = {access_size_,
                             0,
                             RowAccessState::kPeek,
                             tbl,
                             cf_id,
                             row_id,
                             head,
                             newer_rv,
                             nullptr,
                             rv /*, latest_wts */};
  access_size_++;

  return true;
}

template <class StaticConfig>
bool Transaction<StaticConfig>::peek_row(RAHPO& rah, Table<StaticConfig>* tbl,
                                         uint16_t cf_id, uint64_t row_id,
                                         bool check_dup_access) {
  assert(began_);
  if (rah) return false;

  assert(row_id < tbl->row_count());

  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  (void)check_dup_access;
  if (check_dup_access && access_bucket_count_ != 0) {
    uint16_t bkt_id;
    AccessBucket* bkt;

    bkt_id = (reinterpret_cast<size_t>(tbl) / 64 + row_id) %
             StaticConfig::kAccessBucketRootCount;
    bkt = &access_buckets_[bkt_id];
    while (true) {
      for (auto i = 0; i < bkt->count; i++) {
        auto item = &accesses_[bkt->idx[i]];
        if (item->row_id == row_id && item->tbl == tbl &&
            item->cf_id == cf_id) {
          rah.tbl_ = item->tbl;
          rah.cf_id_ = item->cf_id;
          rah.row_id_ = item->row_id;
          if (item->write_rv != nullptr)
            rah.read_rv_ = item->write_rv;
          else
            rah.read_rv_ = item->read_rv;
          return true;
        }
      }
      if (bkt->next == AccessBucket::kEmptyBucketID) break;
      bkt_id = bkt->next;
      bkt = &access_buckets_[bkt_id];
    }
  }

  auto head = tbl->head(cf_id, row_id);
  if (StaticConfig::kInlinedRowVersion && StaticConfig::kInlineWithAltRow &&
      tbl->inlining(cf_id)) {
    auto alt_head = tbl->alt_head(cf_id, row_id);
    (void)alt_head;
  }
  RowCommon<StaticConfig>* newer_rv = head;
  auto rv = head->older_rv;
  // auto head_older = rv;
  // auto latest_wts = rv->wts;

  locate<false, false, false>(newer_rv, rv);

  if (rv == nullptr) return false;

  rah.tbl_ = tbl;
  rah.cf_id_ = cf_id;
  rah.row_id_ = row_id;
  rah.read_rv_ = rv;

  return true;
}

template <class StaticConfig>
template <class DataCopier>
bool Transaction<StaticConfig>::read_row(RAH& rah,
                                         const DataCopier& data_copier) {
  assert(began_);
  if (!rah) return false;

  assert(!peek_only_);

  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  auto item = rah.access_item_; //peek_row中已经拿到的rah

  // New rows are readable by default.
  if (item->state == RowAccessState::kNew) return true;

  // OK to read twice.
  if (item->state == RowAccessState::kRead ||
      item->state == RowAccessState::kReadWrite)
    return true;
  if (item->state != RowAccessState::kPeek) return false;

  item->state = RowAccessState::kRead; //将item状态从peek升级为read 加入读集
  rset_idx_[rset_size_++] = item->i;

  if (StaticConfig::kInlinedRowVersion &&
      StaticConfig::kPromoteNonInlinedVersion &&
      item->tbl->inlining(item->cf_id)) { //若启用优化，将老数据放入inlined中
    if (!item->read_rv->is_inlined() &&
        // item->head->older_rv == item->read_rv &&
        item->read_rv->wts < ctx_->db_->min_rts() &&
        item->head->inlined_rv->status == RowVersionStatus::kInvalid) {
      // Promote a version if (1) it is a non-inlined version, (2) the inlined
      // version is not in use, (3) this non-inlined version was created for a
      // while ago.
      return write_row(rah, kDefaultWriteDataSize, data_copier);
    }
  }

  return true;
}

template <class StaticConfig>
template <class DataCopier>
bool Transaction<StaticConfig>::write_row(RAH& rah, uint64_t data_size,
                                          const DataCopier& data_copier) {
  assert(began_);
  if (!rah) return false;

  assert(!peek_only_);

  Timing t(ctx_->timing_stack(), &Stats::execution_write);

  auto item = rah.access_item_;

  // New rows are writable by default.
  if (item->state == RowAccessState::kNew) return true;

  // OK to write twice.
  if (item->state == RowAccessState::kWrite ||
      item->state == RowAccessState::kReadWrite)
    return true;

  if (item->state != RowAccessState::kPeek &&
      item->state != RowAccessState::kRead)
    return false;

  if (data_size == kDefaultWriteDataSize) data_size = item->read_rv->data_size;

  item->write_rv = ctx_->allocate_version_for_existing_row(
      item->tbl, item->cf_id, item->row_id, item->head, data_size);

  if (item->write_rv == nullptr) {
    if (StaticConfig::kCollectExtraCommitStats) {
      abort_reason_target_count_ = &ctx_->stats().aborted_by_get_row_count;
      abort_reason_target_time_ = &ctx_->stats().aborted_by_get_row_time;
    }
    return false;
  }

  //初始化版本信息
  //新增:设置slot引用
  item->write_rv->writer_thread_id = ctx_->thread_id_;
  item->write_rv->slot_idx = static_cast<uint16_t>(current_slot_idx_);
  item->write_rv->writer_local_seq = current_local_seq_;
  //新增结束

  item->write_rv->wts = ts_;
  item->write_rv->rts.init(ts_);
  item->write_rv->status = RowVersionStatus::kPending; //这里设置为pending，在commit最后修改为commited

  {
    Timing t(ctx_->timing_stack(), &Stats::row_copy); //将数据复制到新的write_rv中
    if (item->state == RowAccessState::kPeek) {
      if (!data_copier(item->cf_id, item->write_rv, nullptr)) return false;
      item->state = RowAccessState::kWrite;
    } else {
      if (!data_copier(item->cf_id, item->write_rv, item->read_rv))
        return false;
      item->state = RowAccessState::kReadWrite;
    }
  }

  wset_idx_[wset_size_++] = item->i;

  return true;
}

template <class StaticConfig>
bool Transaction<StaticConfig>::delete_row(RAH& rah) {
  assert(began_);
  assert(!peek_only_);

  Timing t(ctx_->timing_stack(), &Stats::execution_write);

  if (!rah) return false;

  auto item = rah.access_item_;

  switch (item->state) {
    case RowAccessState::kNew:
      item->state = RowAccessState::kInvalid;
      // Immediately deallocate the version (and the row for cf_id 0).
      ctx_->deallocate_version(item->write_rv);
      item->write_rv = nullptr;
      if (item->cf_id == 0) ctx_->deallocate_row(item->tbl, item->row_id);
      break;
    case RowAccessState::kWrite:
      item->state = RowAccessState::kDelete;
      break;
    case RowAccessState::kReadWrite:
      item->state = RowAccessState::kReadDelete;
      break;
    case RowAccessState::kDelete:
    case RowAccessState::kReadDelete:
    // Not OK to delete twice.
    // Fall through.
    default:
      return false;
  }

  rah.access_item_ = nullptr;

  return true;
}

template <class StaticConfig>
template <bool ForRead, bool ForWrite, bool ForValidation>
void Transaction<StaticConfig>::locate(RowCommon<StaticConfig>*& newer_rv,
                                       RowVersion<StaticConfig>*& rv) {
  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  uint64_t chain_len;
  if (StaticConfig::kCollectProcessingStats) chain_len = 0;

  while (true) { //遍历版本链，获取可见的最新版本
    // This usually should not happen because (1) a new row that can have no new
    // version is not visible unless someone has a dangling row ID (which is
    // rare), and (2) GC ensures that any transaction can find a committed row
    // version whose wts is smaller than that transaction's ts.
    if (rv == nullptr) {
#ifndef NDEBUG
      printf("Transaction:locate(): newer_rv=%p newer_rv->older_rv=%p rv=%p\n",
             newer_rv, newer_rv->older_rv, rv);
#endif
      return;
    }

    if (StaticConfig::kCollectProcessingStats) chain_len++;


    //新增:slot可见性检查(优先级更高)
    auto writer_ctx = ctx_->db_->context(rv->writer_thread_id);
    auto& slot = writer_ctx->get_slot(rv->slot_idx);

    if (slot.local_tx_seq != rv->writer_local_seq) {
      newer_rv = rv;
      rv = rv->older_rv;
      continue;
    }

    if (slot.state != CommitSlotState::kCommitted) {
      newer_rv = rv;
      rv = rv->older_rv;
      continue;
    }

    if (slot.commit_ts >= ts_) {
      newer_rv = rv;
      rv = rv->older_rv;
      continue;
    }

  //保留原有的可见性判断作为后备
  //原有代码暂时保持不变

    if (rv->wts < ts_) {
      RowVersionStatus status;
      if (StaticConfig::kNoWaitForPending) {
        status = rv->status;
        if ((!StaticConfig::kSkipPending || ForValidation) &&
            status == RowVersionStatus::kPending) {
          rv = nullptr;
          break;
        }
      } else
        status = wait_for_pending(rv);

      if (status == RowVersionStatus::kDeleted) {
        rv = nullptr;
        break;
      } else if (status == RowVersionStatus::kCommitted) {
        break;
      }
      assert((!StaticConfig::kNoWaitForPending &&
              status == RowVersionStatus::kAborted) ||
             StaticConfig::kNoWaitForPending);
    } else
      newer_rv = rv;

    if (StaticConfig::kInsertNewestVersionOnly && ForRead && ForWrite &&
        rv->status != RowVersionStatus::kAborted && rv->wts != ts_) {
      // printf("ts=%" PRIu64 " min_rts %" PRIu64 "\n", ts_.t2,
      //        ctx_->db_->min_rts().t2);
      // printf("rv=%p wts=%" PRIu64 " status=%d\n", rv, rv->wts.t2,
      //        static_cast<int>(rv->status));
      rv = nullptr;
      break;
    }

// if (rv->wts > ts_) newer_rv = rv;
#ifndef NDEBUG
    if (rv->older_rv == nullptr) {
      printf(
          "Transaction:locate(): newer_rv=%p newer_rv->older_rv=%p rv=%p "
          "rv->older_rv=%p\n",
          newer_rv, newer_rv->older_rv, rv, rv->older_rv);
      rv = nullptr;
      return;
    }
#endif
    rv = rv->older_rv;
  }

  if (ForWrite) { // rts>ts_tx验证
    // Someone have read this row, preventing this row from being overwritten.
    // Thus, abort this transaction.
    if (rv != nullptr && rv->rts.get() > ts_) rv = nullptr;
  }

  if (StaticConfig::kCollectProcessingStats) {
    if (ctx_->stats().max_read_chain_len < chain_len)
      ctx_->stats().max_read_chain_len = chain_len;
  }
}

template <class StaticConfig>
RowVersionStatus Transaction<StaticConfig>::wait_for_pending(
    RowVersion<StaticConfig>* rv) {
  if (StaticConfig::kNoWaitForPending) assert(false);

  Timing t(ctx_->timing_stack(), &Stats::wait_for_pending);

  auto status = rv->status;
  while (status == RowVersionStatus::kPending) {
    ::mica::util::pause();
    // usleep(1);
    status = rv->status;
  }
  return status;
}

template <class StaticConfig>
bool Transaction<StaticConfig>::insert_version_deferred() {
  for (auto j = 0; j < wset_size_; j++) { //遍历写集
    auto i = wset_idx_[j];
    auto item = &accesses_[i];
    assert(item->write_rv != nullptr);

    while (true) {
      auto rv = item->newer_rv->older_rv; //从newer_rv->older_rv出发，开始遍历目标行的版本链，查找插入点
      if (item->state == RowAccessState::kReadWrite ||
          item->state == RowAccessState::kReadDelete) { //如果是 ReadWrite，需要确认 read_rv 没被并发修改
        locate<true, true, false>(item->newer_rv, rv);
        // Read version changed; abort here without going to validation.
        if (rv != item->read_rv) {
          if (StaticConfig::kReserveAfterAbort)
            reserve(item->tbl, item->cf_id, item->row_id, true, true);
          return false; //读集检测不通过
        }
      } else { //没读过直接写
        assert(item->state == RowAccessState::kWrite ||
               item->state == RowAccessState::kDelete);
        locate<false, true, false>(item->newer_rv, rv);
        /*'''
        找到版本时间戳 ≤ 当前事务时间戳（rv->wts < ts_） 的最近版本；

        如果版本 status == kCommitted，就接受；

        如果版本 status == kPending：

        如果配置允许等待，会挂起；

        否则返回 nullptr（表示 abort）；
        '''*/
      }
      if (rv == nullptr) {  //没找到符合要求的版本
        if (StaticConfig::kReserveAfterAbort)
          reserve(item->tbl, item->cf_id, item->row_id, false, true);
        return false;
      }

      auto older_rv = item->newer_rv->older_rv;

      // It seems that newer_rv got a new older_rv node.  We need to find
      // the new value for rv.
      if (older_rv->wts > ts_) continue;

      item->write_rv->older_rv = older_rv;

      // auto actual_older_rv = __sync_val_compare_and_swap(
      //     &item->newer_rv->older_rv, older_rv, item->write_rv);
      //
      // // Found a newly inserted version that could be used as a read version.
      // if (older_rv != actual_older_rv) continue;
      if (!__sync_bool_compare_and_swap(&item->newer_rv->older_rv, older_rv,
                                        item->write_rv)) //CAS尝试把 RowHead 从 rv 替换为 write_rv
        continue;

      // Mark the write set item that this row version is visible.
      item->inserted = 1;

      if (rv->rts.get() > ts_) {
        // Oops, someone has updated rts just before the row insert.  We did
        // this checking earlier, but we can do this again to stop inserting
        // more stuff.
        if (StaticConfig::kReserveAfterAbort)
          reserve(item->tbl, item->cf_id, item->row_id,
                  item->state == RowAccessState::kReadWrite ||
                      item->state == RowAccessState::kReadDelete,
                  true);
        return false;
      }
      break;
    }
  }

  return true;
}

template <class StaticConfig>
void Transaction<StaticConfig>::insert_row_deferred() {
  for (auto j = 0; j < iset_size_; j++) {
    auto i = iset_idx_[j];
    auto item = &accesses_[i];

    if (item->state == RowAccessState::kInvalid) continue;

    assert(item->write_rv != nullptr);
    item->head->older_rv = item->write_rv;
    item->write_rv->status = RowVersionStatus::kCommitted; //将新版本标记为committed

    item->inserted = 1;
  }
}

template <class StaticConfig>
void Transaction<StaticConfig>::reserve(Table<StaticConfig>* tbl,
                                        uint16_t cf_id, uint64_t row_id,
                                        bool read_hint, bool write_hint) {
  assert(StaticConfig::kReserveAfterAbort);

  to_reserve_.emplace_back(tbl, cf_id, row_id, read_hint, write_hint);
  // to_reserve_.push_back({tbl, row_id, read_hint, write_hint});
}

template <class StaticConfig>
void Transaction<StaticConfig>::print_version_chain(
    const Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id) const {
  auto head = tbl->head(cf_id, row_id);
  auto rv = head->older_rv;

  printf("ts=%" PRIu64 " min_rts %" PRIu64 "\n", ts_.t2,
         ctx_->db_->min_rts().t2);
  while (rv != nullptr) {
    printf("rv=%p wts=%" PRIu64 " status=%d\n", rv, rv->wts.t2,
           static_cast<int>(rv->status));
    rv = rv->older_rv;
  }
  printf("rv=%p\n", rv);
}
}
}

#endif
