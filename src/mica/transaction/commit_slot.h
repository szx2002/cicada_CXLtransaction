#pragma once
#ifndef MICA_TRANSACTION_COMMIT_SLOT_H_
#define MICA_TRANSACTION_COMMIT_SLOT_H_

#include "mica/common.h"
#include "mica/transaction/timestamp.h"

namespace mica {
namespace transaction {

enum class CommitSlotState : uint8_t {
  kActive = 0,
  kCommitting,
  kCommitted,
  kAborted,
};

template <class StaticConfig>
struct CommitSlot {
  typedef typename StaticConfig::Timestamp Timestamp;

  uint64_t local_tx_seq;
  Timestamp start_ts;
  Timestamp commit_ts;
  volatile CommitSlotState state;

  CommitSlot()
    : local_tx_seq(0),
      start_ts(Timestamp::make(0, 0, 0)),
      commit_ts(Timestamp::make(0, 0, 0)),
      state(CommitSlotState::kActive) {}
} __attribute__((aligned(64)));

}
}

#endif