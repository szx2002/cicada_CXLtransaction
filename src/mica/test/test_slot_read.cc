#include <cstdio>
#include "mica/transaction/db.h"
#include "mica/transaction/transaction.h"

struct SimpleSlotTestConfig : public ::mica::transaction::BasicDBConfig {
    typedef ::mica::transaction::NullLogger<SimpleSlotTestConfig> Logger;
    static constexpr bool kEnableSlotCommit = true;
    static constexpr bool kVerbose = true;
};

typedef SimpleSlotTestConfig::Alloc Alloc;
typedef SimpleSlotTestConfig::Logger Logger;
typedef ::mica::transaction::DB<SimpleSlotTestConfig> DB;
typedef ::mica::transaction::Transaction<SimpleSlotTestConfig> Transaction;

bool test_commit_slot_access() {
    printf("=== Testing Commit Slot Access ===\n");

    // 创建最小配置
    ::mica::util::Config config = ::mica::util::Config::empty_dict("test");
    config.insert_dict("alloc", ::mica::util::Config::empty_dict("alloc"));
    Alloc alloc(config.get("alloc"));

    // 创建DB
    auto page_pool_size = 2 * uint64_t(1073741824);
    ::mica::transaction::PagePool<SimpleSlotTestConfig>* page_pools[1];
    page_pools[0] = new ::mica::transaction::PagePool<SimpleSlotTestConfig>(&alloc, page_pool_size, 0);

    ::mica::util::Stopwatch sw;
    sw.init_start();
    sw.init_end();

    Logger logger;
    DB db(page_pools, &logger, &sw, 1);

    // 激活线程
    db.activate(0);
    auto ctx = db.context(0);

    // 创建事务
    Transaction tx(ctx);

    // 开始事务（这会分配slot）
    if (!tx.begin()) {
        printf("Failed to begin transaction\n");
        return false;
    }

    // 通过context访问commit slot
    uint32_t slot_idx = tx.current_slot_index();
    auto& slot = ctx->get_slot(slot_idx);

    // 验证slot数据
    printf("Slot index: %u\n", slot_idx);
    printf("Slot state: %d\n", static_cast<int>(slot.state));
    printf("Slot start_ts: %" PRIu64 "\n", slot.start_ts.t2);
    printf("Slot commit_ts: %" PRIu64 "\n", slot.commit_ts.t2);

    // 提交事务（这会设置commit_ts）
    if (!tx.commit()) {
        printf("Failed to commit transaction\n");
        return false;
    }

    // 再次读取commit_ts
    printf("After commit:\n");
    printf("Slot state: %d\n", static_cast<int>(slot.state));
    printf("Slot commit_ts: %" PRIu64 "\n", slot.commit_ts.t2);

    db.deactivate(0);
    return true;
}

int main() {
    if (test_commit_slot_access()) {
        printf("✓ Commit slot access test PASSED\n");
        return 0;
    } else {
        printf("✗ Commit slot access test FAILED\n");
        return 1;
    }
}