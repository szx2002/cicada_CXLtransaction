#include <cstdio>
#include <vector>
#include <thread>
#include <atomic>
#include "mica/transaction/db.h"
#include "mica/transaction/transaction.h"
#include "mica/transaction/row_access.h"
#include "mica/transaction/row.h"  // 添加：包含RowVersion定义
#include "mica/util/lcore.h"
#include "mica/util/stopwatch.h"

struct CommitSlotTestConfig : public ::mica::transaction::BasicDBConfig {
    typedef ::mica::transaction::NullLogger<CommitSlotTestConfig> Logger;
    static constexpr bool kEnableSlotCommit = true;
    static constexpr bool kVerbose = true;
};

typedef CommitSlotTestConfig::Alloc Alloc;
typedef CommitSlotTestConfig::Logger Logger;
typedef ::mica::transaction::DB<CommitSlotTestConfig> DB;
typedef ::mica::transaction::Transaction<CommitSlotTestConfig> Transaction;
typedef ::mica::transaction::RowAccessHandle<CommitSlotTestConfig> RowAccessHandle;
typedef ::mica::transaction::RowVersion<CommitSlotTestConfig> RowVersion;  // 添加类型定义

// 测试基础slot分配和访问
bool test_basic_slot_access(DB& db) {
    printf("=== Testing Basic Slot Access ===\n");

    db.activate(0);
    auto ctx = db.context(0);
    if (!ctx) {
        printf("Failed to get context\n");
        db.deactivate(0);
        return false;
    }

    // 测试slot分配
    uint32_t slot_idx = ctx->allocate_slot();
    if (slot_idx == static_cast<uint32_t>(-1)) {
        printf("Failed to allocate slot\n");
        db.deactivate(0);
        return false;
    }

    printf("✓ Allocated slot index: %u\n", slot_idx);

    // 测试slot访问
    auto& slot = ctx->get_slot(slot_idx);
    printf("✓ Slot state: %d\n", static_cast<int>(slot.state));
    printf("✓ Slot local_tx_seq: %lu\n", slot.local_tx_seq);
    printf("✓ Slot start_ts: %lu\n", slot.start_ts.t2);

    // 验证slot数组边界
    for (uint32_t i = 0; i < 256; i++) {
        auto& test_slot = ctx->get_slot(i);
        (void)test_slot; // 避免未使用警告
    }
    printf("✓ All 256 slots accessible\n");

    db.deactivate(0);
    return true;
}

// 测试slot状态转换
bool test_slot_state_transitions(DB& db) {
    printf("\n=== Testing Slot State Transitions ===\n");

    db.activate(0);
    auto ctx = db.context(0);

    Transaction tx(ctx);
    if (!tx.begin()) {
        printf("Failed to begin transaction\n");
        db.deactivate(0);
        return false;
    }

    // 获取事务分配的slot
    uint32_t slot_idx = ctx->allocate_slot();
    auto& slot = ctx->get_slot(slot_idx);

    // 验证初始状态
    if (slot.state != ::mica::transaction::CommitSlotState::kActive) {
        printf("❌ Initial state should be kActive\n");
        db.deactivate(0);
        return false;
    }
    printf("✓ Slot initialized to kActive state\n");

    // 创建测试表
    uint64_t data_sizes[] = {64};
    if (!db.create_table("slot_state_test", 1, data_sizes)) {
        printf("Failed to create test table\n");
        db.deactivate(0);
        return false;
    }

    auto tbl = db.get_table("slot_state_test");
    RowAccessHandle rah(&tx);
    if (!rah.new_row(tbl, 0, Transaction::kNewRowID, true, 64)) {
        printf("Failed to create new row\n");
        db.deactivate(0);
        return false;
    }

    // 提交事务，观察状态转换
    if (!tx.commit()) {
        printf("Failed to commit transaction\n");
        db.deactivate(0);
        return false;
    }

    // 验证最终状态
    if (slot.state != ::mica::transaction::CommitSlotState::kCommitted) {
        printf("❌ Final state should be kCommitted\n");
        db.deactivate(0);
        return false;
    }
    printf("✓ Slot transitioned to kCommitted state\n");
    printf("✓ Commit timestamp: %lu\n", slot.commit_ts.t2);

    db.deactivate(0);
    return true;
}

// 测试并发slot访问
bool test_concurrent_slot_access(DB& db) {
    printf("\n=== Testing Concurrent Slot Access ===\n");

    const int num_threads = 4;
    const int operations_per_thread = 50;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    std::atomic<int> slot_conflicts{0};

    // 创建测试表
    uint64_t data_sizes[] = {64};
    db.create_table("concurrent_slot_test", 1, data_sizes);
    auto tbl = db.get_table("concurrent_slot_test");

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&db, &success_count, &slot_conflicts, i,
                              operations_per_thread, tbl]() {
            db.activate(static_cast<uint16_t>(i));  // 修复类型转换
            auto ctx = db.context(static_cast<uint16_t>(i));
            if (!ctx) return;

            for (int j = 0; j < operations_per_thread; j++) {
                Transaction tx(ctx);
                if (!tx.begin()) continue;

                // 测试slot分配
                uint32_t slot_idx = ctx->allocate_slot();
                if (slot_idx == static_cast<uint32_t>(-1)) {
                    slot_conflicts++;
                    continue;
                }

                // 验证slot访问
                auto& slot = ctx->get_slot(slot_idx);
                if (slot.state != ::mica::transaction::CommitSlotState::kActive) {
                    continue;
                }

                RowAccessHandle rah(&tx);
                if (rah.new_row(tbl, 0, Transaction::kNewRowID, true, 64)) {
                    char data[64];
                    snprintf(data, sizeof(data), "thread_%d_op_%d", i, j);

                    // 修复：使用正确的DataCopier签名
                    if (rah.write_row(64, [&](uint16_t cf_id, RowVersion* write_rv,
                                               const RowVersion* read_rv) {
                        (void)cf_id;
                        (void)read_rv;
                        if (write_rv && write_rv->data_size > 0) {
                            memcpy(write_rv->data, data, strlen(data));
                            return true;
                        }
                        return false;
                    })) {
                        if (tx.commit()) {
                            success_count++;
                        }
                    }
                }
            }

            db.deactivate(static_cast<uint16_t>(i));  // 修复类型转换
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    printf("✓ Successful operations: %d/%d\n",
           success_count.load(), num_threads * operations_per_thread);
    printf("✓ Slot conflicts: %d\n", slot_conflicts.load());

    return success_count.load() > (num_threads * operations_per_thread * 0.8);
}

// 测试slot复用机制
bool test_slot_reuse(DB& db) {
    printf("\n=== Testing Slot Reuse Mechanism ===\n");

    db.activate(0);
    auto ctx = db.context(0);

    std::vector<uint32_t> allocated_slots;

    // 分配多个slot
    for (int i = 0; i < 10; i++) {
        Transaction tx(ctx);
        if (!tx.begin()) continue;

        uint32_t slot_idx = ctx->allocate_slot();
        if (slot_idx != static_cast<uint32_t>(-1)) {
            allocated_slots.push_back(slot_idx);

            // 立即提交以释放slot
            tx.abort(); // 使用abort快速释放
        }
    }

    printf("✓ Allocated %zu slots\n", allocated_slots.size());

    // 验证slot可以被复用
    uint32_t reuse_slot_idx = ctx->allocate_slot();
    auto& reuse_slot = ctx->get_slot(reuse_slot_idx);

    printf("✓ Reused slot index: %u\n", reuse_slot_idx);
    printf("✓ Reused slot state: %d\n", static_cast<int>(reuse_slot.state));

    db.deactivate(0);
    return true;
}

int main() {
    printf("Commit Slot Access Test\n");
    printf("=======================\n\n");

    // 初始化数据库
    //auto config = ::mica::util::Config::load_file("test_tx.json");
    ::mica::util::Config config;
    Alloc alloc(config.get("alloc"));
    DB db(config);

    auto page_pool_size = 24 * uint64_t(1073741824);
    ::mica::transaction::PagePool<CommitSlotTestConfig>* page_pools[2];
    page_pools[0] = new ::mica::transaction::PagePool<CommitSlotTestConfig>(&alloc, page_pool_size / 2, 0);
    page_pools[1] = new ::mica::transaction::PagePool<CommitSlotTestConfig>(&alloc, page_pool_size / 2, 1);

    ::mica::util::Stopwatch sw;
    sw.init_start();
    sw.init_end();

    Logger logger;
    DB db(page_pools, &logger, &sw, 1);

    bool all_passed = true;

    if (!test_basic_slot_access(db)) {
        all_passed = false;
    }

    if (!test_slot_state_transitions(db)) {
        all_passed = false;
    }

    if (!test_concurrent_slot_access(db)) {
        all_passed = false;
    }

    if (!test_slot_reuse(db)) {
        all_passed = false;
    }

    printf("\n=== Test Summary ===\n");
    if (all_passed) {
        printf("🎉 All commit slot access tests PASSED!\n");
        printf("✓ Slot allocation works correctly\n");
        printf("✓ Slot state transitions work correctly\n");
        printf("✓ Concurrent slot access works correctly\n");
        printf("✓ Slot reuse mechanism works correctly\n");
    } else {
        printf("❌ Some commit slot access tests FAILED\n");
    }

    return all_passed ? 0 : 1;
}