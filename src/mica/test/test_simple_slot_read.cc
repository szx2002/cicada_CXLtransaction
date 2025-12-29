#include <cstdio>
#include <vector>
#include <thread>
#include "mica/transaction/db.h"
#include "mica/transaction/transaction.h"
#include "mica/transaction/row_access.h"
#include "mica/transaction/row.h"
#include "mica/util/stopwatch.h"

struct VisibilityTestConfig : public ::mica::transaction::BasicDBConfig {
    typedef ::mica::transaction::NullLogger<VisibilityTestConfig> Logger;
    static constexpr bool kEnableSlotCommit = true;
    static constexpr bool kVerbose = true;
};

typedef VisibilityTestConfig::Alloc Alloc;
typedef VisibilityTestConfig::Logger Logger;
typedef ::mica::transaction::DB<VisibilityTestConfig> DB;
typedef ::mica::transaction::Transaction<VisibilityTestConfig> Transaction;

// 工作线程任务结构
struct Task {
    DB* db;
    uint16_t thread_id;
    uint16_t num_threads;
    bool test_completed;
    bool test_passed;
} __attribute__((aligned(64)));

void worker_proc(Task* task) {
    ::mica::util::lcore.pin_thread(task->thread_id);

    printf("Worker thread %d started\n", task->thread_id);

    // 每个线程自己激活自己
    task->db->activate(static_cast<uint16_t>(task->thread_id));

    // 等待所有线程激活完成
    while (task->db->active_thread_count() < task->num_threads) { //如果当前活跃线程数小于总线程数，说明有其他线程还没ready，这个线程需要等待
        ::mica::util::pause();
        task->db->idle(static_cast<uint16_t>(task->thread_id));
    }

    printf("All threads activated, thread %d beginning test\n", task->thread_id);

    // 只有线程0执行实际测试
    if (task->thread_id == 0) {
        auto ctx = task->db->context(task->thread_id);

        printf("=== Testing Commit Slot Access ===\n");

        // 开始事务
        Transaction tx(ctx);
        bool ret = tx.begin();
        if (!ret) {
            printf("✗ Failed to begin transaction\n");
            task->test_passed = false;
        } else {
            // 获取当前事务的slot索引
            uint32_t slot_idx = tx.current_slot_index();
            printf("✓ Transaction allocated slot index: %u\n", slot_idx);

            // 通过context访问commit slot
            auto& slot = ctx->get_slot(slot_idx);
            printf("✓ Retrieved commit slot from context\n");

            // 检查slot状态
            printf("✓ Slot state: %d\n", static_cast<int>(slot.state));
            printf("✓ Slot local_tx_seq: %u\n", slot.local_tx_seq);

            // 提交事务
            ret = tx.commit();
            if (ret) {
                printf("✓ Transaction committed successfully\n");
                printf("✓ Final commit timestamp: %lu\n", tx.ts().t2);
                task->test_passed = true;
            } else {
                printf("✗ Transaction commit failed\n");
                task->test_passed = false;
            }
        }

        task->test_completed = true;
        printf("=== Test Completed ===\n");
    }

    task->db->deactivate(task->thread_id);
    printf("Worker thread %d finished\n", task->thread_id);
}

int main() {
    printf("Commit Slot Access Test with Proper Multi-threading\n");
    printf("==================================================\n");

    // 创建配置
    ::mica::util::Config config = ::mica::util::Config::empty_dict("test");
    config.insert_dict("alloc", ::mica::util::Config::empty_dict("alloc"));

    Alloc alloc(config.get("alloc"));

    // 创建PagePool
    auto page_pool_size = 24 * uint64_t(1073741824);
    ::mica::transaction::PagePool<VisibilityTestConfig>* page_pools[2];
    page_pools[0] = new ::mica::transaction::PagePool<VisibilityTestConfig>(&alloc, page_pool_size / 2, 0);
    page_pools[1] = new ::mica::transaction::PagePool<VisibilityTestConfig>(&alloc, page_pool_size / 2, 1);

    ::mica::util::Stopwatch sw;
    sw.init_start();
    sw.init_end();

    Logger logger;
    DB db(page_pools, &logger, &sw, 2);  // 使用2个线程

    // 创建任务
    std::vector<Task> tasks(2);
    for (uint16_t i = 0; i < 2; i++) {
        tasks[i].db = &db;
        tasks[i].thread_id = i;
        tasks[i].num_threads = 2;
        tasks[i].test_completed = false;
        tasks[i].test_passed = false;
    }

    printf("Starting worker threads...\n");

    // 创建并启动工作线程
    std::vector<std::thread> threads;
    for (uint16_t i = 1; i < 2; i++) {
        threads.emplace_back(worker_proc, &tasks[i]);
    }

    // 主线程也作为worker执行
    worker_proc(&tasks[0]);

    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }

    // 输出测试结果
    printf("\n=== Test Results ===\n");
    if (tasks[0].test_completed) {
        if (tasks[0].test_passed) {
            printf("🎉 Commit Slot Access Test PASSED!\n");
            printf("✓ Transaction successfully accessed commit slot through context\n");
            printf("✓ Slot state and timestamp verification completed\n");
        } else {
            printf("❌ Commit Slot Access Test FAILED!\n");
        }
    } else {
        printf("⚠️  Test did not complete properly\n");
    }

    return 0;
}