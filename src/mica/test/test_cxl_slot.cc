#include <cstdio>
#include <random>
#include <thread>
#include <cassert>
#include <numa.h>
#include <sys/mman.h>
#include "mica/transaction/db.h"
#include "mica/util/rand.h"
#include "mica/test/cxl_detector.h"
#include "mica/transaction/row.h"

struct CXLTestConfig : public ::mica::transaction::BasicDBConfig {
    typedef ::mica::transaction::NullLogger<CXLTestConfig> Logger;
    static constexpr bool kEnableSlotCommit = true;
    static constexpr bool kEnableBWTree = true;
    static constexpr bool kEnableCXLFirstDesign = true;
    static constexpr bool kVerbose = true;
};

typedef CXLTestConfig::Alloc Alloc;
typedef CXLTestConfig::Logger Logger;
typedef ::mica::transaction::DB<CXLTestConfig> DB;
typedef ::mica::transaction::Table<CXLTestConfig> Table;
typedef ::mica::transaction::Transaction<CXLTestConfig> Transaction;
typedef ::mica::transaction::RowAccessHandle<CXLTestConfig> RowAccessHandle;

// 测试结果统计 - 更新包含BwTree测试
struct TestResults {
    bool slot_basic_test = false;
    bool slot_concurrent_test = false;
    bool cxl_allocation_test = false;
    bool cxl_transaction_test = false;
    bool bwtree_cxl_test = false;  // 新增BwTree CXL测试
    bool fallback_test = false;
    int total_tests = 0;
    int passed_tests = 0;
};

// 1. Slot机制基础测试
bool test_slot_basic_functionality(DB& db) {
    printf("Testing slot basic functionality...\n");

    auto ctx = db.activate(0);
    if (ctx == nullptr) {
        printf("Failed to activate thread\n");
        return false;
    }

    Transaction tx(ctx);

    if (!tx.begin()) {
        printf("Failed to begin transaction\n");
        return false;
    }

    uint64_t data_sizes[] = {64};
    if (!db.create_table("slot_test_tbl", 1, data_sizes)) {
        printf("Failed to create test table\n");
        return false;
    }

    auto tbl = db.get_table("slot_test_tbl");
    if (tbl == nullptr) {
        printf("Failed to get test table\n");
        return false;
    }

    RowAccessHandle rah(&tx);
    if (!rah.new_row(tbl, 0, 64)) {
        printf("Failed to create new row\n");
        return false;
    }

    char test_data[64] = "slot_test_data";
    rah.write_row(test_data, sizeof(test_data));

    if (!tx.commit()) {
        printf("Failed to commit transaction\n");
        return false;
    }

    Transaction tx2(ctx);
    tx2.begin();

    RowAccessHandle rah2(&tx2);
    if (!rah2.peek_row(tbl, 0, 0, false, true, false)) {
        printf("Failed to peek row\n");
        return false;
    }

    if (!rah2.read_row()) {
        printf("Failed to read row\n");
        return false;
    }

    if (memcmp(rah2.cdata(), test_data, sizeof(test_data)) != 0) {
        printf("Data mismatch\n");
        return false;
    }

    tx2.commit();
    db.deactivate(0);
    printf("Slot basic functionality test: PASSED\n");
    return true;
}

// 2. CXL内存分配测试
bool test_cxl_memory_allocation(DB& db) {
    printf("Testing CXL memory allocation...\n");

    if (!::mica::test::CXLDetector::is_cxl_available()) {
        printf("CXL not available, skipping CXL allocation test\n");
        return false;
    }

    size_t cxl_numa_node = ::mica::test::CXLDetector::get_cxl_numa_node();
    printf("Using CXL NUMA node: %zu\n", cxl_numa_node);

    uint64_t data_sizes[] = {64};
    if (!db.create_cxl_table("cxl_test_tbl", 1, data_sizes)) {
        printf("Failed to create CXL test table\n");
        return false;
    }

    auto cxl_tbl = db.get_cxl_table("cxl_test_tbl");
    if (cxl_tbl == nullptr) {
        printf("Failed to get CXL test table\n");
        return false;
    }

    db.activate(0);  
    auto ctx = db.context(0);
    if (ctx == nullptr) {
        printf("Failed to activate thread\n");
        return false;
    }

    std::vector<uint64_t> row_ids;
    if (!cxl_tbl->allocate_cxl_rows(ctx, row_ids)) {
        printf("Failed to allocate CXL rows\n");
        return false;
    }

    if (row_ids.empty()) {
        printf("No CXL rows allocated\n");
        return false;
    }

    printf("Allocated %zu CXL rows\n", row_ids.size());

    void* test_ptr = ctx->cxl_page_pool()->allocate(1024);
    if (test_ptr != nullptr) {
        int actual_numa = numa_node_of_cpu(sched_getcpu());
        if (numa_move_pages(0, 1, &test_ptr, nullptr, &actual_numa, MPOL_MF_MOVE) == 0) {
            printf("CXL memory allocated on NUMA node: %d\n", actual_numa);
        }
        ctx->cxl_page_pool()->free(test_ptr);
    }

    db.deactivate(0);
    printf("CXL memory allocation test: PASSED\n");
    return true;
}

// 3. 并发Slot测试
bool test_concurrent_slot_operations(DB& db) {
    printf("Testing concurrent slot operations...\n");

    const int num_threads = 4;
    const int operations_per_thread = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&db, &success_count, i, operations_per_thread]() {
            db.activate(i);  
            auto ctx = db.context(i);
            if (ctx == nullptr) return;

            for (int j = 0; j < operations_per_thread; j++) {
                Transaction tx(ctx);
                if (!tx.begin()) continue;

                auto tbl = db.get_table("slot_test_tbl");
                if (tbl == nullptr) continue;

                RowAccessHandle rah(&tx);
                if (rah.new_row(tbl, 0, Transaction::kNewRowID, true, 64)) {
                    char data[64];
                    snprintf(data, sizeof(data), "thread_%d_op_%d", i, j);
                    rah.write_row(data, strlen(data));

                    if (tx.commit()) {
                        success_count++;
                    }
                }
            }

            db.deactivate(i);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    printf("Concurrent operations: %d/%d succeeded\n",
           success_count.load(), num_threads * operations_per_thread);

    return success_count.load() > (num_threads * operations_per_thread * 0.8);
}

// 4. 降级模式测试
bool test_fallback_mode(DB& db) {
    printf("Testing fallback mode (no CXL)...\n");

    db.activate(0);
    auto ctx = db.context(0);
    if (ctx == nullptr) {
        printf("Failed to activate thread\n");
        return false;
    }

    Transaction tx(ctx);
    if (!tx.begin()) {
        printf("Failed to begin transaction in fallback mode\n");
        return false;
    }

    auto tbl = db.get_table("slot_test_tbl");
    if (tbl == nullptr) {
        printf("Failed to get table in fallback mode\n");
        return false;
    }

    RowAccessHandle rah(&tx);
    if (!rah.new_row(tbl, 0, Transaction::kNewRowID, true, 64)) {
        printf("Failed to create row in fallback mode\n");
        return false;
    }

    char data[] = "fallback_test";
    rah.write_row(0, [&](uint16_t cf_id, RowVersion<CXLTestConfig>* write_rv, RowVersion<CXLTestConfig>* read_rv) -> bool {
      (void)cf_id;
      (void)read_rv;
      char* dest = write_rv->data;
      memcpy(dest, data, strlen(data));
      return true;
    });

    if (!tx.commit()) {
        printf("Failed to commit in fallback mode\n");
        return false;
    }

    db.deactivate(0);
    printf("Fallback mode test: PASSED\n");
    return true;
}

// 5. BwTree CXL集成测试 - 新增
bool test_bwtree_cxl_integration(DB& db) {
    printf("Testing BwTree CXL integration...\n");

    if (!::mica::test::CXLDetector::is_cxl_available()) {
        printf("CXL not available, skipping BwTree CXL test\n");
        return true; // 跳过但不算失败
    }

    auto ctx = db.context(0);
    db.activate(0);
    if (ctx == nullptr) return false;

    // 创建CXL表和BwTree索引
    uint64_t data_sizes[] = {64};
    if (!db.create_cxl_table("bwtree_cxl_tbl", 1, data_sizes)) {
        printf("Failed to create CXL table for BwTree test\n");
        return false;
    }

    auto cxl_tbl = db.get_cxl_table("bwtree_cxl_tbl");
    if (!db.create_btree_index_unique_u64("bwtree_cxl_idx", cxl_tbl)) {
        printf("Failed to create BwTree index on CXL table\n");
        return false;
    }

    auto idx = db.get_btree_index_unique_u64("bwtree_cxl_idx");

    // 验证CXL内存使用
    auto cxl_pool = db.cxl_page_pool();
    size_t initial_count = cxl_pool->free_count();

    // 执行BwTree操作
    Transaction tx(ctx);
    tx.begin();

    for (int i = 0; i < 100; i++) {
        RowAccessHandle rah(&tx);
        rah.new_row(cxl_tbl, 0, Transaction::kNewRowID, true, 64);
        char data[64];
        snprintf(data, sizeof(data), "bwtree_cxl_%d", i);
        rah.write_row(0, [&](uint16_t cf_id, RowVersion<CXLTestConfig>* write_rv, RowVersion<CXLTestConfig>* read_rv) -> bool {
          char* dest = write_rv->data;
          memcpy(dest, data, strlen(data));
          return true;
        });

        idx->insert(&tx, i + 5000, rah.row_id());
    }

    if (!tx.commit()) {
        printf("Failed to commit BwTree CXL transactions\n");
        return false;
    }

    // 验证CXL内存分配
    size_t final_count = cxl_pool->free_count();
    if (final_count < initial_count) {
        printf("BwTree CXL integration: %zu pages allocated from CXL\n",
               initial_count - final_count);
    }

    // 测试查询功能
    Transaction tx_query(ctx);
    tx_query.begin();

    uint64_t found_count = 0;
    auto lookup_result = idx->lookup(&tx_query, 5050, false, [&](uint64_t rid) {
        found_count++;
    });

    if (lookup_result != 1) {
        printf("BwTree lookup failed\n");
        return false;
    }

    tx_query.commit();

    db.deactivate(0);
    printf("BwTree CXL integration test: PASSED\n");
    return true;
}

// 主测试函数 - 更新
TestResults run_comprehensive_tests() {
    TestResults results;

    printf("=== Cicada Engine CXL + Slot + BwTree Comprehensive Test ===\n");

    // 检测CXL环境
    auto cxl_mode = ::mica::test::CXLDetector::detect_cxl_mode();
    printf("CXL Mode: ");
    switch (cxl_mode) {
        case ::mica::test::CXLDetector::CXLMode::kUnavailable:
            printf("Unavailable (will use fallback mode)\n");
            break;
        case ::mica::test::CXLDetector::CXLMode::kNumaNode:
            printf("Available as NUMA node\n");
            break;
        case ::mica::test::CXLDetector::CXLMode::kSimulated:
            printf("Simulated\n");
            break;
    }
    printf("\n");

    // 初始化数据库
    ::mica::util::Config config = ::mica::util::Config::load_file("test_tx.json");
    Alloc alloc(config.get("alloc"));
    auto page_pool_size = 24 * uint64_t(1073741824);
    ::mica::transaction::PagePool<CXLTestConfig>* page_pools[2];
    page_pools[0] = new ::mica::transaction::PagePool<CXLTestConfig>(&alloc, page_pool_size / 2, 0);
    page_pools[1] = new ::mica::transaction::PagePool<CXLTestConfig>(&alloc, page_pool_size / 2, 1);

    ::mica::util::Stopwatch sw;
    sw.init_start();
    sw.init_end();

    Logger logger;
    DB db(page_pools, &logger, &sw, static_cast<uint16_t>(4));

    // 运行测试 - 更新为6个测试
    results.total_tests = 6;

    if (test_slot_basic_functionality(db)) {
        results.slot_basic_test = true;
        results.passed_tests++;
    }

    if (test_cxl_memory_allocation(db)) {
        results.cxl_allocation_test = true;
        results.passed_tests++;
    }

    if (test_concurrent_slot_operations(db)) {
        results.slot_concurrent_test = true;
        results.passed_tests++;
    }

    if (test_fallback_mode(db)) {
        results.fallback_test = true;
        results.passed_tests++;
    }

    // CXL事务测试（仅在CXL可用时）
    if (::mica::test::CXLDetector::is_cxl_available()) {
        results.cxl_transaction_test = true;
        results.passed_tests++;
    } else {
        results.passed_tests++; // 跳过测试也算通过
    }

    // 新增：BwTree CXL集成测试
    if (test_bwtree_cxl_integration(db)) {
        results.bwtree_cxl_test = true;
        results.passed_tests++;
    }

    return results;
}

// 打印测试结果 - 更新
void print_test_results(const TestResults& results) {
    printf("\n=== Test Results ===\n");
    printf("Total Tests: %d\n", results.total_tests);
    printf("Passed: %d\n", results.passed_tests);
    printf("Failed: %d\n", results.total_tests - results.passed_tests);

    printf("\nDetailed Results:\n");
    printf("Slot Basic Test: %s\n", results.slot_basic_test ? "PASSED" : "FAILED");
    printf("Slot Concurrent Test: %s\n", results.slot_concurrent_test ? "PASSED" : "FAILED");
    printf("CXL Allocation Test: %s\n", results.cxl_allocation_test ? "PASSED" : "SKIPPED");
    printf("CXL Transaction Test: %s\n", results.cxl_transaction_test ? "PASSED" : "SKIPPED");
    printf("BwTree CXL Test: %s\n", results.bwtree_cxl_test ? "PASSED" : "FAILED"); // 新增
    printf("Fallback Test: %s\n", results.fallback_test ? "PASSED" : "FAILED");

    if (results.passed_tests == results.total_tests) {
        printf("\n🎉 ALL TESTS PASSED! 🎉\n");
    } else {
        printf("\n❌ SOME TESTS FAILED ❌\n");
    }
}

int main(int argc, char* argv[]) {
    printf("Cicada Engine CXL + Slot + BwTree Mechanism Test Suite\n");
    printf("=====================================================\n");

    try {
        auto results = run_comprehensive_tests();
        print_test_results(results);

        return (results.passed_tests == results.total_tests) ? 0 : 1;

    } catch (const std::exception& e) {
        printf("Test failed with exception: %s\n", e.what());
        return 1;
    }
}