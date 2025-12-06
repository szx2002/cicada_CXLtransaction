#include <cstdio>
#include <random>
#include <thread>
#include <vector>
#include <cassert>
#include "mica/transaction/db.h"
#include "mica/util/rand.h"
#include "mica/test/cxl_detector.h"
#include "mica/transaction/row.h"

struct BwTreeTestConfig : public ::mica::transaction::BasicDBConfig {
    typedef ::mica::transaction::NullLogger<BwTreeTestConfig> Logger;
    static constexpr bool kEnableSlotCommit = true;
    static constexpr bool kEnableBWTree = true;  // 启用BwTree
    static constexpr bool kEnableCXLFirstDesign = true;
    static constexpr bool kVerbose = true;
};

typedef BwTreeTestConfig::Alloc Alloc;
typedef BwTreeTestConfig::Logger Logger;
typedef ::mica::transaction::DB<BwTreeTestConfig> DB;
typedef ::mica::transaction::Table<BwTreeTestConfig> Table;
typedef ::mica::transaction::Transaction<BwTreeTestConfig> Transaction;
typedef ::mica::transaction::RowAccessHandle<BwTreeTestConfig> RowAccessHandle;
typedef DB::BTreeIndexUniqueU64 BwTreeIndex;

// 测试结果统计
struct BwTreeTestResults {
    bool basic_insert_test = false;
    bool visibility_test = false;
    bool concurrent_test = false;
    bool range_query_test = false;
    bool delete_test = false;
    int total_tests = 0;
    int passed_tests = 0;
};

// 1. BwTree基础插入测试
bool test_bwtree_basic_insert(DB& db) {
    printf("Testing BwTree basic insert...\n");

    auto ctx = db.activate_thread(0);
    if (ctx == nullptr) {
        printf("Failed to activate thread\n");
        return false;
    }

    // 创建主表
    uint64_t data_sizes[] = {64};
    if (!db.create_table("bwtree_main_tbl", 1, data_sizes)) {
        printf("Failed to create main table\n");
        return false;
    }

    auto main_tbl = db.get_table("bwtree_main_tbl");
    if (main_tbl == nullptr) {
        printf("Failed to get main table\n");
        return false;
    }

    // 创建BwTree索引
    if (!db.create_btree_index_unique_u64("bwtree_test_idx", main_tbl)) {
        printf("Failed to create BwTree index\n");
        return false;
    }

    auto idx = db.get_btree_index_unique_u64("bwtree_test_idx");
    if (idx == nullptr) {
        printf("Failed to get BwTree index\n");
        return false;
    }

    Transaction tx(ctx);
    if (!tx.begin()) {
        printf("Failed to begin transaction\n");
        return false;
    }

    // 插入主表数据
    RowAccessHandle rah(&tx);
    if (!rah.new_row(main_tbl, 0, 64)) {
        printf("Failed to create new row\n");
        return false;
    }

    char test_data[64] = "bwtree_test_data";
    rah.write_row(test_data, sizeof(test_data));
    uint64_t row_id = rah.row_id();

    // 插入索引数据（使用commit_slot机制）
    uint64_t key = 1001;
    auto insert_result = idx->insert(&tx, key, row_id);
    if (insert_result != 0) {
        printf("Failed to insert into BwTree index\n");
        return false;
    }

    // 提交事务
    if (!tx.commit()) {
        printf("Failed to commit transaction\n");
        return false;
    }

    // 验证索引插入成功
    Transaction tx2(ctx);
    tx2.begin();

    uint64_t found_row_id = 0;
    auto lookup_result = idx->lookup(&tx2, key, false, [&](const uint64_t& key, uint64_t rid) -> bool {
        (void)key;
        found_row_id = rid;
        return true;
    });

    if (lookup_result != 1 || found_row_id != row_id) {
        printf("Index lookup failed: expected %lu, got %lu\n", row_id, found_row_id);
        return false;
    }

    tx2.commit();
    db.deactivate_thread(0);
    printf("BwTree basic insert test: PASSED\n");
    return true;
}

// 2. 事务可见性测试
bool test_bwtree_visibility(DB& db) {
    printf("Testing BwTree transaction visibility...\n");

    auto ctx1 = db.activate_thread(0);
    auto ctx2 = db.activate_thread(1);
    if (!ctx1 || !ctx2) {
        printf("Failed to activate threads\n");
        return false;
    }

    auto main_tbl = db.get_table("bwtree_main_tbl");
    auto idx = db.get_btree_index_unique_u64("bwtree_test_idx");

    // 事务1：插入数据但未提交
    Transaction tx1(ctx1);
    tx1.begin();

    RowAccessHandle rah1(&tx1);
    rah1.new_row(main_tbl, 0, Transaction::kNewRowID, true, 64);
    char data1[] = "uncommitted_data";
    rah1.write_row(data1, sizeof(data1));
    uint64_t row_id1 = rah1.row_id();

    uint64_t key1 = 2001;
    idx->insert(&tx1, key1, row_id1);  // 此时还不可见

    // 事务2：查询应该看不到未提交的数据
    Transaction tx2(ctx2);
    tx2.begin();

    uint64_t found_count = 0;
    auto lookup_result = idx->lookup(&tx2, key1, false, [&](const uint64_t& key, uint64_t rid) -> bool {
        (void)key;
        found_count++;
        return true;
    });

    if (lookup_result != 0 || found_count != 0) {
        printf("Uncommitted data is visible (visibility test failed)\n");
        return false;
    }

    tx2.commit();

    // 提交事务1
    tx1.commit();

    // 事务3：现在应该能看到提交的数据
    Transaction tx3(ctx2);
    tx3.begin();

    uint64_t found_row_id = 0;
    lookup_result = idx->lookup(&tx3, key1, false, [&](const uint64_t& key, uint64_t rid) -> bool {
        (void)key;
        found_row_id = rid;
        return true;
    });

    if (lookup_result != 1 || found_row_id != row_id1) {
        printf("Committed data not visible (visibility test failed)\n");
        return false;
    }

    tx3.commit();

    db.deactivate_thread(0);
    db.deactivate_thread(1);
    printf("BwTree visibility test: PASSED\n");
    return true;
}

// 3. 并发BwTree操作测试
bool test_bwtree_concurrent_operations(DB& db) {
    printf("Testing BwTree concurrent operations...\n");

    const int num_threads = 4;
    const int operations_per_thread = 50;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    auto main_tbl = db.get_table("bwtree_main_tbl");
    auto idx = db.get_btree_index_unique_u64("bwtree_test_idx");

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&db, &success_count, i, operations_per_thread, main_tbl, idx]() {
            auto ctx = db.activate_thread(i);
            if (ctx == nullptr) return;

            for (int j = 0; j < operations_per_thread; j++) {
                Transaction tx(ctx);
                if (!tx.begin()) continue;

                // 插入主表数据
                RowAccessHandle rah(&tx);
                if (!rah.new_row(main_tbl, 0, 64)) {
                    tx.abort();
                    continue;
                }

                char data[64];
                snprintf(data, sizeof(data), "thread_%d_op_%d", i, j);
                rah.write_row(data, strlen(data));
                uint64_t row_id = rah.row_id();

                // 插入索引数据
                uint64_t key = i * 1000 + j;
                auto insert_result = idx->insert(&tx, key, row_id);

                if (insert_result == 1) {
                    if (tx.commit()) {
                        success_count++;
                    }
                } else {
                    tx.abort();
                }
            }

            db.deactivate_thread(i);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    printf("Concurrent BwTree operations: %d/%d succeeded\n",
           success_count.load(), num_threads * operations_per_thread);

    return success_count.load() > (num_threads * operations_per_thread * 0.8);
}

// 4. 范围查询测试
bool test_bwtree_range_query(DB& db) {
    printf("Testing BwTree range query...\n");

    auto ctx = db.activate_thread(0);
    if (!ctx) return false;

    auto main_tbl = db.get_table("bwtree_main_tbl");
    auto idx = db.get_btree_index_unique_u64("bwtree_test_idx");

    // 插入测试数据
    std::vector<uint64_t> test_keys = {3000, 3001, 3002, 3003, 3004};
    std::vector<uint64_t> row_ids;

    for (auto key : test_keys) {
        Transaction tx(ctx);
        tx.begin();

        RowAccessHandle rah(&tx);
        rah.new_row(main_tbl, 0, Transaction::kNewRowID, true, 64);
        char data[64];
        snprintf(data, sizeof(data), "range_test_%lu", key);
        rah.write_row(data, strlen(data));
        row_ids.push_back(rah.row_id());

        idx->insert(&tx, key, rah.row_id());
        tx.commit();
    }

    // 测试范围查询
    Transaction tx_query(ctx);
    tx_query.begin();

    std::vector<std::pair<uint64_t, uint64_t>> results;
    auto range_result = idx->lookup<::mica::transaction::BTreeRangeType::kInclusive,
                                   ::mica::transaction::BTreeRangeType::kInclusive, false>(
        &tx_query, 3001, 3003, false, [&](const uint64_t& key, uint64_t rid) -> bool {
            (void)key;
            results.emplace_back(key, row_id);
            return true;
        });

    if (range_result != 3) {
        printf("Range query returned wrong count: expected 3, got %lu\n", range_result);
        return false;
    }

    // 验证结果
    if (results.size() != 3) {
        printf("Range query results size mismatch\n");
        return false;
    }

    tx_query.commit();
    db.deactivate_thread(0);
    printf("BwTree range query test: PASSED\n");
    return true;
}

// 5. 删除操作测试
bool test_bwtree_delete(DB& db) {
    printf("Testing BwTree delete operations...\n");

    auto ctx = db.activate_thread(0);
    if (!ctx) return false;

    auto main_tbl = db.get_table("bwtree_main_tbl");
    auto idx = db.get_btree_index_unique_u64("bwtree_test_idx");

    // 插入测试数据
    Transaction tx_insert(ctx);
    tx_insert.begin();

    RowAccessHandle rah(&tx_insert);
    rah.new_row(main_tbl, 0, 64);
    char data[] = "delete_test_data";
    rah.write_row(0, [&](uint16_t cf_id, RowVersion<CXLTestConfig>* write_rv, RowVersion<CXLTestConfig>* read_rv) -> bool {  
      (void)cf_id;  
      (void)read_rv;  
      char* dest = write_rv->data;  
      memcpy(dest, data, strlen(data));  
      return true;  
    });
    uint64_t row_id = rah.row_id();

    uint64_t key = 4001;
    idx->insert(&tx_insert, key, row_id);
    tx_insert.commit();

    // 验证插入成功
    Transaction tx_verify(ctx);
    tx_verify.begin();

    uint64_t found_before = 0;
    auto lookup_before = idx->lookup(&tx_verify, key, false, [&](const uint64_t& key, uint64_t rid) -> bool {
        (void)key;
        found_before++;
        return true;
    });

    if (lookup_before != 1) {
        printf("Data not found before delete\n");
        return false;
    }
    tx_verify.commit();

    // 删除数据
    Transaction tx_delete(ctx);
    tx_delete.begin();

    auto delete_result = idx->remove(&tx_delete, key, row_id);
    if (delete_result != 0) {
        printf("Failed to delete from BwTree index\n");
        return false;
    }

    tx_delete.commit();

    // 验证删除成功
    Transaction tx_verify_after(ctx);
    tx_verify_after.begin();

    uint64_t found_after = 0;
    auto lookup_after = idx->lookup(&tx_verify_after, key, false, [&](const uint64_t& key, uint64_t rid) -> bool {
        (void)key;
        found_after++;
        return true;
    });

    if (lookup_after != 0) {
        printf("Data still visible after delete\n");
        return false;
    }

    tx_verify_after.commit();
    db.deactivate(0);
    printf("BwTree delete test: PASSED\n");
    return true;
}

// 主测试函数
BwTreeTestResults run_bwtree_tests() {
    BwTreeTestResults results;

    printf("=== BwTree Index with Commit_Slot Test Suite ===\n");

    // 初始化数据库
    ::mica::util::Config config;
    ::mica::transaction::PagePool<BwTreeTestConfig>* page_pools[8];
    // 初始化page_pools...
    DB db(page_pools, nullptr, nullptr, 4);

    if (false) {
        printf("Failed to initialize database\n");
        return results;
    }

    // 运行测试
    results.total_tests = 5;

    if (test_bwtree_basic_insert(db)) {
        results.basic_insert_test = true;
        results.passed_tests++;
    }

    if (test_bwtree_visibility(db)) {
        results.visibility_test = true;
        results.passed_tests++;
    }

    if (test_bwtree_concurrent_operations(db)) {
        results.concurrent_test = true;
        results.passed_tests++;
    }

    if (test_bwtree_range_query(db)) {
        results.range_query_test = true;
        results.passed_tests++;
    }

    if (test_bwtree_delete(db)) {
        results.delete_test = true;
        results.passed_tests++;
    }

    return results;
}

// 打印测试结果
void print_bwtree_test_results(const BwTreeTestResults& results) {
    printf("\n=== BwTree Test Results ===\n");
    printf("Total Tests: %d\n", results.total_tests);
    printf("Passed: %d\n", results.passed_tests);
    printf("Failed: %d\n", results.total_tests - results.passed_tests);

    printf("\nDetailed Results:\n");
    printf("Basic Insert Test: %s\n", results.basic_insert_test ? "PASSED" : "FAILED");
    printf("Visibility Test: %s\n", results.visibility_test ? "PASSED" : "FAILED");
    printf("Concurrent Test: %s\n", results.concurrent_test ? "PASSED" : "FAILED");
    printf("Range Query Test: %s\n", results.range_query_test ? "PASSED" : "FAILED");
    printf("Delete Test: %s\n", results.delete_test ? "PASSED" : "FAILED");

    if (results.passed_tests == results.total_tests) {
        printf("\n🎉 ALL BWTree TESTS PASSED! 🎉\n");
    } else {
        printf("\n❌ SOME BWTree TESTS FAILED ❌\n");
    }
}

int main(int argc, char* argv[]) {
    printf("Cicada Engine BwTree Index + Commit_Slot Test Suite\n");
    printf("===================================================\n");

    try {
        auto results = run_bwtree_tests();
        print_bwtree_test_results(results);

        return (results.passed_tests == results.total_tests) ? 0 : 1;

    } catch (const std::exception& e) {
        printf("Test failed with exception: %s\n", e.what());
        return 1;
    }
}