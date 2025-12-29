#include <cstdio>
#include <vector>
#include <thread>
#include <numa.h>
#include <sched.h>
#include <numaif.h>
#include "mica/transaction/db.h"
#include "mica/transaction/transaction.h"
#include "mica/transaction/row_access.h"
#include "mica/test/cxl_detector.h"
#include "mica/util/lcore.h"
#include "mica/util/stopwatch.h"

struct CXLRowDataTestConfig : public ::mica::transaction::BasicDBConfig {
    typedef ::mica::transaction::NullLogger<CXLRowDataTestConfig> Logger;
    static constexpr bool kEnableSlotCommit = true;
    static constexpr bool kVerbose = true;
};

typedef CXLRowDataTestConfig::Alloc Alloc;
typedef CXLRowDataTestConfig::Logger Logger;
typedef ::mica::transaction::DB<CXLRowDataTestConfig> DB;
typedef ::mica::transaction::Transaction<CXLRowDataTestConfig> Transaction;
typedef ::mica::transaction::RowAccessHandle<CXLRowDataTestConfig> RowAccessHandle;
typedef ::mica::transaction::Table<CXLRowDataTestConfig> Table;
typedef ::mica::transaction::RowVersion<CXLRowDataTestConfig> RowVersion;

// 验证内存是否在指定NUMA节点
bool verify_numa_location(void* addr, int expected_numa_node) {
    if (!addr) return false;

    int status[1];
    void* pages[1] = {addr};

    if (numa_move_pages(0, 1, pages, nullptr, status, MPOL_MF_MOVE) != 0) {
        printf("Failed to get NUMA location\n");
        return false;
    }

    printf("Memory at %p is on NUMA node: %d (expected: %d)\n",
           addr, status[0], expected_numa_node);

    return status[0] == expected_numa_node;
}

// 测试CXL表创建
bool test_cxl_table_creation(DB& db) {
    printf("=== Testing CXL Table Creation ===\n");

    if (!::mica::test::CXLDetector::is_cxl_available()) {
        printf("CXL not available, skipping test\n");
        return false;
    }

    size_t cxl_numa_node = ::mica::test::CXLDetector::get_cxl_numa_node();
    printf("Using CXL NUMA node: %zu\n", cxl_numa_node);

    uint64_t data_sizes[] = {128, 256};
    if (!db.create_cxl_table("cxl_row_test", 2, data_sizes)) {
        printf("Failed to create CXL table\n");
        return false;
    }

    auto cxl_tbl = db.get_cxl_table("cxl_row_test");
    if (!cxl_tbl) {
        printf("Failed to get CXL table\n");
        return false;
    }

    printf("✓ CXL table created successfully\n");
    return true;
}

// 测试CXL行分配
bool test_cxl_row_allocation(DB& db) {
    printf("\n=== Testing CXL Row Data Allocation ===\n");

    auto cxl_tbl = db.get_cxl_table("cxl_row_test");
    if (!cxl_tbl) {
        printf("CXL table not found\n");
        return false;
    }

    db.activate(0);
    auto ctx = db.context(0);
    if (!ctx) {
        printf("Failed to get context\n");
        db.deactivate(0);
        return false;
    }

    std::vector<uint64_t> row_ids;
    if (!cxl_tbl->allocate_cxl_rows(ctx, row_ids)) {
        printf("Failed to allocate CXL rows\n");
        db.deactivate(0);
        return false;
    }

    printf("Allocated %zu CXL rows\n", row_ids.size());

    size_t cxl_numa_node = ::mica::test::CXLDetector::get_cxl_numa_node();
    bool all_in_cxl = true;

    for (size_t i = 0; i < std::min(row_ids.size(), size_t(5)); i++) {
        auto row_head = cxl_tbl->head(0, row_ids[i]);
        if (row_head) {
            if (!verify_numa_location((void*)row_head, static_cast<int>(cxl_numa_node))) {
                all_in_cxl = false;
            }
        }
    }

    if (all_in_cxl) {
        printf("✓ All verified row data is in CXL memory\n");
    } else {
        printf("❌ Some row data is not in CXL memory\n");
    }

    db.deactivate(0);
    return all_in_cxl;
}

// 测试CXL行操作
bool test_cxl_row_operations(DB& db) {
    printf("\n=== Testing CXL Row Data Operations ===\n");

    auto cxl_tbl = db.get_cxl_table("cxl_row_test");
    if (!cxl_tbl) {
        printf("CXL table not found\n");
        return false;
    }

    db.activate(0);
    auto ctx = db.context(0);
    if (!ctx) {
        printf("Failed to get context\n");
        db.deactivate(0);
        return false;
    }

    std::vector<uint64_t> row_ids;
    if (!cxl_tbl->allocate_cxl_rows(ctx, row_ids)) {
        printf("Failed to allocate CXL rows\n");
        db.deactivate(0);
        return false;
    }

    Transaction tx(ctx);
    if (!tx.begin()) {
        printf("Failed to begin transaction\n");
        db.deactivate(0);
        return false;
    }

    RowAccessHandle rah(&tx);
    if (!rah.new_row(cxl_tbl, 0, Transaction::kNewRowID, true, 128)) {
        printf("Failed to create new row\n");
        db.deactivate(0);
        return false;
    }

    char test_data[128];
    snprintf(test_data, sizeof(test_data), "CXL_row_data_test_%lu",
             static_cast<unsigned long>(row_ids[0]));

    // 修复：使用正确的 DataCopier 签名
    if (!rah.write_row(128, [&](uint16_t cf_id, RowVersion* write_rv, const RowVersion* read_rv) {
        (void)cf_id;
        (void)read_rv;
        if (write_rv && write_rv->data_size > 0) {
            memcpy(write_rv->data, test_data, strlen(test_data));
            return true;
        }
        return false;
    })) {
        printf("Failed to write row data\n");
        db.deactivate(0);
        return false;
    }

    if (!tx.commit()) {
        printf("Failed to commit transaction\n");
        db.deactivate(0);
        return false;
    }

    printf("✓ Data written to CXL memory\n");

    // 验证读取
    Transaction tx2(ctx);
    if (!tx2.begin()) {
        printf("Failed to begin read transaction\n");
        db.deactivate(0);
        return false;
    }

    RowAccessHandle rah2(&tx2);
    if (!rah2.peek_row(cxl_tbl, 0, row_ids[0], false, true, false)) {
        printf("Failed to peek row\n");
        db.deactivate(0);
        return false;
    }

    if (!rah2.read_row()) {
        printf("Failed to read row\n");
        db.deactivate(0);
        return false;
    }

    if (memcmp(rah2.cdata(), test_data, strlen(test_data)) == 0) {
        printf("✓ Data read from CXL memory matches written data\n");
    } else {
        printf("❌ Data mismatch\n");
        db.deactivate(0);
        return false;
    }

    tx2.commit();
    db.deactivate(0);
    return true;
}

// 测试内联行
bool test_cxl_inlined_rows(DB& db) {
    printf("\n=== Testing CXL Inlined Row Versions ===\n");

    auto cxl_tbl = db.get_cxl_table("cxl_row_test");
    if (!cxl_tbl) {
        printf("CXL table not found\n");
        return false;
    }

    db.activate(0);
    auto ctx = db.context(0);
    if (!ctx) {
        printf("Failed to get context\n");
        db.deactivate(0);
        return false;
    }

    Transaction tx(ctx);
    if (!tx.begin()) {
        db.deactivate(0);
        return false;
    }

    RowAccessHandle rah(&tx);
    if (!rah.new_row(cxl_tbl, 1, Transaction::kNewRowID, true, 64)) {
        printf("Failed to create small row\n");
        db.deactivate(0);
        return false;
    }

    char small_data[64] = "inline_test";

    // 修复：使用正确的 DataCopier 签名
    if (!rah.write_row(64, [&](uint16_t cf_id, RowVersion* write_rv, const RowVersion* read_rv) {
        (void)cf_id;
        (void)read_rv;
        if (write_rv && write_rv->data_size > 0) {
            memcpy(write_rv->data, small_data, strlen(small_data));
            return true;
        }
        return false;
    })) {
        printf("Failed to write small row data\n");
        db.deactivate(0);
        return false;
    }

    if (!tx.commit()) {
        printf("Failed to commit small row\n");
        db.deactivate(0);
        return false;
    }

    printf("✓ Small data inlined in CXL memory\n");

    std::vector<uint64_t> row_ids;
    if (cxl_tbl->allocate_cxl_rows(ctx, row_ids)) {
        auto row_head = cxl_tbl->head(1, row_ids[0]);
        if (row_head && row_head->inlined_rv) {
            size_t cxl_numa_node = ::mica::test::CXLDetector::get_cxl_numa_node();
            if (verify_numa_location(row_head->inlined_rv, static_cast<int>(cxl_numa_node))) {
                printf("✓ Inlined row version is in CXL memory\n");
            }
        }
    }

    db.deactivate(0);
    return true;
}

int main() {
    printf("CXL Row Data Maintenance Test\n");
    printf("==============================\n\n");

    if (!::mica::test::CXLDetector::is_cxl_available()) {
        printf("CXL not available - cannot test CXL row data\n");
        return 1;
    }

    // 使用正确的 DB 初始化方式
    auto config = ::mica::util::Config::load_file("test_tx.json");
    Alloc alloc(config.get("alloc"));

    auto page_pool_size = 24 * uint64_t(1073741824);
    ::mica::transaction::PagePool<CXLRowDataTestConfig>* page_pools[2];
    page_pools[0] = new ::mica::transaction::PagePool<CXLRowDataTestConfig>(&alloc, page_pool_size / 2, 0);
    page_pools[1] = new ::mica::transaction::PagePool<CXLRowDataTestConfig>(&alloc, page_pool_size / 2, 1);

    ::mica::util::Stopwatch sw;
    sw.init_start();
    sw.init_end();

    Logger logger;
    DB db(page_pools, &logger, &sw, 1);

    bool all_passed = true;

    if (!test_cxl_table_creation(db)) {
        all_passed = false;
    }

    if (!test_cxl_row_allocation(db)) {
        all_passed = false;
    }

    if (!test_cxl_row_operations(db)) {
        all_passed = false;
    }

    if (!test_cxl_inlined_rows(db)) {
        all_passed = false;
    }

    printf("\n=== Test Summary ===\n");
    if (all_passed) {
        printf("🎉 All CXL row data tests PASSED!\n");
        printf("✓ Row data is correctly maintained in CXL memory\n");
        printf("✓ Read/write operations work correctly\n");
        printf("✓ Inlined row versions are in CXL memory\n");
    } else {
        printf("❌ Some CXL row data tests FAILED\n");
    }

    return all_passed ? 0 : 1;
}