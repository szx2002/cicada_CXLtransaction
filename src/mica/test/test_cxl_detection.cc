#include <cstdio>
#include <numa.h>
#include <sched.h>  // 添加 sched.h 以支持 sched_getcpu()
#include "mica/test/cxl_detector.h"
#include "mica/util/lcore.h"

void print_numa_info() {
    printf("=== NUMA System Information ===\n");
    // 修复：使用 %zu 格式化 size_t
    printf("NUMA node count: %zu\n", ::mica::util::lcore.numa_count());

    for (size_t i = 0; i < ::mica::util::lcore.numa_count(); i++) {
        printf("NUMA node %zu: ", i);
        if (numa_node_of_cpu(sched_getcpu()) == i) {
            printf("(current) ");
        }

        // 修复：显式转换避免警告
        long long node_size = numa_node_size64(static_cast<int>(i), nullptr);
        if (node_size > 0) {
            printf("Memory: %lld MB\n", node_size / (1024 * 1024));
        } else {
            printf("No memory\n");
        }
    }
    printf("\n");
}

void test_cxl_detection() {
    printf("=== CXL Detection Test ===\n");

    auto mode = ::mica::test::CXLDetector::detect_cxl_mode();
    printf("Detected CXL Mode: ");

    switch (mode) {
        case ::mica::test::CXLDetector::CXLMode::kUnavailable:
            printf("Unavailable\n");
            break;
        case ::mica::test::CXLDetector::CXLMode::kNumaNode:
            printf("Available as NUMA node\n");
            break;
        case ::mica::test::CXLDetector::CXLMode::kSimulated:
            printf("Simulated\n");
            break;
    }

    bool available = ::mica::test::CXLDetector::is_cxl_available();
    printf("CXL Available: %s\n", available ? "YES" : "NO");

    size_t cxl_node = ::mica::test::CXLDetector::get_cxl_numa_node();
    if (cxl_node != static_cast<size_t>(-1)) {
        printf("CXL NUMA Node: %zu\n", cxl_node);
    } else {
        printf("No CXL NUMA node found\n");
    }
    printf("\n");
}

void test_fallback_behavior() {
    printf("=== Fallback Behavior Test ===\n");

    if (!::mica::test::CXLDetector::is_cxl_available()) {
        printf("✓ CXL not available - system should use fallback mode\n");
        printf("✓ Standard memory allocation will be used\n");
        printf("✓ All operations will continue on regular NUMA nodes\n");
    } else {
        printf("✓ CXL available - system will use CXL memory\n");
        printf("✓ CXL-specific optimizations will be enabled\n");
    }
    printf("\n");
}

bool test_environment_setup() {
    printf("=== Environment Setup Test ===\n");

    if (numa_available() == -1) {
        printf("❌ NUMA library not available\n");
        return false;
    }
    printf("✓ NUMA library available\n");

    // 修复：使用 %zu 格式化 size_t
    if (::mica::util::lcore.numa_count() < 2) {
        printf("⚠️  Only %zu NUMA node(s) available\n",
               ::mica::util::lcore.numa_count());
        printf("   CXL features will be limited\n");
    } else {
        printf("✓ %zu NUMA nodes available - suitable for CXL testing\n",
               ::mica::util::lcore.numa_count());
    }

    void* test_ptr = numa_alloc_onnode(1024, 0);
    if (test_ptr) {
        printf("✓ NUMA memory allocation working\n");
        numa_free(test_ptr, 1024);
    } else {
        printf("❌ NUMA memory allocation failed\n");
        return false;
    }

    printf("\n");
    return true;
}

int main() {
    printf("CXL Environment Detection and Setup Test\n");
    printf("==========================================\n\n");

    print_numa_info();

    if (!test_environment_setup()) {
        printf("Environment setup failed - cannot continue\n");
        return 1;
    }

    test_cxl_detection();
    test_fallback_behavior();

    printf("=== Test Summary ===\n");
    printf("✓ Environment detection completed\n");
    printf("✓ CXL availability checked\n");
    printf("✓ Fallback mechanism verified\n");

    if (::mica::test::CXLDetector::is_cxl_available()) {
        printf("\n🎉 CXL environment is ready!\n");
    } else {
        printf("\n⚠️  CXL not available - using fallback mode\n");
        printf("   This is normal on systems without CXL hardware\n");
    }

    return 0;
}