#pragma once
#ifndef MICA_TEST_CXL_DETECTOR_H_
#define MICA_TEST_CXL_DETECTOR_H_

#include <numa.h>
#include "mica/util/lcore.h"

namespace mica {
namespace test {

class CXLDetector {
public:
    enum class CXLMode {
        kUnavailable,    // 无CXL设备
        kNumaNode,       // CXL作为NUMA节点
        kSimulated       // 模拟CXL环境
    };

    static CXLMode detect_cxl_mode() {
        // 检查NUMA节点数量
        if (::mica::util::lcore.numa_count() < 2) {
            return CXLMode::kUnavailable;
        }

        // 检查是否有CXL相关的NUMA节点
        for (size_t i = 0; i < ::mica::util::lcore.numa_count(); i++) {
            if (is_cxl_numa_node(i)) {
                return CXLMode::kNumaNode;
            }
        }

        return CXLMode::kUnavailable;
    }

    static size_t get_cxl_numa_node() {
        auto mode = detect_cxl_mode();
        if (mode == CXLMode::kNumaNode) {
            // 返回CXL NUMA节点（根据您的设备是节点1）
            return 1;
        }
        return static_cast<size_t>(-1);
    }

    static bool is_cxl_available() {
        return detect_cxl_mode() != CXLMode::kUnavailable;
    }

private:
    static bool is_cxl_numa_node(size_t numa_node) {
        return numa_node == 1; // 根据您的设备，CXL在NUMA节点1
    }
};

}
}
#endif