#pragma once
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include "mica/common.h"
#include "mica/util/config.h"
  
namespace mica {
namespace alloc {

struct HeartbeatRecord {
    uint16_t thread_id;
    uint64_t timestamp;
    uint64_t transaction_id;
    uint8_t status;  // 0: active, 1: committing, 2: aborted, 3: failed
    char padding[7];  // 对齐到32字节
} __attribute__((packed));

class SSDHeartbeatManager {
public:
    SSDHeartbeatManager(const ::mica::util::Config& config);
    ~SSDHeartbeatManager();
      
    bool initialize();
    void start_monitoring();
    void stop_monitoring();

    // 心跳更新接口
    void update_heartbeat(uint16_t thread_id, uint64_t timestamp, uint64_t tx_id, uint8_t status);

    // 故障检测接口
    std::vector<uint16_t> detect_failed_threads();

    // 恢复接口
    bool recover_from_ssd();

private:
    void monitoring_thread_func();
    bool write_heartbeat_to_ssd(const HeartbeatRecord& record);
    bool read_heartbeats_from_ssd();

    std::string ssd_device_path_;
    std::string heartbeat_file_path_;
    uint64_t heartbeat_timeout_us_;
    uint64_t monitoring_interval_us_;

    int ssd_fd_;
    std::atomic<bool> monitoring_active_;
    std::thread monitoring_thread_;
    std::mutex heartbeat_mutex_;

    HeartbeatRecord heartbeats_[64];  // 最大支持64个线程
    uint64_t last_heartbeat_time_[64];
};

}
}