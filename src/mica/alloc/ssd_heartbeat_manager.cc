#include "mica/alloc/ssd_heartbeat_manager.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#include <chrono>
#include <iostream>

namespace mica {
namespace alloc {

SSDHeartbeatManager::SSDHeartbeatManager(const ::mica::util::Config& config)
    : ssd_fd_(-1), monitoring_active_(false) {

    // 从配置中读取参数
    ssd_device_path_ = config.get("ssd_heartbeat").get("device_path").get_str("/dev/nvme0n1");
    heartbeat_file_path_ = config.get("ssd_heartbeat").get("heartbeat_file").get_str("/mnt/cxl_ssd/cicada_heartbeat.dat");
    heartbeat_timeout_us_ = config.get("ssd_heartbeat").get("timeout_us").get_uint64(200000)// 200ms
    monitoring_interval_us_ = config.get("ssd_heartbeat").get("monitoring_interval_us").get_uint64(100000);  // 100ms

    // 初始化心跳记录数组
    memset(heartbeats_, 0, sizeof(heartbeats_));
    memset(last_heartbeat_time_, 0, sizeof(last_heartbeat_time_));
}

SSDHeartbeatManager::~SSDHeartbeatManager() {
    stop_monitoring();
    if (ssd_fd_ >= 0) {
        close(ssd_fd_);
    }
}

bool SSDHeartbeatManager::initialize() {
    // 打开SSD设备文件
    ssd_fd_ = open(heartbeat_file_path_.c_str(), O_CREAT | O_RDWR | O_SYNC, 0644);
    if (ssd_fd_ < 0) {
        perror("Failed to open SSD heartbeat file");
        return false;
    }

    // 预分配文件空间
    size_t file_size = sizeof(HeartbeatRecord) * 64;  // 64个线程的心跳记录
    if (ftruncate(ssd_fd_, file_size) != 0) {
        perror("Failed to allocate SSD file space");
        close(ssd_fd_);
        ssd_fd_ = -1;
        return false;
    }

    // 尝试从SSD恢复之前的心跳数据
    recover_from_ssd();

    return true;
}

void SSDHeartbeatManager::start_monitoring() {
    if (monitoring_active_.load()) {
        return;  // 已经在监控中
    }

    monitoring_active_.store(true);
    monitoring_thread_ = std::thread(&SSDHeartbeatManager::monitoring_thread_func, this);
}

void SSDHeartbeatManager::stop_monitoring() {
    if (!monitoring_active_.load()) {
        return;
    }

    monitoring_active_.store(false);
    if (monitoring_thread_.joinable()) {
        monitoring_thread_.join();
    }
}

void SSDHeartbeatManager::update_heartbeat(uint16_t thread_id, uint64_t timestamp,
                                         uint64_t tx_id, uint8_t status) {
    if (thread_id >= 64) {
        return;  // 超出支持的线程数量
    }

    std::lock_guard<std::mutex> lock(heartbeat_mutex_);

    // 更新内存中的心跳记录
    heartbeats_[thread_id].thread_id = thread_id;
    heartbeats_[thread_id].timestamp = timestamp;
    heartbeats_[thread_id].transaction_id = tx_id;
    heartbeats_[thread_id].status = status;

    // 记录最后心跳时间
    auto now = std::chrono::steady_clock::now();
    last_heartbeat_time_[thread_id] =
        std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

    // 立即写入SSD
    write_heartbeat_to_ssd(heartbeats_[thread_id]);
}

std::vector<uint16_t> SSDHeartbeatManager::detect_failed_threads() {
    std::vector<uint16_t> failed_threads;
    std::lock_guard<std::mutex> lock(heartbeat_mutex_);

    auto now = std::chrono::steady_clock::now();
    uint64_t current_time = 
        std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

    for (uint16_t i = 0; i < 64; i++) {
        if (last_heartbeat_time_[i] > 0) {  // 线程曾经活跃过
            uint64_t time_diff = current_time - last_heartbeat_time_[i];

            // 检查是否超时且不是正常的abort状态
            if (time_diff > heartbeat_timeout_us_ &&
                heartbeats_[i].status != 2) {  // status 2 = aborted (正常情况)
                failed_threads.push_back(i);
            }
        }
    }

    return failed_threads;
}

bool SSDHeartbeatManager::recover_from_ssd() {
    if (ssd_fd_ < 0) {
        return false;
    }

    // 从SSD读取心跳数据
    lseek(ssd_fd_, 0, SEEK_SET);
    ssize_t bytes_read = read(ssd_fd_, heartbeats_, sizeof(heartbeats_));

    if (bytes_read != sizeof(heartbeats_)) {
        // 文件可能是新创建的，初始化为零
        memset(heartbeats_, 0, sizeof(heartbeats_));
        return true;
    }

    // 恢复最后心跳时间（设置为当前时间，避免误报）
    auto now = std::chrono::steady_clock::now();
    uint64_t current_time = 
        std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

    for (int i = 0; i < 64; i++) {
        if (heartbeats_[i].thread_id == i && heartbeats_[i].timestamp > 0) {
            last_heartbeat_time_[i] = current_time;
        }
    }

    return true;
}

void SSDHeartbeatManager::monitoring_thread_func() {
    while (monitoring_active_.load()) {
        // 检测失效的线程
        auto failed_threads = detect_failed_threads();
    
        if (!failed_threads.empty()) {
            std::cout << "Detected failed threads: ";
            for (uint16_t thread_id : failed_threads) {
                std::cout << thread_id << " ";
            
                // 标记线程为失效状态
                std::lock_guard<std::mutex> lock(heartbeat_mutex_);
                heartbeats_[thread_id].status = 3;  // status 3 = failed
                write_heartbeat_to_ssd(heartbeats_[thread_id]);
            }
            std::cout << std::endl;
        }
    
        // 定期将所有心跳数据批量写入SSD
        {
            std::lock_guard<std::mutex> lock(heartbeat_mutex_);
            lseek(ssd_fd_, 0, SEEK_SET);
            write(ssd_fd_, heartbeats_, sizeof(heartbeats_));
            fsync(ssd_fd_);  // 强制同步到SSD
        }
    
        // 等待下一个监控周期
        std::this_thread::sleep_for(std::chrono::microseconds(monitoring_interval_us_));
    }
}

bool SSDHeartbeatManager::write_heartbeat_to_ssd(const HeartbeatRecord& record) {
    if (ssd_fd_ < 0) {
        return false;
    }

    // 计算在文件中的偏移位置
    off_t offset = record.thread_id * sizeof(HeartbeatRecord);

    // 定位到正确位置并写入
    if (lseek(ssd_fd_, offset, SEEK_SET) != offset) {
        perror("Failed to seek in SSD file");
        return false;
    }
    
    ssize_t bytes_written = write(ssd_fd_, &record, sizeof(HeartbeatRecord));
    if (bytes_written != sizeof(HeartbeatRecord)) {
        perror("Failed to write heartbeat to SSD");
        return false;
    }  

    // todo：立即同步（可能影响性能）
    // fsync(ssd_fd_);
      
    return true;
}  
  
bool SSDHeartbeatManager::read_heartbeats_from_ssd() {
    if (ssd_fd_ < 0) {
        return false;
    }

    lseek(ssd_fd_, 0, SEEK_SET);
    ssize_t bytes_read = read(ssd_fd_, heartbeats_, sizeof(heartbeats_));
      
    return bytes_read == sizeof(heartbeats_);
}  
  
}  // namespace alloc
}  // namespace mica