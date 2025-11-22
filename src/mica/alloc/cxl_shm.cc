#include "mica/alloc/cxl_shm.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <dirent.h>
#include <cstring>
#include <algorithm>
#include <linux/limits.h>
#include "mica/util/barrier.h"
#include "mica/util/lcore.h"
#include "mica/util/safe_cast.h"

namespace mica {
namespace alloc {

void CXL_SHM::clean_files() {
  char cmd[PATH_MAX];
  snprintf(cmd, PATH_MAX, "rm %s/%s* > /dev/null 2>&1", cxl_device_path_.c_str(),
           filename_prefix_.c_str());
  int ret = system(cmd);
  (void)ret;
}

void CXL_SHM::make_path(size_t file_id, char* out_path) {
  snprintf(out_path, PATH_MAX, "%s/%s%zu", cxl_device_path_.c_str(),
           filename_prefix_.c_str(), file_id);
}

void CXL_SHM::lock() {
  while (__sync_lock_test_and_set((volatile uint64_t*)&state_lock_, 1UL) == 1UL)
    ::mica::util::pause();
}

void CXL_SHM::unlock() {
  __sync_lock_release((volatile uint64_t*)&state_lock_);
}

void CXL_SHM::dump_page_info() {
  lock();
  for (size_t page_id = 0; page_id < pages_.size(); page_id++) {
    if (pages_[page_id].addr == nullptr) continue;
    printf("page %zu: addr=%p cxl_node=%zu in_use=%s\n", page_id,
           pages_[page_id].addr, pages_[page_id].cxl_node,
           pages_[page_id].in_use ? "yes" : "no");
  }
  unlock();
}

CXL_SHM::CXL_SHM(const ::mica::util::Config& config) : config_(config) {
  cxl_device_path_ = config.get("cxl_device_path").get_str("/dev/cxl");
  filename_prefix_ = config.get("filename_prefix").get_str("mica_cxl_");
  num_pages_to_init_ = config.get("num_pages_to_init").get_uint64(1048576);

  {
    auto c = config.get("num_pages_to_free");
    if (c.exists()) {
      for (size_t i = 0; i < c.size(); i++) {
        size_t page_count = c.get(i).get_uint64();
        num_pages_to_free_.push_back(page_count);
      }
    }
    for (size_t i = num_pages_to_free_.size(); i < num_cxl_nodes_; i++)
      num_pages_to_free_.push_back(0);
  }

  {
    auto c = config.get("num_pages_to_reserve");
    if (c.exists()) {
      for (size_t i = 0; i < c.size(); i++) {
        size_t page_count = c.get(i).get_uint64();
        num_pages_to_reserve_.push_back(page_count);
      }
    }
    for (size_t i = num_pages_to_reserve_.size(); i < num_cxl_nodes_; i++)
      num_pages_to_reserve_.push_back(0);
  }

  clean_files_on_init_ = config.get("clean_files_on_init").get_bool(false);
  verbose_ = config.get("verbose").get_bool(false);

  state_lock_ = 0;
  used_memory_ = 0;

  initialize();
}

CXL_SHM::~CXL_SHM() {
  for (int fd : cxl_device_fds_) {
    if (fd >= 0) close(fd);
  }

  for (void* region : cxl_memory_regions_) {
    if (region != MAP_FAILED && region != nullptr) {
      munmap(region, kPageSize);
    }
  }
}

void CXL_SHM::initialize() {
  if (verbose_) printf("initializing CXL memory allocator\n");

  detect_cxl_topology();
  initialize_cxl_devices();

  if (clean_files_on_init_) {
    clean_files();
  }

  size_t next_file_id = 0;
  size_t num_allocated_pages = 0;

  for (size_t page_id = 0; page_id < num_pages_to_init_; page_id++) {
    if (verbose_ && page_id % 1000 == 0) {
      printf("allocating CXL page %zu/%zu\n", page_id, num_pages_to_init_);
      fflush(stdout);
    }

    size_t file_id = next_file_id++;
    char path[PATH_MAX];
    make_path(file_id, path);

    int fd = open(path, O_CREAT | O_RDWR, 0755);
    if (fd == -1) {
      perror("");
      fprintf(stderr, "error: could not open CXL file %s\n", path);
      break;
    }

    void* p = mmap(nullptr, kPageSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    if (p == MAP_FAILED) {
      unlink(path);
      break;
    }

    *(size_t*)p = 0;

    pages_.push_back(Page{file_id, p, nullptr, page_id % num_cxl_nodes_, false});
    num_allocated_pages++;
  }

  if (verbose_) printf("allocated %zu CXL pages\n", num_allocated_pages);
}

void CXL_SHM::detect_cxl_topology() {
  if (verbose_) printf("detecting CXL topology\n");

  num_cxl_nodes_ = 1;

  DIR* dir = opendir("/sys/bus/cxl/devices/");
  if (dir != nullptr) {
    struct dirent* entry;
    size_t cxl_device_count = 0;
    while ((entry = readdir(dir)) != nullptr) {
      if (strncmp(entry->d_name, "mem", 3) == 0) {
        cxl_device_count++;
      }
    }
    closedir(dir);

    if (cxl_device_count > 0) {
      num_cxl_nodes_ = cxl_device_count;
    }
  }

  if (verbose_) printf("detected %zu CXL nodes\n", num_cxl_nodes_);
}

void CXL_SHM::initialize_cxl_devices() {
  if (verbose_) printf("initializing CXL devices\n");

  cxl_device_fds_.resize(num_cxl_nodes_, -1);
  cxl_memory_regions_.resize(num_cxl_nodes_, nullptr);

  for (size_t i = 0; i < num_cxl_nodes_; i++) {
    char device_path[PATH_MAX];
    snprintf(device_path, sizeof(device_path), "%s%zu", cxl_device_path_.c_str(), i);

    int fd = open(device_path, O_RDWR);
    if (fd >= 0) {
      cxl_device_fds_[i] = fd;
      if (verbose_) printf("opened CXL device %s\n", device_path);
    } else {
      if (verbose_) printf("failed to open CXL device %s\n", device_path);
    }
  }
}

void* CXL_SHM::find_free_address(size_t size) {
  size_t alignment = kPageSize;

  if (alignment == 0) alignment = 1;

  if (::mica::util::next_power_of_two(alignment) != alignment) {
    fprintf(stderr, "error: invalid alignment\n");
    return nullptr;
  }

  int fd = open("/dev/zero", O_RDONLY);
  if (fd == -1) {
    perror("");
    fprintf(stderr, "error: could not open /dev/zero\n");
    return nullptr;
  }

  void* p = mmap(nullptr, size + alignment, PROT_READ, MAP_PRIVATE, fd, 0);
  close(fd);

  if (p == MAP_FAILED) {
    perror("");
    fprintf(stderr, "error: failed to map /dev/zero to find a free memory region of size %zu\n", size);
    return nullptr;
  }

  munmap(p, size);
  p = (void*)(((size_t)p + (alignment - 1)) & ~(alignment - 1));
  return p;
}

size_t CXL_SHM::alloc(size_t length, size_t cxl_node) {
  if (cxl_node == (size_t)-1) {
    cxl_node = ::mica::util::lcore.numa_id();
    if (cxl_node == ::mica::util::LCore::kUnknown) {
      fprintf(stderr, "warning: failed to detect CXL node for current cpu\n");
      return kInvalidId;
    }
  }

  lock();

  size_t entry_id;
  for (entry_id = 0; entry_id < entries_.size(); entry_id++) {
    if (entries_[entry_id].page_ids.empty()) break;
  }

  if (entry_id == entries_.size()) {
    entries_.push_back(Entry{0, false, 0, 0, std::vector<size_t>()});
  }

  size_t num_pages = (length + (kPageSize - 1)) / kPageSize;
  entries_[entry_id].length = length;
  entries_[entry_id].num_pages = num_pages;
  size_t num_allocated_pages = 0;

  for (size_t page_id = 0; page_id < pages_.size(); page_id++) {
    if (num_allocated_pages == num_pages) break;

    if (pages_[page_id].addr == nullptr) continue;
    if (pages_[page_id].in_use || pages_[page_id].cxl_node != cxl_node) continue;

    entries_[entry_id].page_ids.push_back(page_id);
    num_allocated_pages++;
  }

  if (num_pages != num_allocated_pages) {
    fprintf(stderr, "warning: insufficient CXL memory on node %zu to allocate %zu bytes\n",
            cxl_node, length);
    entries_[entry_id].refcount = 0;
    entries_[entry_id].to_remove = false;
    entries_[entry_id].length = 0;
    entries_[entry_id].num_pages = 0;
    entries_[entry_id].page_ids.clear();
    unlock();
    return static_cast<size_t>(-1);
  }

  for (size_t page_index = 0; page_index < num_pages; page_index++) {
    pages_[entries_[entry_id].page_ids[page_index]].in_use = true;
  }

  unlock();

  if (verbose_) {
    printf("allocated CXL entry %zu (length=%zu, num_pages=%zu) on node %zu\n",
           entry_id, length, num_pages, cxl_node);
  }

  return entry_id;
}

void CXL_SHM::check_release(size_t entry_id) {
  if (entries_[entry_id].refcount == 0 && entries_[entry_id].to_remove != 0) {
    for (size_t page_index = 0; page_index < entries_[entry_id].num_pages; page_index++)
      pages_[entries_[entry_id].page_ids[page_index]].in_use = false;

    entries_[entry_id].refcount = 0;
    entries_[entry_id].to_remove = false;
    entries_[entry_id].length = 0;
    entries_[entry_id].num_pages = 0;
    entries_[entry_id].page_ids.clear();

    used_memory_ -= entries_[entry_id].num_pages * kPageSize;
    if (verbose_) printf("deallocated CXL entry %zu\n", entry_id);
  }
}

bool CXL_SHM::schedule_release(size_t entry_id) {
  lock();

  if (entries_[entry_id].page_ids.empty()) {
    unlock();
    fprintf(stderr, "error: invalid entry\n");
    return false;
  }

  entries_[entry_id].to_remove = 1;
  check_release(entry_id);

  unlock();
  return true;
}

bool CXL_SHM::map(size_t entry_id, void* ptr, size_t offset, size_t length) {
  if (((size_t)ptr & ~(kPageSize - 1)) != (size_t)ptr) {
    fprintf(stderr, "error: invalid ptr alignment\n");
    return false;
  }

  if ((offset & ~(kPageSize - 1)) != offset) {
    fprintf(stderr, "error: invalid offset alignment\n");
    return false;
  }

  lock();

  if (entries_[entry_id].page_ids.empty()) {
    unlock();
    fprintf(stderr, "error: invalid entry\n");
    return false;
  }

  if (offset > entries_[entry_id].length || offset + length > entries_[entry_id].length) {
    unlock();
    fprintf(stderr, "error: invalid offset or length\n");
    return false;
  }

  size_t mapping_id;
  for (mapping_id = 0; mapping_id < mappings_.size(); mapping_id++) {
    if (mappings_[mapping_id].addr == nullptr) break;
  }

  if (mapping_id == mappings_.size())
    mappings_.push_back(Mapping{0, nullptr, 0, 0, 0});

  size_t page_offset = offset / kPageSize;
  size_t num_pages = (length + (kPageSize - 1)) / kPageSize;

  void* p = ptr;
  size_t page_index = page_offset;
  size_t page_index_end = page_offset + num_pages;
  int error = 0;

  while (page_index < page_index_end) {
    char path[PATH_MAX];
    make_path(pages_[entries_[entry_id].page_ids[page_index]].file_id, path);
    int fd = open(path, O_RDWR);
    if (fd == -1) {
      error = 1;
      perror("");
      fprintf(stderr, "error: could not open CXL file %s\n", path);
      break;
    }

    void* ret_p = mmap(p, kPageSize, PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_FIXED, fd, 0);
    close(fd);

    if (ret_p == MAP_FAILED) {
      error = 1;
      fprintf(stderr, "error: CXL mmap failed at %p\n", p);
      break;
    } else if (ret_p != p) {
      error = 1;
      fprintf(stderr, "error: CXL mmap failed at %p (mismatch; mapped at %p)\n", p, ret_p);
      break;
    }

    page_index++;
    p = (void*)((size_t)p + kPageSize);
  }

  if (error) {
    p = ptr;
    size_t page_index_clean = page_offset;
    while (page_index_clean < page_index) {
      munmap(p, kPageSize);
      page_index_clean++;
      p = (void*)((size_t)p + kPageSize);
    }
    unlock();
    return false;
  }

  if (entries_[entry_id].refcount == 0)
    used_memory_ += entries_[entry_id].num_pages * kPageSize;
  entries_[entry_id].refcount++;

  mappings_[mapping_id].entry_id = entry_id;
  mappings_[mapping_id].addr = ptr;
  mappings_[mapping_id].length = length;
  mappings_[mapping_id].page_offset = page_offset;
  mappings_[mapping_id].num_pages = num_pages;

  unlock();

  if (verbose_)
    printf("created CXL mapping %zu (entry %zu, page_offset=%zu, num_pages=%zu) at %p\n",
           mapping_id, entry_id, page_offset, num_pages, ptr);

  return true;
}

bool CXL_SHM::unmap(void* ptr) {
  lock();

  size_t mapping_id;
  for (mapping_id = 0; mapping_id < mappings_.size(); mapping_id++) {
    if (mappings_[mapping_id].addr == ptr) break;
  }

  if (mapping_id == mappings_.size()) {
    unlock();
    fprintf(stderr, "error: invalid ptr for CXL unmap\n");
    return false;
  }

  size_t entry_id = mappings_[mapping_id].entry_id;
  size_t num_pages = mappings_[mapping_id].num_pages;

  void* p = ptr;
  for (size_t page_index = 0; page_index < num_pages; page_index++) {
    munmap(p, kPageSize);
    p = (void*)((size_t)p + kPageSize);
  }

  entries_[entry_id].refcount--;
  check_release(entry_id);

  mappings_[mapping_id].entry_id = 0;
  mappings_[mapping_id].addr = nullptr;
  mappings_[mapping_id].length = 0;
  mappings_[mapping_id].page_offset = 0;
  mappings_[mapping_id].num_pages = 0;

  unlock();

  if (verbose_) printf("unmapped CXL mapping %zu at %p\n", mapping_id, ptr);

  return true;
}

void* CXL_SHM::malloc_contiguous(size_t size, size_t lcore) {
  size = CXL_SHM::roundup(size);
  size_t cxl_node = ::mica::util::lcore.numa_id(lcore);
  if (cxl_node == ::mica::util::lcore.kUnknown) {
    fprintf(stderr, "error: invalid lcore for CXL\n");
    return nullptr;
  }

  size_t entry_id = alloc(size, cxl_node);
  if (entry_id == kInvalidId) return nullptr;

  while (true) {
    void* p = find_free_address(size);
    if (p == nullptr) {
      schedule_release(entry_id);
      return nullptr;
    }
    if (map(entry_id, p, 0, size)) {
      schedule_release(entry_id);
      return p;
    }
  }
}

void* CXL_SHM::malloc_contiguous_local(size_t size) {
  size_t lcore = ::mica::util::lcore.lcore_id();
  return malloc_contiguous(size, lcore);
}

void CXL_SHM::free_contiguous(void* ptr) {
  unmap(ptr);
}

void* CXL_SHM::malloc_striped(size_t size) {
  size = CXL_SHM::roundup(size);
  size_t num_pages = size / kPageSize;

  std::vector<size_t> entry_ids;
  std::vector<void*> addrs;

  for (size_t page_index = 0; page_index < num_pages; page_index++) {
    size_t cxl_node = page_index % num_cxl_nodes_;
    size_t entry_id = alloc(kPageSize, cxl_node);
    if (entry_id == kInvalidId) {
      for (size_t i = 0; i < entry_ids.size(); i++) {
        schedule_release(entry_ids[i]);
      }
      return nullptr;
    }
    entry_ids.push_back(entry_id);
  }

  void* base_addr = find_free_address(size);
  if (base_addr == nullptr) {
    for (size_t entry_id : entry_ids) {
      schedule_release(entry_id);
    }
    return nullptr;
  }

  void* p = base_addr;
  for (size_t i = 0; i < entry_ids.size(); i++) {
    if (!map(entry_ids[i], p, 0, kPageSize)) {
      for (size_t j = 0; j < i; j++) {
        unmap((void*)((size_t)base_addr + j * kPageSize));
      }
      for (size_t entry_id : entry_ids) {
        schedule_release(entry_id);
      }
      return nullptr;
    }
    schedule_release(entry_ids[i]);
    p = (void*)((size_t)p + kPageSize);
  }

  return base_addr;
}

void CXL_SHM::free_striped(void* ptr) {
  // 简化实现：假设striped内存按页面大小对齐
  // 实际实现需要跟踪striped分配的元数据
  unmap(ptr);
}

void* CXL_SHM::malloc_contiguous_any(size_t size) {
  for (size_t cxl_node = 0; cxl_node < num_cxl_nodes_; cxl_node++) {
    void* p = malloc_contiguous(size, cxl_node);
    if (p != nullptr) return p;
  }
  return nullptr;
}

}
}