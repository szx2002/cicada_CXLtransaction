#pragma once
#ifndef MICA_ALLOC_CXL_SHM_H_
#define MICA_ALLOC_CXL_SHM_H_

#include "mica/common.h"
#include "mica/util/config.h"
#include "mica/util/roundup.h"
#include <vector>
#include <string>

namespace mica {
namespace alloc {
class CXL_SHM {
 public:
  CXL_SHM(const ::mica::util::Config& config);
  ~CXL_SHM();

  static constexpr size_t kInvalidId = std::numeric_limits<size_t>::max();
  static constexpr size_t kPageSize = 2 * 1048576;

  static size_t roundup(size_t size) {
    return ::mica::util::roundup<2 * 1048576>(size);
  }

  void* find_free_address(size_t size);
  size_t alloc(size_t length, size_t cxl_node);
  bool schedule_release(size_t entry_id);
  bool map(size_t entry_id, void* ptr, size_t offset, size_t length);
  bool unmap(void* ptr);

  void* malloc_contiguous(size_t size, size_t cxl_node);
  void* malloc_contiguous_local(size_t size);
  void free_contiguous(void* ptr);
  void* malloc_striped(size_t size);
  void free_striped(void* ptr);

  size_t get_memuse() const { return used_memory_; }
  void dump_page_info();

 private:
  void initialize();
  void initialize_cxl_devices();
  void detect_cxl_topology();
  void clean_files();
  void make_path(size_t page_id, char* out_path);
  void lock();
  void unlock();
  void check_release(size_t entry_id);
  void* malloc_contiguous_any(size_t size);

  struct Page {
    size_t file_id;
    void* addr;
    void* paddr;
    size_t cxl_node;
    bool in_use;
  };

  struct Entry {
    size_t refcount;
    bool to_remove;
    size_t length;
    size_t num_pages;
    std::vector<size_t> page_ids;
  };

  struct Mapping {
    size_t entry_id;
    void* addr;
    size_t length;
    size_t page_offset;
    size_t num_pages;
  };

  ::mica::util::Config config_;
  std::string cxl_device_path_;
  std::string filename_prefix_;
  size_t num_cxl_nodes_;
  size_t num_pages_to_init_;
  std::vector<size_t> num_pages_to_free_;
  std::vector<size_t> num_pages_to_reserve_;
  bool clean_files_on_init_;
  bool verbose_;

  uint64_t state_lock_;
  std::vector<Page> pages_;
  std::vector<Entry> entries_;
  std::vector<Mapping> mappings_;
  size_t used_memory_;

  std::vector<int> cxl_device_fds_;
  std::vector<void*> cxl_memory_regions_;
};
}
}
#endif