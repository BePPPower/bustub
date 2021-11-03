//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances) {
  // Allocate and create individual BufferPoolManagerInstances
  std::allocator<BufferPoolManagerInstance> alloca;
  buffer_pool_instances_ = alloca.allocate(num_instances);
  for (size_t i = 0; i < num_instances; ++i) {
    uint32_t tmp_num_instances = static_cast<uint32_t>(num_instances);
    uint32_t instance_index = static_cast<uint32_t>(i);
    alloca.construct(buffer_pool_instances_ + i, pool_size, tmp_num_instances, instance_index, disk_manager,
                     log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  std::allocator<BufferPoolManagerInstance> alloca;
  for (size_t i = 0; i < num_instances_; ++i) {
    alloca.destroy(buffer_pool_instances_ + i);
  }
  alloca.deallocate(buffer_pool_instances_, num_instances_);
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  size_t buffer_pool_instances_index = page_id % num_instances_;
  return &buffer_pool_instances_[buffer_pool_instances_index];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->FetchPgImp(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->UnpinPgImp(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->FlushPgImp(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  Page *p = nullptr;
  for (size_t i = 0; i < num_instances_; ++i) {
    BufferPoolManager *bpm = &buffer_pool_instances_[start_search_index_];
    p = bpm->NewPgImp(page_id);
    if (p != nullptr) {
      start_search_index_ = (start_search_index_ + 1) % num_instances_;
      return p;
    }
    start_search_index_ = (start_search_index_ + 1) % num_instances_;
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->DeletePgImp(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; ++i) {
    BufferPoolManager *bpm = &buffer_pool_instances_[i];
    bpm->FlushAllPgsImp();
  }
}

}  // namespace bustub
