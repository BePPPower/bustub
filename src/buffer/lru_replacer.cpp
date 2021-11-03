//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  list_latch_.lock();
  if (list_.size() > 0) {
    *frame_id = list_.front();
    list_latch_.unlock();
    Remove(*frame_id);
    return true;
  }
  list_latch_.unlock();
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (!this->IsExist(frame_id)) {
    LOG_WARN("Pin a page that is not exist in LRUReplacer: page-%d", frame_id);
    return;
  }
  Remove(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (this->IsExist(frame_id)) {
    return;
  } else {
    this->Put(frame_id);
  }
}

bool LRUReplacer::IsExist(frame_id_t frame_id) {
  hash_table_latch_.lock();
  if (hash_table_.find(frame_id) != hash_table_.end()) {
    hash_table_latch_.unlock();
    return true;
  }
  hash_table_latch_.unlock();
  return false;
}

bool LRUReplacer::Put(frame_id_t frame_id) {
  list_latch_.lock();
  if (list_.size() < num_pages_) {
    list_.emplace_back(frame_id);
    auto it = prev(list_.cend());
    list_latch_.unlock();
    hash_table_latch_.lock();
    hash_table_.insert(std::pair<frame_id_t, LinkList::const_iterator>{frame_id, it});
    hash_table_latch_.unlock();
    return true;
  }
  list_latch_.unlock();
  return false;
}

void LRUReplacer::Remove(frame_id_t frame_id) {
  hash_table_latch_.lock();
  auto it = hash_table_.find(frame_id)->second;
  hash_table_.erase(frame_id);
  hash_table_latch_.unlock();

  list_latch_.lock();
  list_.erase(it);
  list_latch_.unlock();
}

// void LRUReplacer::Flush(frame_id_t frame_id) {
//   Remove(frame_id);
//   list_.emplace_back(frame_id);
//   auto it = prev(list_.cend());
//   hash_table_.insert(std::pair<frame_id_t, LinkList::const_iterator>{frame_id, it});
// }

size_t LRUReplacer::Size() {
  list_latch_.lock();
  size_t size = list_.size();
  list_latch_.unlock();
  return size;
}

}  // namespace bustub
