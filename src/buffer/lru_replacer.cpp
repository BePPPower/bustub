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
  latch_.lock();
  if (!list_.empty()) {
    *frame_id = list_.front();
    Remove(*frame_id);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  if (!this->IsExist(frame_id)) {
    // LOG_WARN("Pin a page that is not exist in LRUReplacer: page-%d", frame_id);
    latch_.unlock();
    return;
  }
  Remove(frame_id);
  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  if (this->IsExist(frame_id)) {
    latch_.unlock();
    return;
  }
  this->Put(frame_id);
  latch_.unlock();
}

size_t LRUReplacer::Size() {
  latch_.lock();
  size_t size = list_.size();
  latch_.unlock();
  return size;
}

bool LRUReplacer::IsExist(frame_id_t frame_id) { return hash_table_.find(frame_id) != hash_table_.end(); }

bool LRUReplacer::Put(frame_id_t frame_id) {
  if (list_.size() < num_pages_) {
    list_.emplace_back(frame_id);
    auto it = prev(list_.cend());
    hash_table_.insert(std::pair<frame_id_t, LinkList::const_iterator>{frame_id, it});
    return true;
  }
  return false;
}

void LRUReplacer::Remove(frame_id_t frame_id) {
  auto it = hash_table_.find(frame_id)->second;
  hash_table_.erase(frame_id);
  list_.erase(it);
}

}  // namespace bustub
