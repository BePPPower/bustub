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
  if (list_.size() > 0) {
    *frame_id = list_.front();
    Remove(*frame_id);
    return true;
  }
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

bool LRUReplacer::IsExist(frame_id_t frame_id) const {
  if (hash_table_.find(frame_id) != hash_table_.end()) {
    return true;
  }
  return false;
}

bool LRUReplacer::Put(frame_id_t frame_id) {
  if (list_.size() < num_pages_) {
    list_.push_back(frame_id);
    auto it = prev(list_.cend());
    hash_table_.insert(std::pair<frame_id_t, LinkList::const_iterator>{frame_id, it});
    return true;
  }
  return false;
}

void LRUReplacer::Remove(frame_id_t frame_id) {
  auto it = hash_table_.find(frame_id)->second;
  list_.erase(it);
  hash_table_.erase(frame_id);
}

void LRUReplacer::Flush(frame_id_t frame_id) {
  Remove(frame_id);
  list_.push_back(frame_id);
  auto it = prev(list_.cend());
  hash_table_.insert(std::pair<frame_id_t, LinkList::const_iterator>{frame_id, it});
}

size_t LRUReplacer::Size() { return list_.size(); }

}  // namespace bustub
