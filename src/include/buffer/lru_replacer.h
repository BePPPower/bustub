//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
#include "common/logger.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);  //阻止隐式转换

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  // void Flush(frame_id_t frame_id);

  size_t Size() override;

 private:
  // TODO(student): implement me!

  bool IsExist(frame_id_t frame_id);
  bool Put(frame_id_t frame_id);
  void Remove(frame_id_t frame_id);

  using LinkList = std::list<frame_id_t>;
  using HashTable = std::unordered_map<frame_id_t, LinkList::const_iterator>;
  /**记录maximum number of pages*/
  size_t num_pages_;
  /**双向链表*/
  LinkList list_{};
  /**HashTable*/
  HashTable hash_table_{};

  std::mutex latch_;
};

}  // namespace bustub
