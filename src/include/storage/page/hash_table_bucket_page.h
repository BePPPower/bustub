//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.h
//
// Identification: src/include/storage/page/hash_table_bucket_page.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "common/config.h"
#include "storage/index/int_comparator.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {
/**
 * Store indexed key and and value together within bucket page. Supports
 * non-unique keys.
 *
 * Bucket page format (keys are stored in order):
 *  ----------------------------------------------------------------
 * | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------
 *
 *  Here '+' means concatenation.
 *  The above format omits the space required for the occupied_ and
 *  readable_ arrays. More information is in storage/page/hash_table_page_defs.h.
 *
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
class HashTableBucketPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  HashTableBucketPage() = delete;

  /**
   * Scan the bucket and collect values that have the matching key
   *
   * @return true if at least one key matched
   */
  bool GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result);

  /**
   * Attempts to insert a key and value in the bucket.  Uses the occupied_
   * and readable_ arrays to keep track of each slot's availability.
   *
   * @param key key to insert
   * @param value value to insert
   * @return true if inserted, false if duplicate KV pair or bucket is full
   */
  bool Insert(KeyType key, ValueType value, KeyComparator cmp);

  /**
   * Removes a key and value.
   *
   * @return true if removed, false if not found
   */
  bool Remove(KeyType key, ValueType value, KeyComparator cmp);

  /**
   * Gets the key at an index in the bucket.
   *
   * @param bucket_idx the index in the bucket to get the key at
   * @return key at index bucket_idx of the bucket
   */
  KeyType KeyAt(uint32_t bucket_idx) const;

  /**
   * Gets the value at an index in the bucket.
   *
   * @param bucket_idx the index in the bucket to get the value at
   * @return value at index bucket_idx of the bucket
   */
  ValueType ValueAt(uint32_t bucket_idx) const;

  /**
   * Remove the KV pair at bucket_idx
   */
  void RemoveAt(uint32_t bucket_idx);

  /**
   * Returns whether or not an index is occupied (key/value pair or tombstone)
   *
   * @param bucket_idx index to look at
   * @return true if the index is occupied, false otherwise
   */
  bool IsOccupied(uint32_t bucket_idx) const;

  /**
   * SetOccupied - Updates the bitmap to indicate that the entry at
   * bucket_idx is occupied.
   *
   * @param bucket_idx the index to update
   */
  void SetOccupied(uint32_t bucket_idx);

  /**
   * Returns whether or not an index is readable (valid key/value pair)
   *
   * @param bucket_idx index to lookup
   * @return true if the index is readable, false otherwise
   */
  bool IsReadable(uint32_t bucket_idx) const;

  /**
   * SetReadable - Updates the bitmap to indicate that the entry at
   * bucket_idx is readable.
   *
   * @param bucket_idx the index to update
   */
  void SetReadable(uint32_t bucket_idx);

  /**
   * @return the number of readable elements, i.e. current size
   */
  uint32_t NumReadable();

  /**
   * @return whether the bucket is full
   */
  bool IsFull();

  /**
   * @return whether the bucket is empty
   */
  bool IsEmpty();

  /**
   * Prints the bucket's occupancy information
   */
  void PrintBucket();

 private:
  bool GetBitMapIndex(uint32_t bucket_idx, uint32_t *arrary_idx, uint8_t *bit_idx) const;
  // type=0代表修改occupied_数组，type=1代表修改readable_数组
  void SetZeroBit(uint32_t bucket_idx, uint32_t type);
  void SetOneBit(uint32_t bucket_idx, uint32_t type);

  /**
   * occupied_,readable_数组在(BUCKET_ARRAY_SIZE - 1) / 8无法被整除的情况下，最后一个char的部分空间是可能多余的
   * 一开始我认为 多余的情况下可能导致 buckey_page大小超过4KB
   * 但是仔细看题要知道 HashTableBucketPage 只有那几种情况（可以看hash_table_bucket_page.cpp文件末尾),都是固定长度kv
   * 将那几种情况逐一计算后，可以得到occupied_,readable_这两个数组最后开辟的那个char也没有溢出4096B
   * 这也就解释了为什么 BUCKET_ARRAY_SIZE的计算公式为
   * BUCKET_ARRAY_SIZE的计算公式为= (PAGE_SIZE)/(sizeof(MappingType) + 0.25)
   * 因为这几种情况都不会超出 PAGE_SIZE
   */
  //  For more on BUCKET_ARRAY_SIZE see storage/page/hash_table_page_defs.h
  char occupied_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1];
  // 0 if tombstone/brand new (never occupied), 1 otherwise.
  char readable_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1];
  MappingType array_[0];
};

}  // namespace bustub
