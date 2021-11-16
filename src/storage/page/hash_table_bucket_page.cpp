//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  if (IsEmpty()) {
    // LOG_WARN("bucket is empty,can not get any value.");
    return false;
  }
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; ++idx) {
    if (!IsOccupied(idx)) {
      break;
    }
    if (IsReadable(idx) && cmp(key, KeyAt(idx)) == 0) {
      result->emplace_back(array_[idx].second);
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsFull()) {
    // LOG_WARN("bucket is full,can not insert anything.");
    return false;
  }
  bool flag = false;
  uint32_t insert_index = 0;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      if (!flag) {
        insert_index = i;
      }
      break;
    }
    if (IsReadable(i)) {
      if (cmp(key, KeyAt(i)) == 0 && value == ValueAt(i)) {
        return false;
      }
    } else if (!flag) {
      insert_index = i;
      flag = true;
    }
  }
  MappingType new_key_value = {key, value};
  array_[insert_index] = new_key_value;
  SetReadable(insert_index);
  if (!IsOccupied(insert_index)) {
    SetOccupied(insert_index);
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsEmpty()) {
    return false;
  }
  bool res = false;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      if (cmp(key, KeyAt(i)) == 0 && value == ValueAt(i)) {
        RemoveAt(i);
        res = true;  // 这里应该可以return true. 因为 kv对是唯一的。
      }
    }
  }
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  if (!IsReadable(bucket_idx)) {
    return {};
  }
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if (!IsReadable(bucket_idx)) {
    return {};
  }
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  if (!IsReadable(bucket_idx)) {
    return;
  }
  uint32_t arrary_idx;
  uint8_t bit_idx;
  if (!GetBitMapIndex(bucket_idx, &arrary_idx, &bit_idx)) {
    return;
  }
  readable_[arrary_idx] &= (~(1 << bit_idx));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint32_t arrary_idx;
  uint8_t bit_idx;
  if (!GetBitMapIndex(bucket_idx, &arrary_idx, &bit_idx)) {
    return false;
  }
  return (occupied_[arrary_idx] & (1 << bit_idx)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t arrary_idx;
  uint8_t bit_idx;
  if (!GetBitMapIndex(bucket_idx, &arrary_idx, &bit_idx)) {
    return;
  }
  occupied_[arrary_idx] |= (1 << bit_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t arrary_idx;
  uint8_t bit_idx;
  if (!GetBitMapIndex(bucket_idx, &arrary_idx, &bit_idx)) {
    return false;
  }
  return (readable_[arrary_idx] & (1 << bit_idx)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t arrary_idx;
  uint8_t bit_idx;
  if (!GetBitMapIndex(bucket_idx, &arrary_idx, &bit_idx)) {
    return;
  }
  readable_[arrary_idx] |= (1 << bit_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    if (!IsOccupied(bucket_idx)) {
      return false;
    }
    if (!IsReadable(bucket_idx)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t count = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx)) {
      ++count;
    }
  }
  return count;
}

/**
 * 这里默认了char[]初始化为全0，是否正确？ 正确
 * 因为bucketPage最后是要放在Page的data_数组中的，而Page的构造函数已经将data_初始化位全0
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetBitMapIndex(uint32_t bucket_idx, uint32_t *arrary_idx, uint8_t *bit_idx) const {
  *arrary_idx = bucket_idx / 8;
  if (*arrary_idx >= (BUCKET_ARRAY_SIZE - 1) / 8 + 1) {
    LOG_ERROR("ararrary_idx out of range.");
    return false;
  }
  *bit_idx = static_cast<uint8_t>(bucket_idx % 8);
  return true;
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
