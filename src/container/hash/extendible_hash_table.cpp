//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(std::string name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : name_{std::move(name)},
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      hash_fn_(std::move(hash_fn)) {
  //  implement me!
  Page *p = buffer_pool_manager_->NewPage(&directory_page_id_);
  if (p == nullptr) {
    LOG_ERROR("create new Page error");
    return;
  }
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(p->GetData());
  dir_page->SetPageId(directory_page_id_);
  dir_page->InitDirectPage();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  auto dir_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(dir_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto bukcet_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bukcet_page_id);

  Page *page = reinterpret_cast<Page *>(bucket_page);
  page->RLatch();

  bool res = bucket_page->GetValue(key, comparator_, result);

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bukcet_page_id, false, nullptr);
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page_ptr = FetchDirectoryPage();
  page_id_t bukcet_page_id = KeyToPageId(key, dir_page_ptr);
  HASH_TABLE_BUCKET_TYPE *bucket_page_ptr;
  if (bukcet_page_id == INVALID_PAGE_ID) {  // 插入extendable_hash_table第一个元素的情况下
    bucket_page_ptr =
        reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&bukcet_page_id)->GetData());
    if (bucket_page_ptr == nullptr) {
      table_latch_.RUnlock();
      buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
      return false;
    }
    auto buckect_index = KeyToDirectoryIndex(key, dir_page_ptr);
    dir_page_ptr->SetBucketPageId(buckect_index, bukcet_page_id);
  } else {
    bucket_page_ptr = FetchBucketPage(bukcet_page_id);
  }

  bool res = true;

  Page *page = reinterpret_cast<Page *>(bucket_page_ptr);
  page->WLatch();

  if (!bucket_page_ptr->Insert(key, value, comparator_)) {
    if (!bucket_page_ptr->IsFull()) {
      page->WUnlatch();
      table_latch_.RUnlock();
      res = false;
    } else {
      page->WUnlatch();
      table_latch_.RUnlock();
      if (SplitInsert(transaction, key, value, dir_page_ptr, bucket_page_ptr)) {
        res = Insert(transaction, key, value);
      } else {  // 分裂失败，所以insert false
        res = false;
      }
    }
  } else {
    page->WUnlatch();
    table_latch_.RUnlock();
  }

  buffer_pool_manager_->UnpinPage(bukcet_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value,
                                  HashTableDirectoryPage *dir_page_ptr, HASH_TABLE_BUCKET_TYPE *bucket_page_ptr) {
  table_latch_.WLock();
  uint32_t bukcet_page_idx = KeyToDirectoryIndex(key, dir_page_ptr);
  auto buckect_local_depth = dir_page_ptr->GetLocalDepth(bukcet_page_idx);
  if (buckect_local_depth == MAX_DEPTH) {
    table_latch_.WUnlock();
    return false;
  }
  auto globale_depth = dir_page_ptr->GetGlobalDepth();

  // 分裂修改depth
  if (buckect_local_depth == globale_depth) {
    dir_page_ptr->IncrGlobalDepth();
    dir_page_ptr->IncrLocalDepth(bukcet_page_idx);
    uint32_t dir_size = dir_page_ptr->Size();
    for (uint32_t idx = 0; idx < dir_size / 2; ++idx) {
      auto local_depth = dir_page_ptr->GetLocalDepth(idx);
      dir_page_ptr->SetLocalDepth(idx + dir_size / 2, local_depth);
      auto page_id = dir_page_ptr->GetBucketPageId(idx);
      dir_page_ptr->SetBucketPageId(idx + dir_size / 2, page_id);
    }
  } else {
    // 修改localdepth
    uint32_t bucket_latest_bit = dir_page_ptr->GetLocalHighBit(bukcet_page_idx);
    uint32_t move_num = dir_page_ptr->GetLocalDepth(bukcet_page_idx);
    for (uint32_t idx = bucket_latest_bit; idx < dir_page_ptr->Size(); idx += (1 << move_num)) {
      dir_page_ptr->IncrLocalDepth(idx);
    }
  }

  // 新建一个bucket_page
  page_id_t new_page_id;
  Page *new_page_ptr = buffer_pool_manager_->NewPage(&new_page_id);
  HASH_TABLE_BUCKET_TYPE *new_bucket_page_ptr = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page_ptr->GetData());

  // 分配记录
  new_page_ptr->WLatch();

  uint32_t pair_bucket_latest_bit = GetPairLatestBit(dir_page_ptr, bukcet_page_idx);
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    KeyType key_tmp = bucket_page_ptr->KeyAt(i);
    ValueType value_tmp = bucket_page_ptr->ValueAt(i);
    uint32_t latest_bit = Hash(key_tmp) & dir_page_ptr->GetLocalDepthMask(bukcet_page_idx);
    if (pair_bucket_latest_bit == latest_bit) {
      new_bucket_page_ptr->Insert(key_tmp, value_tmp, comparator_);
      bucket_page_ptr->RemoveAt(i);
    }
  }

  new_page_ptr->WUnlatch();
  // 修改direct的索引
  uint32_t begin_pair_idx = pair_bucket_latest_bit;
  for (uint32_t idx = begin_pair_idx; idx < dir_page_ptr->Size();
       idx += (1 << dir_page_ptr->GetLocalDepth(bukcet_page_idx))) {
    dir_page_ptr->SetBucketPageId(idx, new_page_id);
  }
  buffer_pool_manager_->UnpinPage(new_page_id, true, nullptr);
  table_latch_.WUnlock();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page_ptr = FetchDirectoryPage();
  page_id_t bukcet_page_id = KeyToPageId(key, dir_page_ptr);
  HASH_TABLE_BUCKET_TYPE *bucket_page_ptr = FetchBucketPage(bukcet_page_id);
  if (bucket_page_ptr == nullptr) {
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    return false;
  }

  // remove
  Page *page = reinterpret_cast<Page *>(bucket_page_ptr);
  page->WLatch();

  if (!bucket_page_ptr->Remove(key, value, comparator_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bukcet_page_id, false, nullptr);
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    return false;
  }

  uint32_t bucket_page_idx = KeyToDirectoryIndex(key, dir_page_ptr);
  if (bucket_page_ptr->IsEmpty()) {
    page->WUnlatch();
    table_latch_.RUnlock();
    if (Merge(transaction, dir_page_ptr, bucket_page_idx)) {
      // 这里不用解锁，因为上面已经解了
      buffer_pool_manager_->UnpinPage(bukcet_page_id, true, nullptr);
      buffer_pool_manager_->DeletePage(bukcet_page_id);
    } else {
      buffer_pool_manager_->UnpinPage(bukcet_page_id, true, nullptr);
    }
  } else {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bukcet_page_id, true, nullptr);
    table_latch_.RUnlock();
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Merge(Transaction *transaction, HashTableDirectoryPage *dir_page_ptr, uint32_t bukcet_page_idx) {
  table_latch_.WLock();

  // auto bucket_page_id = dir_page->GetBucketPageId(dir_idx);
  // HASH_TABLE_BUCKET_TYPE *bucket_page_ptr = FetchBucketPage(bukcet_page_id);
  // Page *page = reinterpret_cast<Page *>(bucket_page_ptr);
  // page->RLatch();
  // if (!bucket_page_ptr->IsEmpty()) {
  //   page->RUnlatch();
  //   buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  //   table_latch_.WUnlock();
  //   return false;
  // }

  if (dir_page_ptr->GetLocalDepth(bukcet_page_idx) == 0) {
    table_latch_.WUnlock();
    return false;
  }

  uint32_t bucket_local_depth = dir_page_ptr->GetLocalDepth(bukcet_page_idx);
  uint32_t pair_bucket_idx = GetPairIndex(dir_page_ptr, bukcet_page_idx);
  if (dir_page_ptr->GetLocalDepth(pair_bucket_idx) != bucket_local_depth) {
    table_latch_.WUnlock();
    return false;
  }

  // 修改direct的索引
  uint32_t bucket_latest_bit = dir_page_ptr->GetLocalHighBit(bukcet_page_idx);
  page_id_t pair_page_id = dir_page_ptr->GetBucketPageId(pair_bucket_idx);
  for (uint32_t idx = bucket_latest_bit; idx < dir_page_ptr->Size();
       idx += (1 << dir_page_ptr->GetLocalDepth(bukcet_page_idx))) {
    dir_page_ptr->SetBucketPageId(idx, pair_page_id);
  }

  // 修改localdepth
  dir_page_ptr->DecrLocalDepth(pair_bucket_idx);
  uint32_t new_bucket_latest_bit = dir_page_ptr->GetLocalHighBit(pair_bucket_idx);
  for (uint32_t idx = new_bucket_latest_bit; idx < dir_page_ptr->Size();
       idx += (1 << dir_page_ptr->GetLocalDepth(pair_bucket_idx))) {
    if (idx == pair_bucket_idx) {
      continue;
    }
    dir_page_ptr->DecrLocalDepth(idx);
  }

  Shrink(dir_page_ptr);
  table_latch_.WUnlock();
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Shrink(HashTableDirectoryPage *dir_page_ptr) {
  if (!dir_page_ptr->CanShrink()) {
    LOG_DEBUG("merge success,now globaldepth=%d", dir_page_ptr->GetGlobalDepth());
    return false;
  }
  uint32_t global_depth = dir_page_ptr->GetGlobalDepth();
  for (uint32_t idx = 0; idx < dir_page_ptr->Size(); ++idx) {
    if (dir_page_ptr->GetLocalDepth(idx) == global_depth) {
      return false;
    }
  }
  dir_page_ptr->DecrGlobalDepth();
  LOG_DEBUG("merge success,now globaldepth=%d", dir_page_ptr->GetGlobalDepth());
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetPairLatestBit(HashTableDirectoryPage *dir_page_ptr, uint32_t bucket_idx) {
  uint32_t bucket_latest_bit = dir_page_ptr->GetLocalHighBit(bucket_idx);
  return bucket_latest_bit ^ (1 << (dir_page_ptr->GetLocalDepth(bucket_idx) - 1));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetPairIndex(HashTableDirectoryPage *dir_page_ptr, uint32_t bucket_idx) {
  return bucket_idx ^ (1 << (dir_page_ptr->GetLocalDepth(bucket_idx) - 1));
}
/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  // dir_page->PrintDirectory();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
