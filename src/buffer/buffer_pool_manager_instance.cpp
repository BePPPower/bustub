//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!

  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];

  bool res = FlushFrameDir(frame_id);
  // 最后释放锁，以防还没flush就有delete请求
  latch_.unlock();
  return res;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  latch_.lock();
  for (size_t i = 0; i < pool_size_; ++i) {
    // TODO(ftw) 这里要不要开多个线程并发进行？否则这就是串行的Flush，后面的会可能会被前面的拖住。
    pages_[i].RLatch();
    if (pages_[i].GetPageId() != INVALID_PAGE_ID && pages_[i].IsDirty()) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    }
    pages_[i].RUnlatch();
  }
  latch_.unlock();
}

/**
 * 创建新的一页，这里的新的一页是逻辑上的概念。
 * 代表要新建一个page_id，page_id就是Page的唯一标识符。
 * newPage肯定是要内存中新建，buffer pool就是DBMS的内存，所以必须在BP中腾出一个frame空间。
 */
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  /**
   * 先从BP中腾出一个frame
   */
  latch_.lock();
  frame_id_t new_frame_id;
  if (!GetAnFrame(&new_frame_id)) {
    latch_.unlock();
    return nullptr;
  }

  pages_[new_frame_id].WLatch();

  // 应该先判断是否还有空间，再来调用AllocatePage(),因为这个函数会使page id变大
  *page_id = AllocatePage();
  ResetFrameMetadata(new_frame_id, *page_id);
  pages_[new_frame_id].pin_count_ = 1;
  // replacer_->Pin(new_frame_id);    起初该frame并不在lru中，所以不需要Pin
  pages_[new_frame_id].WUnlatch();
  page_table_[*page_id] = new_frame_id;
  latch_.unlock();
  return &pages_[new_frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  if (page_id == INVALID_PAGE_ID) {
    LOG_WARN("invalid page id = %d !", page_id);
    return nullptr;
  }

  /**查看这个page_id是否在自己buffer pool中
   * 如果不在，就要去磁盘中取。
   */
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page *p = &pages_[frame_id];
    p->WLatch();
    ++p->pin_count_;
    p->WUnlatch();
    replacer_->Pin(frame_id);
    latch_.unlock();
    return p;
  }

  /**
   * 去磁盘中取该page_id,取之前应该先将buffer pool中腾出一个frame用于存放该page。
   */
  frame_id_t frame_id;
  if (!GetAnFrame(&frame_id)) {
    LOG_ERROR("Get An frame fail");
    latch_.unlock();
    return nullptr;  // 如果既没有free page也无法从lru从剔除页面，就返回nullptr
  }

  pages_[frame_id].WLatch();
  pages_[frame_id].page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].WUnlatch();

  page_table_[page_id] = frame_id;
  latch_.unlock();
  return &pages_[frame_id];
}

/**
 * DeletePg是删除一页，应该回收该页的page_id并重新利用,这是逻辑上的Delete，对应上面的NewPgImp
 * 应该需要新的数据结构（如队列）来维护被删除的page_id列表，并可以被Allocate重新利用
 */
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];

  pages_[frame_id].RLatch();
  if (pages_[frame_id].GetPinCount() > 0) {
    pages_[frame_id].RUnlatch();
    latch_.unlock();
    return false;
  }
  pages_[frame_id].RUnlatch();

  pages_[frame_id].WLatch();
  ResetFrameMetadata(frame_id, INVALID_PAGE_ID);
  pages_[frame_id].WUnlatch();

  this->free_list_.emplace_back(frame_id);

  page_table_.erase(page_id);  // 这一步必须在GetPinCount()<= 0判断之后才可以做
  latch_.unlock();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  latch_.lock();

  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_WARN("The page id isn't in BP!");
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];

  pages_[frame_id].WLatch();
  if (pages_[frame_id].GetPinCount() <= 0) {
    pages_[frame_id].WUnlatch();
    latch_.unlock();
    return false;
  }

  /**
   * Unpin到0时就可以加入到lru中了。
   */
  pages_[frame_id].is_dirty_ = pages_[frame_id].is_dirty_ || is_dirty;
  --pages_[frame_id].pin_count_;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }
  pages_[frame_id].WUnlatch();
  latch_.unlock();
  return true;
}

// 根据这里的实现可以知道各个BP实例管理的page id规律
//  BPI:         0           1          2
//  pageID:   0,3,6...     1,4,7...    2,5,8...
page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

bool BufferPoolManagerInstance::GetFrameFromFreeList(frame_id_t *frame_id) {
  if (free_list_.empty()) {
    return false;
  }
  *frame_id = free_list_.front();
  free_list_.pop_front();
  return true;
}

// 调用这个函数前后必须对page_table_加锁
bool BufferPoolManagerInstance::GetAnFrame(frame_id_t *frame_id) {
  // free_list无剩余空间
  if (!GetFrameFromFreeList(frame_id)) {
    // lru无剩余空间
    if (!replacer_->Victim(frame_id)) {
      return false;
    }
    // 将lru释放出来的page写回磁盘
    if (!FlushFrameDir(*frame_id)) {
      return false;
    }
    page_table_.erase(pages_[*frame_id].GetPageId());
  }
  return true;
}
bool BufferPoolManagerInstance::FlushFrameDir(const frame_id_t frame_id) {
  Page *p = &pages_[frame_id];

  p->RLatch();
  if (p->GetPageId() == INVALID_PAGE_ID) {
    p->RUnlatch();
    return false;
  }
  if (p->IsDirty()) {
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
  }
  p->RUnlatch();

  return true;
}

void BufferPoolManagerInstance::ResetFrameMetadata(frame_id_t frame_id, page_id_t page_id) {
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
}

}  // namespace bustub
