//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
  transaction_ = exec_ctx_->GetTransaction();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 1. 调用孩子executor的Next获取到下一条要删除的Tuple，注意要用while循环
  // 2. 用上面获得的RID，调用TableHeap的MarkDelete删除表中记录
  // 3. 修改索引：删除索引中旧值

  child_executor_->Init();

  while (child_executor_->Next(tuple, rid)) {
    if (!table_info_->table_->MarkDelete(*rid, transaction_)) {
      throw "Delete Tuple Exception!";
    }

    for (IndexInfo *index : indexs_) {
      IndexMetadata *index_meta = index->index_->GetMetadata();
      Tuple delete_tuple =
          tuple->KeyFromTuple(table_info_->schema_, *index_meta->GetKeySchema(), index_meta->GetKeyAttrs());
      index->index_->DeleteEntry(delete_tuple, *rid, transaction_);
    }
  }
  return false;
}

}  // namespace bustub
