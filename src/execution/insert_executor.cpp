//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 这里是一次性把所有的rows全部插入
  if (plan_->IsRawInsert()) {
    for (auto &row : plan_->RawValues()) {
      Tuple tuple(row, &table_info_->schema_);
      InsertTuple(tuple, rid, exec_ctx_->GetTransaction());
    }
  } else {
    child_executor_->Init();

    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      InsertTuple(tuple, &rid, exec_ctx_->GetTransaction());
    }
  }
  return false;
}

void InsertExecutor::InsertTuple(const Tuple &tuple, RID *rid, Transaction *txn) {
  if (!table_info_->table_->InsertTuple(tuple, rid, txn)) {
    throw "Insert Tuple Exception!";
  }
  for (IndexInfo *index : indexs_) {
    index->index_->InsertEntry(tuple, *rid, exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
