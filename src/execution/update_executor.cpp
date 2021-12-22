//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
  transaction_ = exec_ctx_->GetTransaction();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 1. 调用孩子executor的Next获取到下一条要修改的Tuple，注意要用while循环
  // 2. 用上面生成的Tuple作为参数调用GenerateUpdatedTuple来生成 update_tuple
  // 3. 使用Updatetuple的RID来修改记录，调用TableHeap的函数
  // 4. 修改索引：删除索引中旧值、新增新值到索引

  child_executor_->Init();

  while (child_executor_->Next(tuple, rid)) {
    Tuple update_tuple = GenerateUpdatedTuple(*tuple);
    if (!table_info_->table_->UpdateTuple(update_tuple, *rid, transaction_)) {
      throw "Update Tuple Exception!";
    }

    for (IndexInfo *index : indexs_) {
      index->index_->DeleteEntry(*tuple, *rid, transaction_);
      index->index_->InsertEntry(update_tuple, *rid, transaction_);
    }
  }  // while

  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
