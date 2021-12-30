//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  Catalog *cata_log = exec_ctx_->GetCatalog();
  table_info_ = cata_log->GetTable(plan_->GetTableOid());
  /** 感觉这样做并不安全，因为table_现在指向的一个对象被Unique_ptr指向，这个对象可能被unique_ptr释放 */
  table_ = table_info_->table_.get();
  table_iterator_ = table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (table_iterator_ != table_->End()) {
    Tuple next_tuple = *table_iterator_;
    ++table_iterator_;
    if (plan_->GetPredicate() != nullptr) {
      bool is_ok = plan_->GetPredicate()->Evaluate(&next_tuple, &(table_info_->schema_)).GetAs<bool>();
      if (!is_ok) {
        continue;
      }
    }
    *tuple = GenerateSeqScanTuple(&next_tuple, GetOutputSchema());
    *rid = next_tuple.GetRid();
    return true;
  }
  // 这一步的目的是让rid指向一个空RID,这样上层调用函数就可以根据这个来判断是否到达Next末尾。
  *tuple = Tuple();
  *rid = RID();
  return false;
}

Tuple SeqScanExecutor::GenerateSeqScanTuple(const Tuple *tuple, const Schema *schema) {
  const std::vector<Column> &cols = schema->GetColumns();
  std::vector<Value> values(cols.size());
  for (size_t idx = 0; idx < cols.size(); ++idx) {
    values[idx] = cols[idx].GetExpr()->Evaluate(tuple, &(table_info_->schema_));
  }
  return Tuple(values, schema);
}

}  // namespace bustub
