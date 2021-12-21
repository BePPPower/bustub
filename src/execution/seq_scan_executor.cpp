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
    *tuple = *table_iterator_;
    *rid = tuple->GetRid();
    ++table_iterator_;
    bool is_ok = true;
    if (plan_->GetPredicate()) {
      Value res = plan_->GetPredicate()->Evaluate(tuple, &(table_info_->schema_));
      is_ok = res.GetAs<bool>();
    }
    if (is_ok) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
