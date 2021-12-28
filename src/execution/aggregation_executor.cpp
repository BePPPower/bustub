//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  transaction_ = exec_ctx_->GetTransaction();
  having_ = plan_->GetHaving();

  child_->Init();
  Tuple agg_tuple;
  RID agg_rid;
  while (child_->Next(&agg_tuple, &agg_rid)) {
    AggregateKey key = MakeAggregateKey(&agg_tuple);
    AggregateValue value = MakeAggregateValue(&agg_tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    AggregateKey key = aht_iterator_.Key();
    AggregateValue values = aht_iterator_.Val();
    ++aht_iterator_;
    if (having_) {
      bool res = having_->EvaluateAggregate(key.group_bys_, values.aggregates_).GetAs<bool>();
      if (!res) {
        continue;
      }
    }
    *tuple = GenerateAggregateTuple(key.group_bys_, values.aggregates_);
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

Tuple AggregationExecutor::GenerateAggregateTuple(const std::vector<Value> &group_bys,
                                                  const std::vector<Value> &aggregates) {
  const std::vector<Column> &cols = GetOutputSchema()->GetColumns();
  std::vector<Value> row;
  for (auto col : cols) {
    row.emplace_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregates));
  }
  return Tuple(row, GetOutputSchema());
}

}  // namespace bustub
