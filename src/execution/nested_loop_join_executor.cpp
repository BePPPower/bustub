//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_plan_ = plan_->GetLeftPlan();
  right_plan_ = plan_->GetRightPlan();
  left_executor_->Init();
  left_executor_->Next(&left_tuple_, &left_rid_);
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (!(left_rid_ == RID())) {
    Tuple right_tuple;
    RID right_rid;

    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (!plan_->Predicate()) {
        throw "NestLoopJoin Exception: join predicate is nullptr";
      }
      bool res = plan_->Predicate()
                     ->EvaluateJoin(&left_tuple_, left_plan_->OutputSchema(), &right_tuple, right_plan_->OutputSchema())
                     .GetAs<bool>();
      if (res) {
        *tuple = GenerateJoinTuple(left_tuple_, right_tuple);
        *rid = tuple->GetRid();
        return true;
      }
    }
    right_executor_->Init();
    left_executor_->Next(&left_tuple_, &left_rid_);
  }
  return false;
}

Tuple NestedLoopJoinExecutor::GenerateJoinTuple(const Tuple &left_tuple, const Tuple &right_tuple) {
  /** left table、right table and out tbale*/
  const Schema *schema = GetOutputSchema();
  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();
  std::vector<Value> values;

  const std::vector<Column> &cols = schema->GetColumns();
  for (auto col : cols) {
    Value value = col.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
    values.emplace_back(value);
  }

  return Tuple(values, schema);
}

}  // namespace bustub
