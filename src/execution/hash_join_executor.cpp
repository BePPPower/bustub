//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_schema_ = left_executor_->GetOutputSchema();
  right_schema_ = right_executor_->GetOutputSchema();

  left_expression_ = plan_->LeftJoinKeyExpression();
  right_expression_ = plan_->RightJoinKeyExpression();

  left_executor_->Init();
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    Value key_value = left_expression_->EvaluateJoin(&tuple, left_schema_, nullptr, nullptr);
    HashJoinKey key(std::move(key_value));
    std::vector<Value> row;
    GenarateTupleValues(left_schema_, tuple, &row);
    HashJoinValue value(std::move(row));
    hash_table_.Insert(key, value);
  }

  right_executor_->Init();
  right_executor_->Next(&right_tuple_, &right_rid_);
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (!(right_rid_ == RID())) {
    Value right_key = right_expression_->EvaluateJoin(nullptr, nullptr, &right_tuple_, right_schema_);
    HashJoinKey key(std::move(right_key));
    HashJoinValue value({});
    while (hash_table_.GetKeyOf(key, &value)) {
      const std::vector<Value> &left_row = value.GetValues();
      std::vector<Value> right_row{};
      GenarateTupleValues(right_schema_, right_tuple_, &right_row);
      *tuple = GenerateJoinTuple(left_row, right_tuple_);
      *rid = tuple->GetRid();
      return true;
    }
    right_executor_->Next(&right_tuple_, &right_rid_);
  }
  return false;
}

void HashJoinExecutor::GenarateTupleValues(const Schema *schema, const Tuple &tuple, std::vector<Value> *values) {
  uint32_t col_count = schema->GetColumnCount();
  for (uint32_t idx = 0; idx < col_count; ++idx) {
    values->emplace_back(tuple.GetValue(schema, idx));
  }
}

Tuple HashJoinExecutor::GenerateJoinTuple(const std::vector<Value> &left_values, const Tuple &right_tuple) {
  /** left table、right table and out tbale*/
  const Schema *schema = GetOutputSchema();
  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();
  Tuple left_tuple(left_values, left_schema);

  const std::vector<Column> &cols = schema->GetColumns();
  std::vector<Value> values(cols.size());
  for (size_t idx = 0; idx < cols.size(); ++idx) {
    values[idx] = cols[idx].GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
  }
  return Tuple(values, schema);
}

}  // namespace bustub
