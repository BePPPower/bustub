//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

// 疑问：一开始HashJoinKey和HashJoinValue都是放在这个地方的，会报错。放到hash_join_plan.h头文件中去就不报错了

/**
 * ftw- a hash table for hash_join
 */
class SimpleHashJoinHashTable {
 public:
  SimpleHashJoinHashTable() {}

  void Insert(const HashJoinKey &key, const HashJoinValue &value) {
    if (mp_.count(key) == 0) {
      mp_.insert({key, {value}});
      return;
    }
    std::vector<HashJoinValue> &v = mp_.at(key);
    v.emplace_back(value);
  }

  bool GetKeyOf(const HashJoinKey &key, HashJoinValue &value) {
    if (mp_.count(key) == 0) {
      return false;
    }
    if (tmp_values_.size() == 0) {
      tmp_values_ = mp_.at(key);
      tmp_values_it_ = tmp_values_.begin();
    }
    if (tmp_values_it_ == tmp_values_.end()) {
      tmp_values_.clear();
      return false;
    }
    value = *tmp_values_it_;
    ++tmp_values_it_;
    return true;
  }

 private:
  std::unordered_map<HashJoinKey, std::vector<HashJoinValue> > mp_{};

  std::vector<HashJoinValue> tmp_values_{};
  std::vector<HashJoinValue>::iterator tmp_values_it_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** ftw
   * @param[out] values
   */
  void GenarateTupleValues(const Schema *schema, const Tuple &tuple, std::vector<Value> &values);

  Tuple GenerateJoinTuple(const std::vector<Value> &left_row, const std::vector<Value> &right_row);

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** ftw*/
  Transaction *transaction;
  /** ftw- child plan*/
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  const AbstractExpression *left_expression_;
  const AbstractExpression *right_expression_;

  /** ftw- child schema*/
  const Schema *left_schema_;
  const Schema *right_schema_;

  SimpleHashJoinHashTable hash_table_{};

  Tuple right_tuple_;
  RID right_rid_;
};

}  // namespace bustub
