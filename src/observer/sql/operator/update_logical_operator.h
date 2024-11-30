#pragma once

#include "common/value.h"
#include "sql/operator/logical_operator.h"
#include "sql/stmt/update_stmt.h"
#include <vector>

/**
 * @brief 逻辑算子，用于执行delete语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, const char *attr_name, UpdateStmt *update_stmt);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  Table              *table() const { return table_; }
  const char *attr_name() const { return attr_name_; }
  UpdateStmt *update_stmt() const { return update_stmt_; }

private:
  Table *table_ = nullptr;
  UpdateStmt *update_stmt_ = nullptr;
  const char *attr_name_;
};
