#include "sql/operator/update_logical_operator.h"

 UpdateLogicalOperator::UpdateLogicalOperator(Table *table, const char *attr_name, UpdateStmt *update_stmt)
      : table_(table), update_stmt_(update_stmt), attr_name_(attr_name) {}