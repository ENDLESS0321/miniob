#pragma once

#include "common/rc.h"

class SQLStageEvent;

/**
 * @brief 更新表的执行器
 * @ingroup Executor
 */

class UpdateExecutor
{
public:
  UpdateExecutor() = default;
  virtual ~UpdateExecutor() = default;

  RC execute(SQLStageEvent *sql_event);
};