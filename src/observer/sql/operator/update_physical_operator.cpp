#include "update_physical_operator.h"
#include "common/log/log.h"
#include "sql/operator/physical_operator.h"
RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];

  RC rc = child->open(trx);

  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    records_.emplace_back(std::move(record));
  }

  child->close();

  for (Record &record : records_) {
    rc = trx_->update_record(table_, &record, update_stmt_->attr_name(), update_stmt_->value());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
  
  // LOG_WARN("fuck 114514");
  
  // if (children_.size() != 1) {
  //   LOG_WARN("update operator must has 1 child");
  //   return RC::INTERNAL;
  // }

  // PhysicalOperator *child = children_[0].get(); // get()返回智能指针的原始指针
  // RC                rc    = child->open(trx);
  // if (rc != RC::SUCCESS) {
  //   LOG_WARN("failed to open child operator: %s", strrc(rc));
  //   return rc;
  // }
  // Table *table = update_stmt_->table();
  
  // LOG_WARN("fuck 2");
  
  // while (RC::SUCCESS == (rc = child->next())) {
  //   LOG_WARN("fuck 3");
  //   Tuple *tuple = child->current_tuple();
  //   if (nullptr == tuple) {
  //     LOG_WARN("failed to get current record: %s", strrc(rc));
  //     return rc;
  //   }
  //   RowTuple    *row_tuple = static_cast<RowTuple *>(tuple);
  //   Record      &record    = row_tuple->record();
  //   const Value *value     = update_stmt_->value();
  //   rc                     = table->update_record(trx_, &record, update_stmt_->attr_name(), value);
  //   if (rc != RC::SUCCESS) {
  //     LOG_WARN("failed to update record: %s", strrc(rc));
  //     return rc;
  //   }
  // }

  // Tuple *tuple = child->current_tuple();
  // if (tuple == nullptr) {
  //   LOG_WARN("failed to get current record: %s", strrc(rc));
  //   return rc;
  // }

  // LOG_WARN("11111111111111111111");

  // RowTuple    *row_tuple = static_cast<RowTuple *>(tuple);
  // Record      &record    = row_tuple->record();
  // const Value *value     = update_stmt_->value();
  // rc                     = table->update_record(trx_, &record, update_stmt_->attr_name(), value);
  // if (rc != RC::SUCCESS) {
  //   LOG_WARN("failed to update record: %s", strrc(rc));
  //   return rc;
  // }


  // return rc;
}

RC UpdatePhysicalOperator::next() { return RC::RECORD_EOF; }

RC UpdatePhysicalOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}

// RC UpdatePhysicalOperator::open(Trx *trx) {
//   LOG_WARN("Opening UpdatePhysicalOperator...");

//   if (children_.size() != 1) {
//     LOG_WARN("update operator must have 1 child");
//     return RC::INTERNAL;
//   }

//   PhysicalOperator *child = children_[0].get(); // 获取子操作符
//   RC rc = child->open(trx);  // 打开子操作符
//   if (rc != RC::SUCCESS) {
//     LOG_WARN("failed to open child operator: %s", strrc(rc));
//     return rc;
//   }

//   return RC::SUCCESS;
// }

// RC UpdatePhysicalOperator::next() {
//   // 获取子操作符
//   PhysicalOperator *child = children_[0].get();

//   // 从子操作符获取下一个tuple
//   RC rc = child->next();
//   if (rc != RC::SUCCESS) {
//     return RC::RECORD_EOF;  // 如果没有更多tuple，返回EOF
//   }

//   // 获取当前tuple
//   Tuple *tuple = child->current_tuple();
//   if (tuple == nullptr) {
//     return RC::RECORD_EOF;  // 如果当前tuple为空，返回EOF
//   }

//   RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
//   Record &record = row_tuple->record();
//   const Value *value = update_stmt_->value();
  
//   // 执行记录更新
//   Table *table = update_stmt_->table();
//   rc = table->update_record(trx_, &record, update_stmt_->attr_name(), value);
//   if (rc != RC::SUCCESS) {
//     LOG_WARN("failed to update record: %s", strrc(rc));
//     return rc;
//   }

//   return RC::SUCCESS;
// }

// RC UpdatePhysicalOperator::close() {
//   if (!children_.empty()) {
//     children_[0]->close();  // 关闭子操作符
//   }
//   return RC::SUCCESS;
// }

Tuple *UpdatePhysicalOperator::current_tuple() {
  // 处理逻辑，获取子操作符的当前 Tuple
  if (children_.empty()) {
    return nullptr;  // 如果没有子操作符，直接返回空指针
  }

  // 获取第一个子操作符的 Tuple
  PhysicalOperator *child = children_[0].get();
  return child->current_tuple();
}