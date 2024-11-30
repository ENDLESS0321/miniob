#include "sql/executor/update_executor.h"
#include "common/type/data_type.h"
#include "sql/operator/update_physical_operator.h"

#include "common/log/log.h"
#include "common/types.h"
#include "common/type/attr_type.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "storage/db/db.h"
#include "storage/record/record_log.h"
#include "sql/expr/expression.h"

#include "sql/operator/table_scan_physical_operator.h"
#include "sql/operator/predicate_physical_operator.h"
#include <memory>

RC UpdateExecutor::execute(SQLStageEvent *sql_event) {
    
    Stmt *stmt = sql_event->stmt();
    SessionEvent *session_event = sql_event->session_event();
    Session *session = session_event->session();
    // Db *db = session->get_current_db();
    Trx *trx = session->current_trx();
    // CLogManager *clog_manager = db->get_clog_manager();
    if (stmt == nullptr) {
        LOG_WARN("cannot find statement");
        return RC::INVALID_ARGUMENT;
    }
    
    UpdateStmt *update_stmt = (UpdateStmt *)stmt;
    Table *table = update_stmt->table();

    // FilterStmt *filter_stmt = update_stmt->filter_stmt();

    // 指向底层表扫描算子的智能指针，使用读写模式
    auto scan_oper_ptr = std::make_unique<TableScanPhysicalOperator>(table, ReadWriteMode::READ_WRITE);

    Value value;

    value.set_type(AttrType::INTS);

    std::unique_ptr<ValueExpr> value_expr_ptr = std::make_unique<ValueExpr>(value);

    // 指向谓词过滤算子的智能指针
    auto pred_oper_ptr = std::make_unique<PredicatePhysicalOperator>(std::move(value_expr_ptr));
    
    // TableScanOperator scan_oper(update_stmt->table());
    // PredicateOperator pred_oper(update_stmt->filter_stmt());

    // 谓词过滤算子指向表扫描算子
    pred_oper_ptr->add_child(std::move(scan_oper_ptr));

    UpdatePhysicalOperator update_oper(table, update_stmt);

    // 实体指向谓词过滤算子，至此火山模型构建完成
    update_oper.add_child(std::move(pred_oper_ptr));
    
    // 返回结果 
    RC rc = update_oper.open(trx);
    
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to open update operator: %s", strrc(rc));
        return rc;
    } else {
        LOG_INFO("open update operator success");
    }
    return rc;
}