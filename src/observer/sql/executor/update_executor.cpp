#include "sql/executor/update_executor.h"
#include "common/type/data_type.h"
#include "sql/operator/update_operator.h"

#include "common/log/log.h"
#include "common/types.h"
#include "common/type/attr_type.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/update_stmt.h"
#include "storage/db/db.h"
#include "storage/record/record_log.h"
#include "sql/expr/expression.h"

#include "sql/operator/table_scan_physical_operator.h"
#include "sql/operator/predicate_physical_operator.h"
#include <memory>

RC UpdateExecutor::execute(SQLStageEvent *sql_event) {
    // Stmt *stmt = sql_event->stmt();
    // Session *session = sql_event->session_event()->session();
    // ASSERT(stmt->type() == StmtType::UPDATE,
    //          "update executor can not run this command: %d",
    //          static_cast<int>(stmt->type()));
    
    // UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);
    
    // const char *table_name = update_stmt->table_name().c_str();
    // RC rc = session->get_current_db()->update_table(table_name);
    // return rc;

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

    TableScanPhysicalOperator scan_oper(update_stmt->table(), ReadWriteMode::READ_WRITE);

    std::unique_ptr<TableScanPhysicalOperator> scan_oper_ptr = std::make_unique<TableScanPhysicalOperator>(table, ReadWriteMode::READ_WRITE);

    Value value;

    value.set_type(AttrType::INTS);

    std::unique_ptr<ValueExpr> value_expr = std::make_unique<ValueExpr>(value);

    std::unique_ptr<PredicatePhysicalOperator> pred_oper = std::make_unique<PredicatePhysicalOperator>(std::move(value_expr));
    
    // TableScanOperator scan_oper(update_stmt->table());
    // PredicateOperator pred_oper(update_stmt->filter_stmt());
    pred_oper->add_child(std::move(scan_oper_ptr));
    UpdateOperator update_oper(update_stmt, trx);
    update_oper.add_child(std::move(pred_oper));
    RC rc = update_oper.open(trx);
    
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to open update operator: %s", strrc(rc));
        return rc;
    } else {
        LOG_INFO("open update operator success");
    }
    return rc;
}