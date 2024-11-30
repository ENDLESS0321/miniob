//update_operator.h
#pragma once
#include "sql/expr/expression_tuple.h"
#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include "sql/stmt/update_stmt.h"
#include "storage/trx/trx.h"
class UpdateStmt;
class UpdateOperator : public PhysicalOperator {
public:
    UpdateOperator(UpdateStmt *update_stmt, Trx *trx)
        : update_stmt_(update_stmt), trx_(trx) {}

    virtual ~UpdateOperator() = default;

    // 实现type()方法
    PhysicalOperatorType type() const override {
        return PhysicalOperatorType::INSERT;
    }

    RC open(Trx *trx) override;
    RC next() override;
    RC close() override;

    Tuple *current_tuple() override;

private:
    UpdateStmt *update_stmt_ = nullptr;
    Trx *trx_ = nullptr;
};