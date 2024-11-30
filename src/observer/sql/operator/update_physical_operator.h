//update_operator.h
#pragma once
#include "sql/expr/expression_tuple.h"
#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include "sql/stmt/update_stmt.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include <vector>
class UpdateStmt;
class UpdatePhysicalOperator : public PhysicalOperator {
public:
    UpdatePhysicalOperator(Table *table, UpdateStmt *update_stmt) : table_(table), update_stmt_(update_stmt) {}

    virtual ~UpdatePhysicalOperator() = default;

    // 实现type()方法
    PhysicalOperatorType type() const override {
        return PhysicalOperatorType::UPDATE;
    }

    RC open(Trx *trx) override;
    RC next() override;
    RC close() override;

    Tuple *current_tuple() override;

private:
    Table               *table_ = nullptr;
    Trx                 *trx_ = nullptr;
    UpdateStmt          *update_stmt_ = nullptr;    
    std::vector<Record> records_;
};