#pragma once

#include <mutex>
#include "catalog/mssql_table_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class LogicalDelete;
class LogicalUpdate;

struct MSSQLDirectDMLTarget {
	string catalog_name;
	string operation;
	string sql;
};

class MSSQLDirectDMLPlanner {
public:
	static bool TryBuildUpdate(ClientContext &context, LogicalUpdate &op, MSSQLTableEntry &table_entry,
							   MSSQLDirectDMLTarget &target, string &reason);

	static bool TryBuildDelete(ClientContext &context, LogicalDelete &op, MSSQLTableEntry &table_entry,
							   MSSQLDirectDMLTarget &target, string &reason);
};

class MSSQLPhysicalDirectDML : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	MSSQLPhysicalDirectDML(PhysicalPlan &plan, vector<LogicalType> types, idx_t estimated_cardinality,
						   MSSQLDirectDMLTarget target);

	string GetName() const override {
		return "MSSQL_DIRECT_DML";
	}

	bool IsSource() const override {
		return true;
	}

	OrderPreservationType SourceOrder() const override {
		return OrderPreservationType::NO_ORDER;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
									 OperatorSourceInput &input) const override;

private:
	MSSQLDirectDMLTarget target_;
};

class MSSQLDirectDMLGlobalSourceState : public GlobalSourceState {
public:
	bool executed = false;
	bool returned = false;
	idx_t rows_affected = 0;
	std::mutex mutex;
};

}  // namespace duckdb
