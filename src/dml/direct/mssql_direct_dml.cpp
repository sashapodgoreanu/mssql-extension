#include "dml/direct/mssql_direct_dml.hpp"

#include "catalog/mssql_catalog.hpp"
#include "connection/mssql_connection_provider.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "mssql_functions.hpp"
#include "query/mssql_simple_query.hpp"
#include "table_scan/filter_encoder.hpp"

#include <cctype>
#include <cstdio>
#include <cstdlib>

static int GetDirectDMLDebugLevel() {
	static int level = -1;
	if (level == -1) {
		const char *env = std::getenv("MSSQL_DEBUG");
		level = env ? std::atoi(env) : 0;
	}
	return level;
}

#define DIRECT_DML_DEBUG(level, fmt, ...)                                   \
	do {                                                                    \
		if (GetDirectDMLDebugLevel() >= level) {                            \
			fprintf(stderr, "[MSSQL DIRECT DML] " fmt "\n", ##__VA_ARGS__); \
		}                                                                   \
	} while (0)

namespace duckdb {

using mssql::ExpressionEncodeContext;
using mssql::FilterEncoder;

namespace {

struct DirectDMLContext {
	vector<column_t> column_ids;
	vector<string> column_names;
	vector<LogicalType> column_types;
};

static string EscapeIdentifier(const string &identifier) {
	return "[" + FilterEncoder::EscapeBracketIdentifier(identifier) + "]";
}

static string FullyQualifiedTableName(const MSSQLTableEntry &table_entry) {
	return EscapeIdentifier(table_entry.ParentSchema().name) + "." + EscapeIdentifier(table_entry.name);
}

static DirectDMLContext BuildContext(const MSSQLTableEntry &table_entry, const LogicalGet &get) {
	DirectDMLContext result;

	for (const auto &column : table_entry.GetMSSQLColumns()) {
		result.column_names.push_back(column.name);
		result.column_types.push_back(column.duckdb_type);
	}

	for (const auto &column_id : get.GetColumnIds()) {
		result.column_ids.push_back(column_id.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID
																: column_id.GetPrimaryIndex());
	}

	return result;
}

static void DebugLogicalTree(LogicalOperator &op, idx_t depth = 0) {
	if (GetDirectDMLDebugLevel() < 2) {
		return;
	}

	string indent(depth * 2, ' ');
	DIRECT_DML_DEBUG(2, "%s%s children=%zu expressions=%zu", indent.c_str(), op.GetName().c_str(), op.children.size(),
					 op.expressions.size());
	for (idx_t i = 0; i < op.expressions.size(); i++) {
		DIRECT_DML_DEBUG(2, "%s  expr[%llu]: %s class=%d type=%d", indent.c_str(), (unsigned long long)i,
						 op.expressions[i]->ToString().c_str(), (int)op.expressions[i]->GetExpressionClass(),
						 (int)op.expressions[i]->type);
	}

	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		DIRECT_DML_DEBUG(2, "%s  get table_index=%llu columns=%zu table_filters=%zu function=%s", indent.c_str(),
						 (unsigned long long)get.table_index, get.GetColumnIds().size(),
						 get.table_filters.filters.size(), get.function.name.c_str());
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		DIRECT_DML_DEBUG(2, "%s  projection table_index=%llu projection_count=%zu", indent.c_str(),
						 (unsigned long long)projection.table_index, projection.expressions.size());
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			DIRECT_DML_DEBUG(2, "%s    projection[%llu]: %s class=%d type=%d", indent.c_str(), (unsigned long long)i,
							 projection.expressions[i]->ToString().c_str(),
							 (int)projection.expressions[i]->GetExpressionClass(),
							 (int)projection.expressions[i]->type);
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		DIRECT_DML_DEBUG(2, "%s  comparison_join type=%d conditions=%zu", indent.c_str(), (int)join.join_type,
						 join.conditions.size());
		for (idx_t i = 0; i < join.conditions.size(); i++) {
			auto &condition = join.conditions[i];
			DIRECT_DML_DEBUG(2, "%s    condition[%llu]: %s %d %s", indent.c_str(), (unsigned long long)i,
							 condition.left->ToString().c_str(), (int)condition.comparison,
							 condition.right->ToString().c_str());
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
		auto &expr_get = op.Cast<LogicalExpressionGet>();
		DIRECT_DML_DEBUG(2, "%s  expression_get table_index=%llu columns=%zu rows=%zu", indent.c_str(),
						 (unsigned long long)expr_get.table_index, expr_get.expr_types.size(),
						 expr_get.expressions.size());
		for (idx_t row_idx = 0; row_idx < expr_get.expressions.size(); row_idx++) {
			for (idx_t col_idx = 0; col_idx < expr_get.expressions[row_idx].size(); col_idx++) {
				DIRECT_DML_DEBUG(2, "%s    value[%llu][%llu]: %s", indent.c_str(), (unsigned long long)row_idx,
								 (unsigned long long)col_idx,
								 expr_get.expressions[row_idx][col_idx]->ToString().c_str());
			}
		}
	}

	for (auto &child : op.children) {
		DebugLogicalTree(*child, depth + 1);
	}
}

static bool ExtractFilteredGet(LogicalOperator &op, LogicalGet *&get, vector<const Expression *> &filters,
							   string &reason) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		get = &op.Cast<LogicalGet>();
		return true;
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &expr : filter.expressions) {
			filters.push_back(expr.get());
		}
		if (filter.children.size() != 1) {
			reason = "filter has multiple children";
			return false;
		}
		return ExtractFilteredGet(*filter.children[0], get, filters, reason);
	}
	default:
		reason = "unsupported logical operator in DML source: " + op.GetName();
		return false;
	}
}

static string QualifyBracketIdentifiers(const string &sql, const string &qualifier) {
	if (qualifier.empty() || sql.empty()) {
		return sql;
	}

	string result;
	result.reserve(sql.size() + qualifier.size() * 4);
	bool in_string = false;
	for (idx_t i = 0; i < sql.size(); i++) {
		char c = sql[i];
		if (c == '\'') {
			result += c;
			if (in_string && i + 1 < sql.size() && sql[i + 1] == '\'') {
				result += sql[++i];
				continue;
			}
			in_string = !in_string;
			continue;
		}
		if (!in_string && c == '[') {
			idx_t prev_idx = result.size();
			while (prev_idx > 0 && std::isspace(static_cast<unsigned char>(result[prev_idx - 1]))) {
				prev_idx--;
			}
			if (prev_idx == 0 || result[prev_idx - 1] != '.') {
				result += EscapeIdentifier(qualifier);
				result += ".";
			}
		}
		result += c;
	}
	return result;
}

static bool EncodeWhereClause(const LogicalGet &get, const DirectDMLContext &ctx,
							  const vector<const Expression *> &filters, string &where_sql, string &reason,
							  const string &column_qualifier = "") {
	vector<string> conditions;
	ExpressionEncodeContext expr_ctx(ctx.column_ids, ctx.column_names, ctx.column_types);
	expr_ctx.column_qualifier = column_qualifier;

	for (const auto *filter : filters) {
		auto encoded = FilterEncoder::EncodeExpression(*filter, expr_ctx);
		if (!encoded.supported || encoded.sql.empty()) {
			reason = "unsupported WHERE expression: " + filter->ToString();
			return false;
		}
		conditions.push_back(encoded.sql);
	}

	auto pushed_filters =
		FilterEncoder::Encode(&get.table_filters, ctx.column_ids, ctx.column_names, ctx.column_types, column_qualifier);
	if (pushed_filters.needs_duckdb_filter) {
		reason = "unsupported pushed-down table filter";
		return false;
	}
	if (!pushed_filters.where_clause.empty()) {
		conditions.push_back(pushed_filters.where_clause);
	}

	if (get.bind_data) {
		auto &bind_data = get.bind_data->Cast<MSSQLCatalogScanBindData>();
		if (!bind_data.complex_filter_where_clause.empty()) {
			conditions.push_back(QualifyBracketIdentifiers(bind_data.complex_filter_where_clause, column_qualifier));
		}
	}

	if (conditions.empty()) {
		where_sql.clear();
		return true;
	}

	where_sql = conditions[0];
	for (idx_t i = 1; i < conditions.size(); i++) {
		where_sql = "(" + where_sql + " AND " + conditions[i] + ")";
	}
	return true;
}

struct ValuesSource {
	idx_t output_table_index = DConstants::INVALID_INDEX;
	vector<LogicalType> types;
	vector<vector<string>> rows;

	idx_t ColumnCount() const {
		return types.size();
	}
};

enum class JoinChildKind : uint8_t { TARGET, VALUES_SOURCE };

struct JoinChildContext {
	JoinChildKind kind;
	string alias;
	idx_t output_table_index;
	const LogicalGet *target_get = nullptr;
	const DirectDMLContext *target_ctx = nullptr;
	const ValuesSource *source = nullptr;

	idx_t ColumnCount() const {
		if (kind == JoinChildKind::TARGET) {
			return target_ctx->column_ids.size();
		}
		return source->ColumnCount();
	}
};

static bool EncodeStandaloneExpression(const Expression &expr, string &sql, string &reason) {
	static const vector<column_t> empty_column_ids;
	static const vector<string> empty_column_names;
	static const vector<LogicalType> empty_column_types;
	ExpressionEncodeContext expr_ctx(empty_column_ids, empty_column_names, empty_column_types);
	auto encoded = FilterEncoder::EncodeExpression(expr, expr_ctx);
	if (!encoded.supported || encoded.sql.empty()) {
		reason = "unsupported VALUES expression: " + expr.ToString();
		return false;
	}
	sql = encoded.sql;
	return true;
}

static bool IsIdentityProjection(const LogicalProjection &projection, idx_t expected_columns) {
	if (projection.expressions.size() != expected_columns) {
		return false;
	}
	for (idx_t i = 0; i < projection.expressions.size(); i++) {
		auto &expr = *projection.expressions[i];
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_REF) {
			return false;
		}
		auto &ref = expr.Cast<BoundReferenceExpression>();
		if (ref.index != i) {
			return false;
		}
	}
	return true;
}

static bool ExtractValuesSource(LogicalOperator &op, ValuesSource &source, string &reason) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		if (projection.children.size() != 1) {
			reason = "VALUES source projection has multiple children";
			return false;
		}
		if (!ExtractValuesSource(*projection.children[0], source, reason)) {
			return false;
		}
		if (!IsIdentityProjection(projection, source.ColumnCount())) {
			reason = "VALUES source projection is not a simple identity projection";
			return false;
		}
		source.output_table_index = projection.table_index;
		return true;
	}

	if (op.type != LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
		reason = "UPDATE FROM source is not a VALUES/EXPRESSION_GET source: " + op.GetName();
		return false;
	}

	auto &expr_get = op.Cast<LogicalExpressionGet>();
	source.output_table_index = expr_get.table_index;
	source.types = expr_get.expr_types;
	source.rows.clear();
	source.rows.reserve(expr_get.expressions.size());

	for (const auto &row : expr_get.expressions) {
		if (row.size() != source.types.size()) {
			reason = "VALUES row width does not match VALUES column count";
			return false;
		}
		vector<string> encoded_row;
		encoded_row.reserve(row.size());
		for (const auto &expr : row) {
			string sql;
			if (!EncodeStandaloneExpression(*expr, sql, reason)) {
				return false;
			}
			encoded_row.push_back(std::move(sql));
		}
		source.rows.push_back(std::move(encoded_row));
	}

	if (source.rows.empty()) {
		reason = "empty VALUES source is not supported";
		return false;
	}

	return true;
}

static string ValuesSourceSQL(const ValuesSource &source, const string &alias) {
	string sql = "(VALUES ";
	for (idx_t row_idx = 0; row_idx < source.rows.size(); row_idx++) {
		if (row_idx > 0) {
			sql += ", ";
		}
		sql += "(";
		for (idx_t col_idx = 0; col_idx < source.rows[row_idx].size(); col_idx++) {
			if (col_idx > 0) {
				sql += ", ";
			}
			sql += source.rows[row_idx][col_idx];
		}
		sql += ")";
	}
	sql += ") AS " + EscapeIdentifier(alias) + "(";
	for (idx_t col_idx = 0; col_idx < source.ColumnCount(); col_idx++) {
		if (col_idx > 0) {
			sql += ", ";
		}
		sql += EscapeIdentifier("c" + std::to_string(col_idx));
	}
	sql += ")";
	return sql;
}

static bool EncodeTargetProjectedColumn(const DirectDMLContext &ctx, idx_t projected_idx, const string &alias,
										string &sql, string &reason) {
	column_t table_col_idx;
	if (ctx.column_ids.empty()) {
		table_col_idx = projected_idx;
	} else if (projected_idx >= ctx.column_ids.size()) {
		reason = "target column reference is out of range";
		return false;
	} else {
		table_col_idx = ctx.column_ids[projected_idx];
	}
	if (table_col_idx == COLUMN_IDENTIFIER_ROW_ID || table_col_idx >= ctx.column_names.size()) {
		reason = "target column reference is not a regular table column";
		return false;
	}
	sql = EscapeIdentifier(alias) + "." + EscapeIdentifier(ctx.column_names[table_col_idx]);
	return true;
}

static bool EncodeSourceColumn(const ValuesSource &source, idx_t projected_idx, const string &alias, string &sql,
							   string &reason) {
	if (projected_idx >= source.ColumnCount()) {
		reason = "source column reference is out of range";
		return false;
	}
	sql = EscapeIdentifier(alias) + "." + EscapeIdentifier("c" + std::to_string(projected_idx));
	return true;
}

static bool EncodeChildColumn(const JoinChildContext &child, idx_t projected_idx, string &sql, string &reason) {
	if (child.kind == JoinChildKind::TARGET) {
		return EncodeTargetProjectedColumn(*child.target_ctx, projected_idx, child.alias, sql, reason);
	}
	return EncodeSourceColumn(*child.source, projected_idx, child.alias, sql, reason);
}

static bool EncodeChildExpression(const Expression &expr, const JoinChildContext &child, string &sql, string &reason) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expr.Cast<BoundCastExpression>();
		if (cast.try_cast) {
			reason = "TRY_CAST is not supported in UPDATE FROM pushdown";
			return false;
		}
		return EncodeChildExpression(*cast.child, child, sql, reason);
	}
	case ExpressionClass::BOUND_REF:
		return EncodeChildColumn(child, expr.Cast<BoundReferenceExpression>().index, sql, reason);
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		if (col_ref.binding.table_index != child.output_table_index) {
			reason = "column reference belongs to a different join child";
			return false;
		}
		return EncodeChildColumn(child, col_ref.binding.column_index, sql, reason);
	}
	case ExpressionClass::BOUND_CONSTANT:
		return EncodeStandaloneExpression(expr, sql, reason);
	default:
		reason = "unsupported join expression: " + expr.ToString();
		return false;
	}
}

static vector<idx_t> ProjectionMapOrIdentity(const vector<idx_t> &projection_map, idx_t column_count) {
	if (!projection_map.empty()) {
		return projection_map;
	}
	vector<idx_t> result;
	result.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(i);
	}
	return result;
}

static bool EncodeJoinOutputExpression(const Expression &expr, const JoinChildContext &left_child,
									   const vector<idx_t> &left_output_map, const JoinChildContext &right_child,
									   const vector<idx_t> &right_output_map, string &sql, string &reason) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expr.Cast<BoundCastExpression>();
		if (cast.try_cast) {
			reason = "TRY_CAST is not supported in UPDATE FROM pushdown";
			return false;
		}
		return EncodeJoinOutputExpression(*cast.child, left_child, left_output_map, right_child, right_output_map, sql,
										  reason);
	}
	case ExpressionClass::BOUND_REF: {
		auto index = expr.Cast<BoundReferenceExpression>().index;
		if (index < left_output_map.size()) {
			return EncodeChildColumn(left_child, left_output_map[index], sql, reason);
		}
		auto right_index = index - left_output_map.size();
		if (right_index >= right_output_map.size()) {
			reason = "join output column reference is out of range";
			return false;
		}
		return EncodeChildColumn(right_child, right_output_map[right_index], sql, reason);
	}
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		if (col_ref.binding.table_index == left_child.output_table_index) {
			return EncodeChildColumn(left_child, col_ref.binding.column_index, sql, reason);
		}
		if (col_ref.binding.table_index == right_child.output_table_index) {
			return EncodeChildColumn(right_child, col_ref.binding.column_index, sql, reason);
		}
		reason = "column reference does not belong to the UPDATE join output";
		return false;
	}
	case ExpressionClass::BOUND_CONSTANT:
		return EncodeStandaloneExpression(expr, sql, reason);
	default:
		reason = "unsupported UPDATE FROM SET expression: " + expr.ToString();
		return false;
	}
}

static bool TryExtractTargetGet(LogicalOperator &op, LogicalGet *&get, vector<const Expression *> &filters) {
	string ignored_reason;
	if (!ExtractFilteredGet(op, get, filters, ignored_reason)) {
		return false;
	}
	return get && get->function.name == "mssql_catalog_scan";
}

static bool TryBuildUpdateFromJoin(LogicalUpdate &op, MSSQLTableEntry &table_entry, MSSQLDirectDMLTarget &target,
								   string &reason) {
	if (op.return_chunk) {
		reason = "RETURNING is not supported by direct UPDATE pushdown";
		return false;
	}
	if (op.children.size() != 1 || op.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		reason = "UPDATE child is not a projection";
		return false;
	}

	auto &projection = op.children[0]->Cast<LogicalProjection>();
	if (projection.children.size() != 1 ||
		projection.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		reason = "unsupported logical operator in DML source: " +
				 (projection.children.empty() ? string("missing child") : projection.children[0]->GetName());
		return false;
	}

	auto &join = projection.children[0]->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER || join.children.size() != 2 || join.conditions.empty()) {
		reason = "UPDATE FROM pushdown only supports INNER comparison joins with conditions";
		return false;
	}

	LogicalGet *left_get = nullptr;
	LogicalGet *right_get = nullptr;
	vector<const Expression *> left_filters;
	vector<const Expression *> right_filters;
	bool left_is_target = TryExtractTargetGet(*join.children[0], left_get, left_filters);
	bool right_is_target = TryExtractTargetGet(*join.children[1], right_get, right_filters);
	if (left_is_target == right_is_target) {
		reason = "UPDATE FROM pushdown requires exactly one MSSQL target scan";
		return false;
	}

	auto *target_get = left_is_target ? left_get : right_get;
	auto *source_op = left_is_target ? join.children[1].get() : join.children[0].get();
	auto &target_filters = left_is_target ? left_filters : right_filters;

	ValuesSource source;
	if (!ExtractValuesSource(*source_op, source, reason)) {
		return false;
	}

	auto target_ctx = BuildContext(table_entry, *target_get);
	JoinChildContext target_child;
	target_child.kind = JoinChildKind::TARGET;
	target_child.alias = "tgt";
	target_child.output_table_index = target_get->table_index;
	target_child.target_get = target_get;
	target_child.target_ctx = &target_ctx;

	JoinChildContext source_child;
	source_child.kind = JoinChildKind::VALUES_SOURCE;
	source_child.alias = "src";
	source_child.output_table_index = source.output_table_index;
	source_child.source = &source;

	auto &left_child = left_is_target ? target_child : source_child;
	auto &right_child = left_is_target ? source_child : target_child;
	auto left_output_map = ProjectionMapOrIdentity(join.left_projection_map, left_child.ColumnCount());
	auto right_output_map = ProjectionMapOrIdentity(join.right_projection_map, right_child.ColumnCount());

	if (projection.expressions.size() < op.columns.size()) {
		reason = "UPDATE projection does not contain all SET expressions";
		return false;
	}

	auto &columns = table_entry.GetMSSQLColumns();
	vector<string> assignments;
	assignments.reserve(op.columns.size());
	for (idx_t i = 0; i < op.columns.size(); i++) {
		auto physical_idx = op.columns[i].index;
		if (physical_idx >= columns.size()) {
			reason = "UPDATE target column index is out of range";
			return false;
		}

		string encoded;
		if (!EncodeJoinOutputExpression(*projection.expressions[i], left_child, left_output_map, right_child,
										right_output_map, encoded, reason)) {
			reason = "unsupported SET expression for column '" + columns[physical_idx].name +
					 "': " + projection.expressions[i]->ToString() + " (" + reason + ")";
			return false;
		}
		assignments.push_back(EscapeIdentifier(columns[physical_idx].name) + " = " + encoded);
	}

	vector<string> join_conditions;
	join_conditions.reserve(join.conditions.size());
	for (auto &condition : join.conditions) {
		string op_sql;
		if (!FilterEncoder::GetComparisonOperator(condition.comparison, op_sql)) {
			reason = "unsupported UPDATE FROM join comparison";
			return false;
		}
		string left_sql;
		string right_sql;
		if (!EncodeChildExpression(*condition.left, left_child, left_sql, reason) ||
			!EncodeChildExpression(*condition.right, right_child, right_sql, reason)) {
			reason = "unsupported UPDATE FROM join condition: " + reason;
			return false;
		}
		join_conditions.push_back("(" + left_sql + op_sql + right_sql + ")");
	}

	string target_where;
	if (!EncodeWhereClause(*target_get, target_ctx, target_filters, target_where, reason, "tgt")) {
		return false;
	}

	string sql = "UPDATE " + EscapeIdentifier("tgt") + " SET ";
	for (idx_t i = 0; i < assignments.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		sql += assignments[i];
	}
	sql += " FROM " + FullyQualifiedTableName(table_entry) + " AS " + EscapeIdentifier("tgt");
	sql += " INNER JOIN " + ValuesSourceSQL(source, "src") + " ON ";
	for (idx_t i = 0; i < join_conditions.size(); i++) {
		if (i > 0) {
			sql += " AND ";
		}
		sql += join_conditions[i];
	}
	if (!target_where.empty()) {
		sql += " WHERE " + target_where;
	}

	DIRECT_DML_DEBUG(1, "planned UPDATE FROM SQL: %s", sql.c_str());

	target.catalog_name = table_entry.GetMSSQLCatalog().GetContextName();
	target.operation = "UPDATE";
	target.sql = std::move(sql);
	return true;
}

static bool ExtractUpdateShape(LogicalUpdate &op, LogicalProjection *&projection, LogicalGet *&get,
							   vector<const Expression *> &filters, string &reason) {
	if (op.return_chunk) {
		reason = "RETURNING is not supported by direct UPDATE pushdown";
		return false;
	}
	if (op.children.size() != 1) {
		reason = "UPDATE has multiple children";
		return false;
	}
	if (op.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		reason = "UPDATE child is not a projection";
		return false;
	}

	projection = &op.children[0]->Cast<LogicalProjection>();
	if (projection->children.size() != 1) {
		reason = "UPDATE projection has multiple children";
		return false;
	}

	return ExtractFilteredGet(*projection->children[0], get, filters, reason);
}

static bool ExtractDeleteShape(LogicalDelete &op, LogicalGet *&get, vector<const Expression *> &filters,
							   string &reason) {
	if (op.return_chunk) {
		reason = "RETURNING is not supported by direct DELETE pushdown";
		return false;
	}
	if (op.children.size() != 1) {
		reason = "DELETE has multiple children";
		return false;
	}

	return ExtractFilteredGet(*op.children[0], get, filters, reason);
}

}  // namespace

bool MSSQLDirectDMLPlanner::TryBuildUpdate(ClientContext &context, LogicalUpdate &op, MSSQLTableEntry &table_entry,
										   MSSQLDirectDMLTarget &target, string &reason) {
	LogicalProjection *projection = nullptr;
	LogicalGet *get = nullptr;
	vector<const Expression *> filters;
	if (!ExtractUpdateShape(op, projection, get, filters, reason)) {
		if (TryBuildUpdateFromJoin(op, table_entry, target, reason)) {
			return true;
		}
		DebugLogicalTree(*op.children[0]);
		return false;
	}

	if (projection->expressions.size() < op.columns.size()) {
		reason = "UPDATE projection does not contain all SET expressions";
		return false;
	}

	auto ctx = BuildContext(table_entry, *get);
	ExpressionEncodeContext expr_ctx(ctx.column_ids, ctx.column_names, ctx.column_types);
	auto &columns = table_entry.GetMSSQLColumns();

	vector<string> assignments;
	assignments.reserve(op.columns.size());
	for (idx_t i = 0; i < op.columns.size(); i++) {
		auto physical_idx = op.columns[i].index;
		if (physical_idx >= columns.size()) {
			reason = "UPDATE target column index is out of range";
			return false;
		}

		auto encoded = FilterEncoder::EncodeExpression(*projection->expressions[i], expr_ctx);
		if (!encoded.supported || encoded.sql.empty()) {
			reason = "unsupported SET expression for column '" + columns[physical_idx].name +
					 "': " + projection->expressions[i]->ToString();
			return false;
		}

		assignments.push_back(EscapeIdentifier(columns[physical_idx].name) + " = " + encoded.sql);
	}

	string where_sql;
	if (!EncodeWhereClause(*get, ctx, filters, where_sql, reason)) {
		return false;
	}

	string sql = "UPDATE " + FullyQualifiedTableName(table_entry) + " SET ";
	for (idx_t i = 0; i < assignments.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		sql += assignments[i];
	}
	if (!where_sql.empty()) {
		sql += " WHERE " + where_sql;
	}

	DIRECT_DML_DEBUG(1, "planned UPDATE SQL: %s", sql.c_str());

	target.catalog_name = table_entry.GetMSSQLCatalog().GetContextName();
	target.operation = "UPDATE";
	target.sql = std::move(sql);
	return true;
}

bool MSSQLDirectDMLPlanner::TryBuildDelete(ClientContext &context, LogicalDelete &op, MSSQLTableEntry &table_entry,
										   MSSQLDirectDMLTarget &target, string &reason) {
	LogicalGet *get = nullptr;
	vector<const Expression *> filters;
	if (!ExtractDeleteShape(op, get, filters, reason)) {
		DebugLogicalTree(*op.children[0]);
		return false;
	}

	auto ctx = BuildContext(table_entry, *get);
	string where_sql;
	if (!EncodeWhereClause(*get, ctx, filters, where_sql, reason)) {
		return false;
	}

	string sql = "DELETE FROM " + FullyQualifiedTableName(table_entry);
	if (!where_sql.empty()) {
		sql += " WHERE " + where_sql;
	}

	DIRECT_DML_DEBUG(1, "planned DELETE SQL: %s", sql.c_str());

	target.catalog_name = table_entry.GetMSSQLCatalog().GetContextName();
	target.operation = "DELETE";
	target.sql = std::move(sql);
	return true;
}

MSSQLPhysicalDirectDML::MSSQLPhysicalDirectDML(PhysicalPlan &plan, vector<LogicalType> types,
											   idx_t estimated_cardinality, MSSQLDirectDMLTarget target)
	: PhysicalOperator(plan, TYPE, std::move(types), estimated_cardinality), target_(std::move(target)) {}

unique_ptr<GlobalSourceState> MSSQLPhysicalDirectDML::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<MSSQLDirectDMLGlobalSourceState>();
}

SourceResultType MSSQLPhysicalDirectDML::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
														 OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<MSSQLDirectDMLGlobalSourceState>();
	lock_guard<mutex> lock(gstate.mutex);

	if (gstate.returned) {
		return SourceResultType::FINISHED;
	}

	if (!gstate.executed) {
		auto &catalog = Catalog::GetCatalog(context.client, target_.catalog_name);
		auto &mssql_catalog = catalog.Cast<MSSQLCatalog>();
		auto operation_lock = ConnectionProvider::AcquireTransactionOperationLock(context.client, mssql_catalog);
		auto connection = ConnectionProvider::GetConnection(context.client, mssql_catalog);

		try {
			auto result = MSSQLSimpleQuery::Execute(*connection, target_.sql);
			if (result.HasError()) {
				ConnectionProvider::ReleaseConnection(context.client, mssql_catalog, std::move(connection));
				throw IOException("MSSQL direct %s failed: SQL Server error %d: %s", target_.operation.c_str(),
								  result.error_number, result.error_message.c_str());
			}
			gstate.rows_affected = result.rows_affected <= 0 ? 0 : static_cast<idx_t>(result.rows_affected);
		} catch (...) {
			ConnectionProvider::ReleaseConnection(context.client, mssql_catalog, std::move(connection));
			throw;
		}

		ConnectionProvider::ReleaseConnection(context.client, mssql_catalog, std::move(connection));
		gstate.executed = true;
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(static_cast<int64_t>(gstate.rows_affected)));
	gstate.returned = true;
	return SourceResultType::FINISHED;
}

}  // namespace duckdb
