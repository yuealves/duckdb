#define DUCKDB_EXTENSION_MAIN

#include "csi_extension.hpp"

#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

struct CSIData : public GlobalTableFunctionState {
	CSIData() : offset(0) {
	}
	idx_t offset;
};

static duckdb::unique_ptr<FunctionData> CSIQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {

	names.emplace_back("csi row id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("csi value");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static void CSIQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<CSIData>();
	idx_t total_rows = 5; // 5 rows are enough for demo
	if (data.offset >= total_rows) {
		return;
	}
	idx_t chunk_count = 0;
	while (data.offset < total_rows && chunk_count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)data.offset + 1));
		output.SetValue(1, chunk_count, Value("csi dummy string"));
		data.offset++;
		chunk_count++;
	}
	output.SetCardinality(chunk_count);
}

//===--------------------------------------------------------------------===//
// Scan Replacement
//===--------------------------------------------------------------------===//
unique_ptr<TableRef> CSIScanReplacement(ClientContext &context, ReplacementScanInput &input,
                                        optional_ptr<ReplacementScanData> data) {
	auto table_name = ReplacementScan::GetFullPath(input);
	if (!ReplacementScan::CanReplace(table_name, {"csi"})) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("csi_scan", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		auto &fs = FileSystem::GetFileSystem(context);
		table_function->alias = fs.ExtractBaseName(table_name);
	}

	return std::move(table_function);
}

unique_ptr<GlobalTableFunctionState> CSIInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<CSIData>();
	return std::move(result);
}

static void LoadInternal(DuckDB &db) {
	auto &db_instance = *db.instance;
	// create the CSI_QUERIES function that returns the query
	TableFunction csi_query_func("csi_queries", {}, CSIQueryFunction, CSIQueryBind, CSIInit);
	ExtensionUtil::RegisterFunction(db_instance, csi_query_func);
}

void CsiExtension::Load(DuckDB &db) {
	LoadInternal(db);
}

std::string CsiExtension::Name() {
	return "csi";
}

std::string CsiExtension::Version() const {
	return "";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void csi_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::CsiExtension>();
}

DUCKDB_EXTENSION_API const char *csi_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
