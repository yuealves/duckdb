#define DUCKDB_EXTENSION_MAIN

#include "csi_extension.hpp"
#include "csi_scan.hpp"

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

	names.emplace_back("csi_row_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("csi_value");
	// return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::INTEGER);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> CSIScanInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto csi_fake_table = make_shared_ptr<CSIFakeTable>(2 /*num_cols*/);
	auto result = make_uniq<CSIScanGlobalState>(csi_fake_table, input.column_ids);
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> CSIScanInitLocal(ExecutionContext &, TableFunctionInitInput &,
                                                     GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<CSIScanGlobalState>();
	auto result = make_uniq<CSIScanLocalState>();
	result->pack_index = gstate.GetNextPackIdx();
	return std::move(result);
}

static void CSIQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<CSIScanGlobalState>();
	auto &lstate = data_p.local_state->Cast<CSIScanLocalState>();
	idx_t total_rows = lstate.GetPackSize(); // 4 rows are enough for demo
	if (lstate.pack_index < 0) {
		return;
	}
	if (lstate.row_index >= total_rows) {
		lstate.pack_index = gstate.GetNextPackIdx();
		if (lstate.pack_index < 0) {
			return;
		}
		lstate.row_index = 0;
	}
	// idx_t chunk_count = 0;
	// while (lstate.row_index < total_rows && chunk_count < STANDARD_VECTOR_SIZE) {
	// 	for (idx_t col_idx = 0; col_idx < gstate.column_ids.size(); col_idx++) {
	// 		column_t col = gstate.column_ids[col_idx];
	// 		output.SetValue(col_idx, chunk_count,
	// 		                Value::INTEGER((int32_t)gstate.GetFakeCSIValue(col, lstate.pack_index, lstate.row_index)));
	// 	}
	// 	lstate.row_index++;
	// 	chunk_count++;
	// 	// output.SetValue(0, chunk_count,
	// 	//                 Value::INTEGER((int32_t)gstate.GetFakeCSIValue(0, lstate.pack_index, lstate.row_index)));
	// 	// output.SetValue(1, chunk_count,
	// 	//                 Value::INTEGER((int32_t)gstate.GetFakeCSIValue(1, lstate.pack_index, lstate.row_index)));
	// 	// output.SetValue(1, chunk_count, Value("csi dummy string"));
	// }
	idx_t chunk_count = 0;
	idx_t col_idx;

	// process the first column，update lstate.row_index and chunk_count
	col_idx = 0;
	column_t first_col = gstate.column_ids[col_idx];
	while (lstate.row_index < total_rows && chunk_count < STANDARD_VECTOR_SIZE) {
		output.SetValue(
		    col_idx, chunk_count,
		    Value::INTEGER((int32_t)gstate.GetFakeCSIValue(first_col, lstate.pack_index, lstate.row_index)));
		lstate.row_index++;
		chunk_count++;
	}

	// process remaining columns
	for (col_idx = 1; col_idx < gstate.column_ids.size(); col_idx++) {
		column_t col = gstate.column_ids[col_idx];
		for (idx_t i = 0; i < chunk_count; i++) {
			output.SetValue(col_idx, i, Value::INTEGER((int32_t)gstate.GetFakeCSIValue(col, lstate.pack_index, i)));
		}
	}
	output.SetCardinality(chunk_count);
}

// static void CSIQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
// 	auto &data = data_p.global_state->Cast<CSIData>();
// 	idx_t total_rows = 5; // 5 rows are enough for demo
// 	if (data.offset >= total_rows) {
// 		return;
// 	}
// 	idx_t chunk_count = 0;
// 	while (data.offset < total_rows && chunk_count < STANDARD_VECTOR_SIZE) {
// 		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)data.offset + 1));
// 		output.SetValue(1, chunk_count, Value("csi dummy string"));
// 		data.offset++;
// 		chunk_count++;
// 	}
// 	output.SetCardinality(chunk_count);
// }

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
	// create the CSI_SCAN function that returns the query
	TableFunction csi_scan_func("csi_scan", {LogicalType::VARCHAR}, CSIQueryFunction, CSIQueryBind, CSIScanInitGlobal, CSIScanInitLocal);
	csi_scan_func.arguments[0] = LogicalType::VARCHAR;
	csi_scan_func.projection_pushdown = true;
	ExtensionUtil::RegisterFunction(db_instance, csi_scan_func);

	// csi replacement scan
	auto &config = DBConfig::GetConfig(*db.instance);
	config.replacement_scans.emplace_back(CSIScanReplacement);
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
