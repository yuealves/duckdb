#define DUCKDB_EXTENSION_MAIN

#include "csi_extension.hpp"

namespace duckdb {

static void LoadInternal(DuckDB &db) {
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
