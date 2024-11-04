#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {
class HintInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::HINT_INFO;
	HintInfo() : ParseInfo(TYPE) {
	}
	string hint_type;
	int64_t hint_value;

public:
	unique_ptr<HintInfo> Copy() const;
	string ToString() const;
};

} // namespace duckdb
