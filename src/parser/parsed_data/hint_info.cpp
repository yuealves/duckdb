#include "duckdb/parser/parsed_data/hint_info.hpp"
#include "duckdb/common/string_util.hpp"
namespace duckdb {
string HintInfo::ToString() const {
    return StringUtil::Format("%s(%lld)", hint_type, hint_value);
}

unique_ptr<HintInfo> HintInfo::Copy() const {
    auto result = make_uniq<HintInfo>();
    result->hint_type = hint_type;
    result->hint_value = hint_value;
    return result;
}
} // namespace duckdb
