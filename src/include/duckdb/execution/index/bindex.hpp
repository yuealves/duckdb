//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/bindex.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/common/array.hpp"
#include <thread>

namespace duckdb{

class BindexPair{
public:
    int64_t key;
    int64_t rowid;

    BindexPair(int64_t k, int64_t v){
        key = k;
        rowid = v;
    }
};


class Bindex{
public:

    string table_name;
    idx_t key_column_id;
    idx_t row_group_id;

    vector<idx_t> position_array;
    vector<BindexPair>  raw_data;
    bool finish_read;
    std::thread::id create_tid;
    vector<int64_t> area_map_lower_bound;
    idx_t area_size;
    vector<vector<uint64_t>> filter_bit_vector;


    Bindex() = default;
    Bindex(string tableName,idx_t keyColumnId, idx_t rowGroupId);

    void Init(string tableName,idx_t keyColumnId, idx_t rowGroupId);

    void insertPair(int64_t key , int64_t rowid);

    string getInfo();

    void createPostitionArray();

    void createAreaMap(idx_t area_num);

    void createFilterBitVector();

    void buildBindex(idx_t area_num);

    idx_t scanAreaMap(int64_t predicate);

    idx_t scanPositionArray(int64_t predicate ,idx_t area_idx);

    void getScanBitmap(idx_t area_idx, idx_t pos_idx , vector<uint64_t>& result);

    void scanLessThan(int64_t predicate , vector<uint64_t>& vector_bitmap );  
};

} 