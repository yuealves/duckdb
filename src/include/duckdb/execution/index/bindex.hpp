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

class BindexBase{
public:
    bool finish_read;
    std::thread::id create_tid;

};

class BindexPair{
public:
    int64_t key;
    int64_t rowid;

    BindexPair(int64_t k, int64_t v){
        key = k;
        rowid = v;
    }
};


class Bindex : public BindexBase{
public:

    string table_name;
    idx_t key_column_id;
    idx_t row_group_id;
    

    vector<BindexPair>  raw_data;
    vector<int64_t> raw_data_key;

    vector<int64_t> position_array;
    vector<int64_t> area_map_lower_bound;
    idx_t area_size;
    idx_t area_num;
    vector<vector<uint64_t>> filter_bit_vector;


    Bindex() = default;
    Bindex(string tableName,idx_t keyColumnId, idx_t rowGroupId);

    void Init(string tableName,idx_t keyColumnId, idx_t rowGroupId);

    void insertPair(int64_t key , int64_t rowid);

    string getInfo();

    void createPostitionArray();

    void createAreaMap(idx_t area_size);

    void createFilterBitVector();

    void buildBindex(idx_t area_size);

    idx_t scanAreaMapLessThan(int64_t predicate);

    idx_t scanPositionArrayLessThan(int64_t predicate ,idx_t area_idx);

    void copyDraft(idx_t area_idx, vector<uint64_t>& result);

    void refineDraft(idx_t left_pos, idx_t right_pos, vector<uint64_t>& result, bool refineToOne);

    void scanLessThan(int64_t predicate , vector<uint64_t>& vector_bitmap );  
};

} 