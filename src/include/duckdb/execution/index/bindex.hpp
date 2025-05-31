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
    string table_name;
    idx_t key_column_id;
    idx_t row_group_id;

};

template<typename KeyType>
class BindexPair{
public:
    KeyType key;
    int64_t rowid;

    BindexPair(KeyType k, int64_t v){
        key = k;
        rowid = v;
    }
};

template<typename KeyType>
class Bindex : public BindexBase{
public:

    vector<BindexPair<KeyType>>  raw_data;
    vector<KeyType> raw_data_key;

    vector<int64_t> position_array;
    vector<KeyType> area_map_lower_bound;
    idx_t area_size;
    idx_t area_num;
    vector<vector<uint64_t>> filter_bit_vector;


    Bindex() = default;
    Bindex(string tableName,idx_t keyColumnId, idx_t rowGroupId);

    void Init(string tableName,idx_t keyColumnId, idx_t rowGroupId);
    void insertPair(KeyType key , int64_t rowid);
    string getInfo();

    void createPostitionArray();
    void createAreaMap(idx_t area_size);
    void createFilterBitVector();
    void buildBindex(idx_t area_size);

    idx_t scanAreaMapLessThan(KeyType predicate);
    idx_t scanPositionArrayLessThan(KeyType predicate ,idx_t area_idx);
    idx_t scanAreaMapLessEqual(KeyType predicate);
    idx_t scanPositionArrayLessEqual(KeyType predicate ,idx_t area_idx);

    void copyDraft(idx_t area_idx, vector<uint64_t>& result);
    void refineDraft(idx_t left_pos, idx_t right_pos, vector<uint64_t>& result, bool refineToOne);
    void negationBitmap(vector<uint64_t>& vector_bitmap);

    void scanLessThan(KeyType predicate , vector<uint64_t>& vector_bitmap );  
    void scanLessEqual(KeyType predicate , vector<uint64_t>& vector_bitmap );  
    void scanGreaterThan(KeyType predicate, vector<uint64_t>& vector_bitmap );
    void scanGreaterEqual(KeyType predicate, vector<uint64_t>& vector_bitmap ); 
    void scanEqual(KeyType predicate, vector<uint64_t>& vector_bitmap);
    void scanNotEqual(KeyType predicate, vector<uint64_t>& vector_bitmap);
};

template<typename KeyType>
Bindex<KeyType>::Bindex(string tableName,idx_t keyColumnId, idx_t rowGroupId){
    table_name = tableName;
    key_column_id = keyColumnId;
    row_group_id = rowGroupId;
    raw_data.clear();
    raw_data_key.clear();
    finish_read = false;
}

template<typename KeyType>
void Bindex<KeyType>::Init(string tableName,idx_t keyColumnId, idx_t rowGroupId){
    table_name = tableName;
    key_column_id = keyColumnId;
    row_group_id = rowGroupId;
    raw_data.clear();
    raw_data_key.clear();
    finish_read = false;
}

template<typename KeyType>
void Bindex<KeyType>::insertPair(KeyType key , int64_t rowid){
    raw_data.push_back(BindexPair<KeyType>(key,rowid));
}

template<typename KeyType>
string Bindex<KeyType>::getInfo(){
    return "Bindex,Table:" + table_name + ",KeyColumn:" + to_string(key_column_id) 
            + ",RowGroupId:" + to_string(row_group_id) + ",Size:" + to_string(raw_data.size());
}

template<typename KeyType>
void Bindex<KeyType>::createPostitionArray(){
    for(size_t i = 0 ; i < raw_data.size() ;i++){
        raw_data_key.push_back( raw_data[i].key  );
    }

    std::sort( raw_data.begin() , raw_data.end() , 
                [](const BindexPair<KeyType>& a , const BindexPair<KeyType>& b){return a.key < b.key ;} );

    for(idx_t i = 0 ; i< raw_data.size() ;i++){
        position_array.push_back(  raw_data[i].rowid );
    }
    vector<BindexPair<KeyType>>().swap(raw_data);
}

template<typename KeyType>
void Bindex<KeyType>::createAreaMap(idx_t area_size){
    this->area_size = area_size;
    this->area_num = position_array.size() / area_size ;
    if( position_array.size() % area_size  ){
        this->area_num++;
    }

    for(size_t i = 0 ; i  < area_num ;i++ ){
        KeyType key = raw_data_key[ position_array [i * area_size] ];
        area_map_lower_bound.push_back( key );
    }

}

template<typename KeyType>
void Bindex<KeyType>::createFilterBitVector(){
    idx_t uint_count = position_array.size() / 64;
    if( position_array.size() % 64 ){
        uint_count++;
    }
    vector<uint64_t> filter_bit_tmp(uint_count,0u);
    for(idx_t i = 0 ; i < position_array.size() ;i++){
        if( i % area_size == 0 ){
            filter_bit_vector.push_back(filter_bit_tmp);
        }
        int64_t row_id = position_array[i];
        idx_t entry_id = row_id / 64;
        idx_t id_in_entry = row_id % 64;
        filter_bit_tmp[entry_id]  |=  (1UL << id_in_entry);
    }
    filter_bit_vector.push_back(filter_bit_tmp);
}

template<typename KeyType>
void Bindex<KeyType>::buildBindex(idx_t area_size){
    createPostitionArray();
    createAreaMap(area_size);
    createFilterBitVector();
}

//! return the first area whose values all greater than predicate
//! need to conside < min  and > max  before this function
template<typename KeyType>
idx_t Bindex<KeyType>::scanAreaMapLessThan(KeyType predicate){
    idx_t i = 0;
    for( ; i < area_map_lower_bound.size() ;i++){
        if( predicate <= area_map_lower_bound[i]  ){
            return i;
        }
    }
    return area_map_lower_bound.size() ;
}


//! return the first position arrray index that this area and before  all < predicate
//! area_idx is the area where the predicate in (not directly from scanAreaMap)
template<typename KeyType>
idx_t Bindex<KeyType>::scanPositionArrayLessThan(KeyType predicate ,idx_t area_idx){
    idx_t left = area_idx * area_size;
    idx_t right = (area_idx + 1) * area_size - 1;
    if( right > position_array.size() ){
        right = position_array.size() - 1;
    }

    auto it = std::lower_bound( position_array.begin() + left , position_array.begin() + right + 1 ,
                        predicate , [&](const int64_t& rowid , const KeyType& pred)
                                    { return raw_data_key[rowid] < pred;  } ) ;
    
    return ( std::distance(position_array.begin(), it)  - 1 ) ;
}

template <typename KeyType>
idx_t Bindex<KeyType>::scanAreaMapLessEqual(KeyType predicate){
    idx_t i = 0;
    for( ; i < area_map_lower_bound.size() ;i++){
        if( predicate < area_map_lower_bound[i]  ){
            return i;
        }
    }
    return area_map_lower_bound.size() ;
}

template <typename KeyType>
idx_t Bindex<KeyType>::scanPositionArrayLessEqual(KeyType predicate ,idx_t area_idx){
    idx_t left = area_idx * area_size;
    idx_t right = (area_idx + 1) * area_size - 1;
    if( right > position_array.size() ){
        right = position_array.size() - 1;
    }

    auto it = std::lower_bound( position_array.begin() + left , position_array.begin() + right + 1 ,
                        predicate , [&](const int64_t& rowid , const KeyType& pred)
                                    { return raw_data_key[rowid] <= pred;  } ) ;
    
    return ( std::distance(position_array.begin(), it)  - 1 ) ;
}


template<typename KeyType>
void Bindex<KeyType>::copyDraft(idx_t area_idx, vector<uint64_t>& result){
    result.clear();
    result.assign( filter_bit_vector[area_idx].begin() , filter_bit_vector[area_idx].end()  );
}

template<typename KeyType>
void Bindex<KeyType>::refineDraft(idx_t left_pos, idx_t right_pos, vector<uint64_t>& result, bool refineToOne){

    if( refineToOne ){
        for( idx_t pos = left_pos ; pos <= right_pos ; pos++ ){
            int64_t row_id = position_array[pos];
            idx_t entry_id = row_id / 64;
            idx_t id_in_entry = row_id % 64;
            result[entry_id]  |=  (1UL << id_in_entry);
        }
    }else{
        for( idx_t pos = left_pos ; pos <= right_pos ; pos++ ){
            int64_t row_id = position_array[pos];
            idx_t entry_id = row_id / 64;
            idx_t id_in_entry = row_id % 64;
            result[entry_id]  &=  (~(1UL << id_in_entry)) ;
        }
    }
}

template <typename KeyType>
void Bindex<KeyType>::negationBitmap(vector<uint64_t>& vector_bitmap ){
    size_t data_num = position_array.size(); 
    size_t size = data_num / 64 ;
    size_t i = 0;
    for (; i + 3 < size; i += 4) {
        __m256i vec = _mm256_loadu_si256((__m256i *)(&vector_bitmap[i]));
        vec = ~vec;
        _mm256_storeu_si256((__m256i *)(&vector_bitmap[i]), vec);
    }
    for (; i < size; ++i) {
        vector_bitmap[i] = ~vector_bitmap[i];
    }
    
    size_t left_bit = data_num - size * 64;
    for(size_t j = 0 ; j < left_bit ; j++  ){
        vector_bitmap[size] ^= ( 1ULL << j );
    }
}

template<typename KeyType>
void Bindex<KeyType>::scanLessThan(KeyType predicate ,vector<uint64_t>& vector_bitmap ){
    idx_t area_idx = scanAreaMapLessThan(predicate) - 1;
    idx_t pos_idx = scanPositionArrayLessThan(predicate,area_idx);

    idx_t left_pos = 0;
    idx_t right_pos = 0;
    bool refineToOne = true;
    if( (pos_idx - area_idx * area_size) <=  (area_size / 2)  ){
        left_pos = area_idx * area_size;
        right_pos = pos_idx;
    }else{
        area_idx++;
        left_pos = pos_idx+1;
        right_pos = area_idx * area_size - 1;
        if( right_pos > position_array.size() ){
            right_pos = position_array.size() - 1 ;
        }
        refineToOne = false;
    }
    copyDraft(area_idx,vector_bitmap);
    refineDraft(left_pos,right_pos,vector_bitmap,refineToOne);
}

template <typename KeyType>
void Bindex<KeyType>::scanLessEqual(KeyType predicate ,vector<uint64_t>& vector_bitmap ){
    idx_t area_idx = scanAreaMapLessEqual(predicate) - 1;
    idx_t pos_idx = scanPositionArrayLessEqual(predicate,area_idx);

    idx_t left_pos = 0;
    idx_t right_pos = 0;
    bool refineToOne = true;
    if( (pos_idx - area_idx * area_size) <=  (area_size / 2)  ){
        left_pos = area_idx * area_size;
        right_pos = pos_idx;
    }else{
        area_idx++;
        left_pos = pos_idx+1;
        right_pos = area_idx * area_size - 1;
        if( right_pos > position_array.size() ){
            right_pos = position_array.size() - 1 ;
        }
        refineToOne = false;
    }
    copyDraft(area_idx,vector_bitmap);
    refineDraft(left_pos,right_pos,vector_bitmap,refineToOne);
}

template <typename KeyType>
void Bindex<KeyType>::scanGreaterThan(KeyType predicate ,vector<uint64_t>& vector_bitmap ){
    scanLessEqual(predicate,vector_bitmap);
    negationBitmap(vector_bitmap);
}

template <typename KeyType>
void Bindex<KeyType>::scanGreaterEqual(KeyType predicate ,vector<uint64_t>& vector_bitmap ){
    scanLessThan(predicate,vector_bitmap);
    negationBitmap(vector_bitmap);
}

template <typename KeyType>
void Bindex<KeyType>::scanEqual(KeyType predicate ,vector<uint64_t>& vector_bitmap ){
    idx_t lower_area_idx = scanAreaMapLessThan(predicate) - 1;
    idx_t lower_pos_idx = scanPositionArrayLessThan(predicate,lower_area_idx) + 1 ;
    idx_t upper_area_idx = scanAreaMapLessEqual(predicate) - 1;
    idx_t upper_pos_idx = scanPositionArrayLessEqual(predicate,upper_area_idx);
    
    uint32_t bitmap_size = filter_bit_vector[0].size();
    vector_bitmap.resize(bitmap_size,0u);
    refineDraft(lower_pos_idx,upper_pos_idx,vector_bitmap,true);
}

template <typename KeyType>
void Bindex<KeyType>::scanNotEqual(KeyType predicate ,vector<uint64_t>& vector_bitmap ){
    idx_t lower_area_idx = scanAreaMapLessThan(predicate) - 1;
    idx_t lower_pos_idx = scanPositionArrayLessThan(predicate,lower_area_idx) + 1 ;
    idx_t upper_area_idx = scanAreaMapLessEqual(predicate) - 1;
    idx_t upper_pos_idx = scanPositionArrayLessEqual(predicate,upper_area_idx);
    
    uint32_t bitmap_size = filter_bit_vector[0].size();
    vector_bitmap.resize(bitmap_size,0xFFFFFFFFFFFFFFFF);
    refineDraft(lower_pos_idx,upper_pos_idx,vector_bitmap,false);
}

} 