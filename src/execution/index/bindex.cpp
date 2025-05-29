#include "duckdb/execution/index/bindex.hpp"

namespace duckdb{

    Bindex::Bindex(string tableName,idx_t keyColumnId, idx_t rowGroupId){
        table_name = tableName;
        key_column_id = keyColumnId;
        row_group_id = rowGroupId;
        raw_data.clear();
        raw_data_key.clear();
        finish_read = false;
    }

    void Bindex::Init(string tableName,idx_t keyColumnId, idx_t rowGroupId){
        table_name = tableName;
        key_column_id = keyColumnId;
        row_group_id = rowGroupId;
        raw_data.clear();
        raw_data_key.clear();
        finish_read = false;
    }
    
    void Bindex::insertPair(int64_t key , int64_t rowid){
        raw_data.push_back(BindexPair(key,rowid));
    }

    string Bindex::getInfo(){
        return "Bindex,Table:" + table_name + ",KeyColumn:" + to_string(key_column_id) 
                + ",RowGroupId:" + to_string(row_group_id) + ",Size:" + to_string(raw_data.size());
    }

    void Bindex::createPostitionArray(){
        for(size_t i = 0 ; i < raw_data.size() ;i++){
            raw_data_key.push_back( raw_data[i].key  );
        }

        std::sort( raw_data.begin() , raw_data.end() , 
                    [](const BindexPair& a , const BindexPair& b){return a.key < b.key ;} );

        for(idx_t i = 0 ; i< raw_data.size() ;i++){
            position_array.push_back(  raw_data[i].rowid );
        }
        vector<BindexPair>().swap(raw_data);
    }

    void Bindex::createAreaMap(idx_t area_size){
        this->area_size = area_size;
        this->area_num = position_array.size() / area_size ;
        if( position_array.size() % area_size  ){
            this->area_num++;
        }

        for(size_t i = 0 ; i  < area_num ;i++ ){
            int64_t key = raw_data_key[ position_array [i * area_size] ];
            area_map_lower_bound.push_back( key );
        }

    }

    void Bindex::createFilterBitVector(){
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

    void Bindex::buildBindex(idx_t area_size){
        createPostitionArray();
        createAreaMap(area_size);
        createFilterBitVector();
    }

    //! return the first area whose values all greater than predicate
    //! need to conside < min  and > max  before this function
    idx_t Bindex::scanAreaMapLessThan(int64_t predicate){
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
    idx_t Bindex::scanPositionArrayLessThan(int64_t predicate ,idx_t area_idx){
        idx_t left = area_idx * area_size;
        idx_t right = (area_idx + 1) * area_size - 1;
        if( right > position_array.size() ){
            right = position_array.size() - 1;
        }

        auto it = std::lower_bound( position_array.begin() + left , position_array.begin() + right + 1 ,
                            predicate , [&](const int64_t& rowid , const int64_t& pred)
                                        { return raw_data_key[rowid] < pred;  } ) ;
        
        return ( std::distance(position_array.begin(), it)  - 1 ) ;
    }

    void Bindex::copyDraft(idx_t area_idx, vector<uint64_t>& result){
        result.clear();
        result.assign( filter_bit_vector[area_idx].begin() , filter_bit_vector[area_idx].end()  );

    }

    void Bindex::refineDraft(idx_t left_pos, idx_t right_pos, vector<uint64_t>& result, bool refineToOne){

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

    

    void Bindex::scanLessThan(int64_t predicate ,vector<uint64_t>& vector_bitmap ){
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


}