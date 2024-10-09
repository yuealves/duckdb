#include "duckdb/execution/index/bindex.hpp"

namespace duckdb{

    Bindex::Bindex(string tableName,idx_t keyColumnId, idx_t rowGroupId){
        table_name = tableName;
        key_column_id = keyColumnId;
        row_group_id = rowGroupId;
        raw_data.clear();
    }

    void Bindex::Init(string tableName,idx_t keyColumnId, idx_t rowGroupId){
        table_name = tableName;
        key_column_id = keyColumnId;
        row_group_id = rowGroupId;
        raw_data.clear();
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
        vector<BindexPair> data_tmp(raw_data.begin(),raw_data.end());

        std::sort( data_tmp.begin() , data_tmp.end() , 
                    [](const BindexPair& a , const BindexPair& b){return a.key < b.key ;} );

        for(idx_t i = 0 ; i< data_tmp.size() ;i++){
            position_array.push_back(  data_tmp[i].rowid );
        }
    }

    void Bindex::createAreaMap(idx_t area_num){
        area_size = position_array.size() / area_num ;
        if( position_array.size() % area_num  ){
            area_size++;
        }

        for(idx_t i = 0 ; i  < area_num ;i++ ){
            int64_t key = raw_data[ position_array [i * area_size] ].key;
            area_map_lower_bound.push_back( key );
        }
        
        /*
        for(idx_t i = 0 ; i< area_num ;i++  ){
            std::cout <<"lower bound: " <<  area_map_lower_bound[i]  << "  index: "  << i  << std::endl;
        }
        */

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

        //cout << filter_bit_vector.size() << "==" << area_map_lower_bound.size() <<  endl;
        //for(idx_t i = 0 ; i < filter_bit_vector.size() ;i++  ){
        //    uint64_t value = filter_bit_vector[i][0];
        //    bitset<64> bin(value);
        //    cout << bin << endl;
        //}
    }

    void Bindex::buildBindex(idx_t area_num){
        createPostitionArray();
        createAreaMap(area_num);
        createFilterBitVector();
    }

    //! return the first area whose values all greater than predicate
    //! need to conside < min  and > max  before this function
    idx_t Bindex::scanAreaMap(int64_t predicate){
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
    idx_t Bindex::scanPositionArray(int64_t predicate ,idx_t area_idx){
        idx_t left = area_idx * area_size;
        idx_t right = (area_idx + 1) * area_size - 1;
        if( right > position_array.size() ){
            right = position_array.size() - 1;
        }

        if( left == right ){
            if( raw_data[ position_array[left] ].key  < predicate ) {
                return left;
            }else{
                return left - 1;
            }
        }

        while( left < right ){
            if( left == right -1 ){
                if(  raw_data[ position_array[right] ].key  < predicate ){
                    return right;
                }else if( raw_data[ position_array[left] ].key  < predicate ) {
                    return left;
                }else{
                    return left - 1;
                } 
            }
            idx_t mid = (left + right) / 2 ;
            if( raw_data[ position_array[mid] ].key < predicate ){
                left = mid ;
            }else{
                right = mid;
            }
        }
        return left;
    }

    void Bindex::getScanBitmap(idx_t area_idx, idx_t pos_idx , vector<uint64_t>& result){
        result.clear();
        result.assign( filter_bit_vector[ area_idx ].begin() ,  filter_bit_vector[ area_idx ].end() );

        for( idx_t pos = area_idx * area_size ; pos <= pos_idx ; pos++ ){
            int64_t row_id = position_array[pos];
            idx_t entry_id = row_id / 64;
            idx_t id_in_entry = row_id % 64;
            result[entry_id]  |=  (1UL << id_in_entry);
        }
    }

    void Bindex::scanLessThan(int64_t predicate ,vector<uint64_t>& vector_bitmap ){
        //std::cout << "pred: " << predicate << std::endl;
        idx_t area_idx = scanAreaMap(predicate);
        //std::cout << "area idx: " << area_idx << std::endl;
        idx_t pos_idx = scanPositionArray(predicate,area_idx-1);
        getScanBitmap(area_idx-1,pos_idx,vector_bitmap);
        
    }


}