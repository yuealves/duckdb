//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_size.hpp"
#include <immintrin.h> 
#include <iostream>

namespace duckdb {
class VectorBuffer;
struct SelectionBitmap;

struct SelectionData {
	DUCKDB_API explicit SelectionData(idx_t count);

	unsafe_unique_array<sel_t> owned_data;
};

struct SelectionVector {
	SelectionVector() : sel_vector(nullptr) {
	}
	explicit SelectionVector(sel_t *sel) {
		Initialize(sel);
	}
	explicit SelectionVector(idx_t count) {
		Initialize(count);
	}
	SelectionVector(idx_t start, idx_t count) {
		Initialize(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < count; i++) {
			set_index(i, start + i);
		}
	}
	SelectionVector(const SelectionVector &sel_vector) {
		Initialize(sel_vector);
	}
	explicit SelectionVector(buffer_ptr<SelectionData> data) {
		Initialize(std::move(data));
	}
	SelectionVector &operator=(SelectionVector &&other) noexcept {
		sel_vector = other.sel_vector;
		other.sel_vector = nullptr;
		selection_data = std::move(other.selection_data);
		return *this;
	}

public:
	static idx_t Inverted(const SelectionVector &src, SelectionVector &dst, idx_t source_size, idx_t count) {
		idx_t src_idx = 0;
		idx_t dst_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			if (src_idx < source_size && src.get_index(src_idx) == i) {
				src_idx++;
				// This index is selected by 'src', skip it in 'dst'
				continue;
			}
			// This index does not exist in 'src', add it to the selection of 'dst'
			dst.set_index(dst_idx++, i);
		}
		return dst_idx;
	}

	void Initialize(sel_t *sel) {
		selection_data.reset();
		sel_vector = sel;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		selection_data = make_shared_ptr<SelectionData>(count);
		sel_vector = selection_data->owned_data.get();
	}
	void Initialize(buffer_ptr<SelectionData> data) {
		selection_data = std::move(data);
		sel_vector = selection_data->owned_data.get();
	}
	void Initialize(const SelectionVector &other) {
		selection_data = other.selection_data;
		sel_vector = other.sel_vector;
	}

	void Initialize(SelectionBitmap &other, idx_t max_count);

	inline void set_index(idx_t idx, idx_t loc) { // NOLINT: allow casing for legacy reasons
		sel_vector[idx] = UnsafeNumericCast<sel_t>(loc);
	}
	inline void swap(idx_t i, idx_t j) { // NOLINT: allow casing for legacy reasons
		sel_t tmp = sel_vector[i];
		sel_vector[i] = sel_vector[j];
		sel_vector[j] = tmp;
	}
	inline idx_t get_index(idx_t idx) const { // NOLINT: allow casing for legacy reasons
		return sel_vector ? sel_vector[idx] : idx;
	}
	sel_t *data() { // NOLINT: allow casing for legacy reasons
		return sel_vector;
	}
	const sel_t *data() const { // NOLINT: allow casing for legacy reasons
		return sel_vector;
	}
	buffer_ptr<SelectionData> sel_data() { // NOLINT: allow casing for legacy reasons
		return selection_data;
	}
	buffer_ptr<SelectionData> Slice(const SelectionVector &sel, idx_t count) const;

	string ToString(idx_t count = 0) const;
	void Print(idx_t count = 0) const;

	inline sel_t &operator[](idx_t index) const {
		return sel_vector[index];
	}
	inline bool IsSet() const {
		return sel_vector;
	}
	void Verify(idx_t count, idx_t vector_size) const;

private:
	sel_t *sel_vector;
	buffer_ptr<SelectionData> selection_data;
};


struct SelectionDataBitmap{
        DUCKDB_API explicit SelectionDataBitmap(idx_t size){
			idx_t count = size / 64 ;
			if( size % 64 ) count++;

            owned_data = make_unsafe_uniq_array<uint64_t>(count);
            //for (idx_t i = 0; i < count; i++) {
            //    owned_data[i] = 0u;
			//}

        };
        unsafe_unique_array<uint64_t> owned_data;
};

struct SelectionBitmap {
    SelectionBitmap() : sel_vector_bitmap(nullptr) {
    }
    
    explicit SelectionBitmap(idx_t count) {
        Initialize(count);
    }

public:
        
    void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
        selection_data_bitmap = make_shared_ptr<SelectionDataBitmap>(count);
        sel_vector_bitmap = selection_data_bitmap->owned_data.get();
    }
        
    void Initialize(const SelectionVector &other,idx_t count) {
        selection_data_bitmap = make_shared_ptr<SelectionDataBitmap>(STANDARD_VECTOR_SIZE);
        sel_vector_bitmap = selection_data_bitmap->owned_data.get();
        for(size_t sel_idx = 0 ; sel_idx < count ; sel_idx++){
            set_index( other.get_index(sel_idx) );
        }
    }

    void Initialize(const SelectionBitmap &other) {
        selection_data_bitmap = other.selection_data_bitmap;
        sel_vector_bitmap = other.sel_vector_bitmap;
    }

    void Initialize(bool init ,idx_t count) {
            selection_data_bitmap = make_shared_ptr<SelectionDataBitmap>(STANDARD_VECTOR_SIZE);
            sel_vector_bitmap = selection_data_bitmap->owned_data.get();
            if( init ){
				//use avx2 to speed up init
				idx_t count_256 = count / 256;
				idx_t left_256 = count % 256;

				__m256i ones = _mm256_set1_epi64x(-1);
                for (idx_t i = 0; i < count_256 ; i++) {
        			_mm256_storeu_si256((__m256i *)(sel_vector_bitmap + i*4), ones);
    			}
				if( left_256 > 0 ){
					for ( idx_t pos = count_256 * 256; pos < count ; pos++){
						set_index(pos);
					}
				}
            }
    }

    inline void set_index(idx_t loc) { 
		idx_t entry_idx = loc / 64;
		idx_t idx_in_entry = loc % 64;
        sel_vector_bitmap[entry_idx] |= (1UL << idx_in_entry);
    }

      
    inline bool get_index(idx_t idx) const { 
			idx_t entry_idx = idx / 64;
			idx_t idx_in_entry = idx % 64;
            if( sel_vector_bitmap[entry_idx] & (1UL << idx_in_entry) ) return true;
            else return false;
    }

	inline void print(){
		string str;
		idx_t count = 0;
		for(idx_t i  = 0 ; i< STANDARD_VECTOR_SIZE ;i++){
			if( get_index(i) ){
				count++;
				str += std::to_string(i) + " , ";
			}
		}
		std::cout << " count : " << count << std::endl;
		std::cout << "show row ids: " << std::endl;
		std::cout << str << std::endl << std::endl;
	}

	uint64_t* data(){
		return sel_vector_bitmap;
	}

private:
    uint64_t *sel_vector_bitmap;
    buffer_ptr<SelectionDataBitmap> selection_data_bitmap;
};

inline void SelectionVector::Initialize(SelectionBitmap &other, idx_t max_count){
	selection_data = make_shared_ptr<SelectionData>(max_count);
	sel_vector = selection_data->owned_data.get();

	union U {
		__m256i vec;
		unsigned long arr[4];
    } u;
	
	idx_t now_count = 0;
    idx_t num_entries = STANDARD_VECTOR_SIZE / 256;
	uint64_t* bitset = other.data();
    for (idx_t i = 0; i < num_entries; ++i) {
        u.vec = _mm256_loadu_si256((__m256i *)(bitset + i * 4));
        for (idx_t k = 0; k < 4; ++k) {
            unsigned long chunk = u.arr[k];
            while (chunk) {
                unsigned long bit_index = _tzcnt_u64(chunk);
                idx_t bit_pos = i * 256 + k * 64 + bit_index;
                
				sel_vector[now_count++] = UnsafeNumericCast<sel_t>(bit_pos);
				
                chunk &= chunk - 1; // Clear the first set bit
            }
        }
    }		
}

class OptionalSelection {
public:
	explicit OptionalSelection(SelectionVector *sel_p) {
		Initialize(sel_p);
	}
	void Initialize(SelectionVector *sel_p) {
		sel = sel_p;
		if (sel) {
			vec.Initialize(sel->data());
			sel = &vec;
		}
	}

	inline operator SelectionVector *() { // NOLINT: allow implicit conversion to SelectionVector
		return sel;
	}

	inline void Append(idx_t &count, const idx_t idx) {
		if (sel) {
			sel->set_index(count, idx);
		}
		++count;
	}

	inline void Advance(idx_t completed) {
		if (sel) {
			sel->Initialize(sel->data() + completed);
		}
	}

private:
	SelectionVector *sel;
	SelectionVector vec;
};

// Contains a selection vector, combined with a count
class ManagedSelection {
public:
	explicit inline ManagedSelection(idx_t size, bool initialize = true)
	    : initialized(initialize), size(size), internal_opt_selvec(nullptr) {
		count = 0;
		if (!initialized) {
			return;
		}
		sel_vec.Initialize(size);
		internal_opt_selvec.Initialize(&sel_vec);
	}

public:
	bool Initialized() const {
		return initialized;
	}
	void Initialize(idx_t new_size) {
		D_ASSERT(!initialized);
		this->size = new_size;
		sel_vec.Initialize(new_size);
		internal_opt_selvec.Initialize(&sel_vec);
		initialized = true;
	}

	inline idx_t operator[](idx_t index) const {
		D_ASSERT(index < size);
		return sel_vec.get_index(index);
	}
	inline bool IndexMapsToLocation(idx_t idx, idx_t location) const {
		return idx < count && sel_vec.get_index(idx) == location;
	}
	inline void Append(const idx_t idx) {
		internal_opt_selvec.Append(count, idx);
	}
	inline idx_t Count() const {
		return count;
	}
	inline idx_t Size() const {
		return size;
	}
	inline const SelectionVector &Selection() const {
		return sel_vec;
	}
	inline SelectionVector &Selection() {
		return sel_vec;
	}

private:
	bool initialized = false;
	idx_t count;
	idx_t size;
	SelectionVector sel_vec;
	OptionalSelection internal_opt_selvec;
};

} // namespace duckdb
