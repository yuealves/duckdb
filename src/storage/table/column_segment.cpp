#include "duckdb/storage/table/column_segment.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

#include <cstring>
#include <iostream>
#include <algorithm>
#include <immintrin.h> 

namespace duckdb {

//===--------------------------------------------------------------------===//
// Create
//===--------------------------------------------------------------------===//

unique_ptr<ColumnSegment> ColumnSegment::CreatePersistentSegment(DatabaseInstance &db, BlockManager &block_manager,
                                                                 block_id_t block_id, idx_t offset,
                                                                 const LogicalType &type, idx_t start, idx_t count,
                                                                 CompressionType compression_type,
                                                                 BaseStatistics statistics,
                                                                 unique_ptr<ColumnSegmentState> segment_state) {

	auto &config = DBConfig::GetConfig(db);
	optional_ptr<CompressionFunction> function;
	shared_ptr<BlockHandle> block;

	if (block_id == INVALID_BLOCK) {
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, type.InternalType());
	} else {
		function = config.GetCompressionFunction(compression_type, type.InternalType());
		block = block_manager.RegisterBlock(block_id);
	}

	auto segment_size = block_manager.GetBlockSize();
	return make_uniq<ColumnSegment>(db, std::move(block), type, ColumnSegmentType::PERSISTENT, start, count, *function,
	                                std::move(statistics), block_id, offset, segment_size, std::move(segment_state));
}

unique_ptr<ColumnSegment> ColumnSegment::CreateTransientSegment(DatabaseInstance &db, const LogicalType &type,
                                                                const idx_t start, const idx_t segment_size,
                                                                const idx_t block_size) {

	// Allocate a buffer for the uncompressed segment.
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto block = buffer_manager.RegisterTransientMemory(segment_size, block_size);

	// Get the segment compression function.
	auto &config = DBConfig::GetConfig(db);
	auto function = config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType());

	return make_uniq<ColumnSegment>(db, std::move(block), type, ColumnSegmentType::TRANSIENT, start, 0U, *function,
	                                BaseStatistics::CreateEmpty(type), INVALID_BLOCK, 0U, segment_size);
}

//===--------------------------------------------------------------------===//
// Construct/Destruct
//===--------------------------------------------------------------------===//
ColumnSegment::ColumnSegment(DatabaseInstance &db, shared_ptr<BlockHandle> block_p, const LogicalType &type,
                             const ColumnSegmentType segment_type, const idx_t start, const idx_t count,
                             CompressionFunction &function_p, BaseStatistics statistics, const block_id_t block_id_p,
                             const idx_t offset, const idx_t segment_size_p,
                             const unique_ptr<ColumnSegmentState> segment_state_p)

    : SegmentBase<ColumnSegment>(start, count), db(db), type(type), type_size(GetTypeIdSize(type.InternalType())),
      segment_type(segment_type), function(function_p), stats(std::move(statistics)), block(std::move(block_p)),
      block_id(block_id_p), offset(offset), segment_size(segment_size_p) {

	if (function.get().init_segment) {
		segment_state = function.get().init_segment(*this, block_id, segment_state_p.get());
	}

	// For constant segments (CompressionType::COMPRESSION_CONSTANT) the block is a nullptr.
	D_ASSERT(!block || segment_size <= GetBlockManager().GetBlockSize());
}

ColumnSegment::ColumnSegment(ColumnSegment &other, const idx_t start)

    : SegmentBase<ColumnSegment>(start, other.count.load()), db(other.db), type(std::move(other.type)),
      type_size(other.type_size), segment_type(other.segment_type), function(other.function),
      stats(std::move(other.stats)), block(std::move(other.block)), block_id(other.block_id), offset(other.offset),
      segment_size(other.segment_size), segment_state(std::move(other.segment_state)) {

	// For constant segments (CompressionType::COMPRESSION_CONSTANT) the block is a nullptr.
	D_ASSERT(!block || segment_size <= GetBlockManager().GetBlockSize());
}

ColumnSegment::~ColumnSegment() {
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void ColumnSegment::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &) {
	if (!block || block->BlockId() >= MAXIMUM_BLOCK) {
		// not an on-disk block
		return;
	}
	if (function.get().init_prefetch) {
		function.get().init_prefetch(*this, prefetch_state);
	} else {
		prefetch_state.AddBlock(block);
	}
}

void ColumnSegment::InitializeScan(ColumnScanState &state) {
	state.scan_state = function.get().init_scan(*this);
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset,
                         ScanVectorType scan_type) {
	if (scan_type == ScanVectorType::SCAN_ENTIRE_VECTOR) {
		D_ASSERT(result_offset == 0);
		Scan(state, scan_count, result);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		ScanPartial(state, scan_count, result, result_offset);
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	}
}

void ColumnSegment::Skip(ColumnScanState &state) {
	function.get().skip(*this, state, state.row_index - state.internal_index);
	state.internal_index = state.row_index;
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result) {
	function.get().scan_vector(*this, state, scan_count, result);
}

void ColumnSegment::ScanPartial(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset) {
	function.get().scan_partial(*this, state, scan_count, result, result_offset);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ColumnSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	function.get().fetch_row(*this, state, UnsafeNumericCast<int64_t>(UnsafeNumericCast<idx_t>(row_id) - this->start),
	                         result, result_idx);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t ColumnSegment::SegmentSize() const {
	return segment_size;
}

void ColumnSegment::Resize(idx_t new_size) {
	D_ASSERT(new_size > segment_size);
	D_ASSERT(offset == 0);
	D_ASSERT(block && new_size <= GetBlockManager().GetBlockSize());

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto old_handle = buffer_manager.Pin(block);
	auto new_handle = buffer_manager.Allocate(MemoryTag::IN_MEMORY_TABLE, new_size);
	auto new_block = new_handle.GetBlockHandle();
	memcpy(new_handle.Ptr(), old_handle.Ptr(), segment_size);

	this->block_id = new_block->BlockId();
	this->block = std::move(new_block);
	this->segment_size = new_size;
}

void ColumnSegment::InitializeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().init_append) {
		throw InternalException("Attempting to init append to a segment without init_append method");
	}
	state.append_state = function.get().init_append(*this);
}

idx_t ColumnSegment::Append(ColumnAppendState &state, UnifiedVectorFormat &append_data, idx_t offset, idx_t count) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().append) {
		throw InternalException("Attempting to append to a segment without append method");
	}
	return function.get().append(*state.append_state, *this, stats, append_data, offset, count);
}

idx_t ColumnSegment::FinalizeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().finalize_append) {
		throw InternalException("Attempting to call FinalizeAppend on a segment without a finalize_append method");
	}
	auto result_count = function.get().finalize_append(*this, stats);
	state.append_state.reset();
	return result_count;
}

void ColumnSegment::RevertAppend(idx_t start_row) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (function.get().revert_append) {
		function.get().revert_append(*this, start_row);
	}
	this->count = start_row - this->start;
}

//===--------------------------------------------------------------------===//
// Convert To Persistent
//===--------------------------------------------------------------------===//
void ColumnSegment::ConvertToPersistent(optional_ptr<BlockManager> block_manager, block_id_t block_id_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	segment_type = ColumnSegmentType::PERSISTENT;

	block_id = block_id_p;
	offset = 0;

	if (block_id == INVALID_BLOCK) {
		// constant block: reset the block buffer
		D_ASSERT(stats.statistics.IsConstant());
		block.reset();
	} else {
		D_ASSERT(!stats.statistics.IsConstant());
		// non-constant block: write the block to disk
		// the data for the block already exists in-memory of our block
		// instead of copying the data we alter some metadata so the buffer points to an on-disk block
		block = block_manager->ConvertToPersistent(block_id, std::move(block));
	}
}

void ColumnSegment::MarkAsPersistent(shared_ptr<BlockHandle> block_p, uint32_t offset_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	segment_type = ColumnSegmentType::PERSISTENT;

	block_id = block_p->BlockId();
	offset = offset_p;
	block = std::move(block_p);
}

DataPointer ColumnSegment::GetDataPointer() {
	if (segment_type != ColumnSegmentType::PERSISTENT) {
		throw InternalException("Attempting to call ColumnSegment::GetDataPointer on a transient segment");
	}
	// set up the data pointer directly using the data from the persistent segment
	DataPointer pointer(stats.statistics.Copy());
	pointer.block_pointer.block_id = GetBlockId();
	pointer.block_pointer.offset = NumericCast<uint32_t>(GetBlockOffset());
	pointer.row_start = start;
	pointer.tuple_count = count;
	pointer.compression_type = function.get().type;
	if (function.get().serialize_state) {
		pointer.segment_state = function.get().serialize_state(*this);
	}
	return pointer;
}

//===--------------------------------------------------------------------===//
// Drop Segment
//===--------------------------------------------------------------------===//
void ColumnSegment::CommitDropSegment() {
	if (segment_type != ColumnSegmentType::PERSISTENT) {
		// not persistent
		return;
	}
	if (block_id != INVALID_BLOCK) {
		GetBlockManager().MarkBlockAsModified(block_id);
	}
	if (function.get().cleanup_state) {
		function.get().cleanup_state(*this);
	}
}

//===--------------------------------------------------------------------===//
// Filter Selection
//===--------------------------------------------------------------------===//
template <class T, class OP, bool HAS_NULL>
static idx_t TemplatedFilterSelection(UnifiedVectorFormat &vdata, T predicate, SelectionVector &sel,
                                      idx_t approved_tuple_count, SelectionVector &result_sel) {
	auto &mask = vdata.validity;
	auto vec = UnifiedVectorFormat::GetData<T>(vdata);
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		auto idx = sel.get_index(i);
		auto vector_idx = vdata.sel->get_index(idx);
		bool comparison_result =
			(!HAS_NULL || mask.RowIsValid(vector_idx)) && OP::Operation(vec[vector_idx], predicate);
		result_sel.set_index(result_count, idx);
		result_count += comparison_result;
	}
	return result_count;
}

template <class T, class OP, bool HAS_NULL>
static idx_t TemplatedFilterSelectionBitmap(UnifiedVectorFormat &vdata, T predicate, SelectionBitmap &sel_bitmap,
                                      idx_t approved_tuple_count, SelectionBitmap &result_sel) {
	auto &mask = vdata.validity;
	auto vec = UnifiedVectorFormat::GetData<T>(vdata);
	idx_t result_count = 0;
	
	union U {
        __m256i vec;
        unsigned long arr[4];
    } u;

    idx_t num_entries = STANDARD_VECTOR_SIZE / 256;
	uint64_t* bitset = sel_bitmap.data();
    for (idx_t i = 0; i < num_entries; ++i) {
        u.vec = _mm256_loadu_si256((__m256i *)(bitset + i * 4));
        for (idx_t k = 0; k < 4; ++k) {
            unsigned long chunk = u.arr[k];
            while (chunk) {
                unsigned long bit_index = _tzcnt_u64(chunk);
                idx_t bit_pos = i * 256 + k * 64 + bit_index;
                
				auto vector_idx = vdata.sel->get_index(bit_pos);
				bool comparison_result =
					(!HAS_NULL || mask.RowIsValid(vector_idx)) && OP::Operation(vec[vector_idx], predicate) ;
				if( comparison_result ){
					result_sel.set_index(bit_pos);
				}
				result_count += comparison_result;

                chunk &= chunk - 1; // Clear the first set bit
            }
        }
    }
	/*
	idx_t count_now = 0;
	for(idx_t i = 0 ; i < STANDARD_VECTOR_SIZE ;i++){
		if( sel_bitmap.get_index(i) ){
			count_now++;
			auto vector_idx = vdata.sel->get_index(i);
			bool comparison_result =
		    	(!HAS_NULL || mask.RowIsValid(vector_idx)) && OP::Operation(vec[vector_idx], predicate);
			if( comparison_result ){
				result_sel.set_index(i);
			}
			result_count += comparison_result;
		}
		if( count_now >= approved_tuple_count ) break;
	}
	*/
	return result_count;
}

template <class T>
static void FilterSelectionSwitch(UnifiedVectorFormat &vdata, T predicate, SelectionVector &sel,
                                  idx_t &approved_tuple_count, ExpressionType comparison_type) {
	SelectionVector new_sel(approved_tuple_count);
	auto &mask = vdata.validity;
	// the inplace loops take the result as the last parameter
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_NOTEQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, GreaterThan, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, GreaterThan, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, false>(vdata, predicate, sel,
			                                                                          approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThanEquals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, false>(vdata, predicate, sel,
			                                                                             approved_tuple_count, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, true>(vdata, predicate, sel,
			                                                                            approved_tuple_count, new_sel);
		}
		break;
	}
	default:
		throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
	}
	sel.Initialize(new_sel);
}

template <class T>
static void FilterSelectionSwitchBindex(UnifiedVectorFormat &vdata, T predicate, SelectionVector &sel,
                                  idx_t &approved_tuple_count, ExpressionType comparison_type,
								  ConstantFilter& constant_filter, shared_ptr<BindexBase> bindex,idx_t vector_index ) {
	SelectionVector new_sel(approved_tuple_count);
	//auto &mask = vdata.validity;
	// the inplace loops take the result as the last parameter
	switch (comparison_type) {
	case ExpressionType::COMPARE_LESSTHAN: {

		approved_tuple_count = 0;
		shared_ptr<Bindex<int64_t>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<int64_t>>(bindex);
		if( constant_filter.rowgroup_bitmap[bindex_ptr->row_group_id].size() == 0 ){
			bindex_ptr->scanLessThan(predicate,constant_filter.rowgroup_bitmap[bindex_ptr->row_group_id]);
		}

		vector<uint64_t>& bitmap = constant_filter.rowgroup_bitmap[bindex_ptr->row_group_id] ;

		idx_t pos = STANDARD_VECTOR_SIZE / 64 * vector_index;
		//std::cout << pos << " : " << pos+4 << " : " << draft.vector_drafts.size() << std::endl ;
		if(  pos + STANDARD_VECTOR_SIZE / 64  <= bitmap.size()  ){     // warning : 32
			uint64_t* bitset = bitmap.data() + pos;

			union U {
				__m256i vec;
				unsigned long arr[4];
			} u;

			idx_t num_entries = STANDARD_VECTOR_SIZE / 256;
			for (idx_t i = 0; i < num_entries; ++i) {
				u.vec = _mm256_loadu_si256((__m256i *)(bitset + i * 4));
				for (idx_t k = 0; k < 4; ++k) {
					unsigned long chunk = u.arr[k];
					while (chunk) {
						unsigned long bit_index = _tzcnt_u64(chunk);
						idx_t bit_pos = i * 256 + k * 64 + bit_index;
						
						new_sel.set_index(approved_tuple_count,bit_pos);
						//std::cout <<"from simd bitmap: " << bit_pos << std::endl;
						approved_tuple_count++;
						
						chunk &= chunk - 1; // Clear the first set bit
					}
				}
			}
		}else{
			for(idx_t i = 0 ; i < STANDARD_VECTOR_SIZE ;i++){
				idx_t rowid = STANDARD_VECTOR_SIZE*vector_index + i;
				idx_t entry_idx = rowid / 64;
				idx_t idx_in_entry = rowid % 64;
				if( entry_idx >= bitmap.size() ) break;
				if( bitmap[entry_idx] & (1UL << idx_in_entry) ){
					//std::cout <<"from norm bitmap: " << i  << std::endl;
					new_sel.set_index(approved_tuple_count,i);
					approved_tuple_count++;
				}
			}
		}

		/*
		// only do less than now! 
		
		//std::cout <<"read sel from : " <<  bindex->getInfo() << std::endl;
		//std::cout << "now vector index: " << vector_index << std::endl;
		if( constant_filter.rowgroup_drafts.count(bindex->row_group_id) == 0 ){
			constant_filter.rowgroup_drafts[bindex->row_group_id] = RowGroupRraft();
			bindex->scanLessThan(predicate,constant_filter.rowgroup_drafts[bindex->row_group_id].vector_sels,
											constant_filter.rowgroup_drafts[bindex->row_group_id].vector_drafts);
		}
		if(sel.data() != nullptr){
			std::cout << "[warning] error occor!" << std::endl;
		}
		sel.Initialize(STANDARD_VECTOR_SIZE);
		RowGroupRraft& draft = constant_filter.rowgroup_drafts[bindex->row_group_id] ;
		approved_tuple_count = draft.vector_sels[vector_index].size();
		for(idx_t i = 0 ; i < approved_tuple_count ; i++){
			//idx_t id_in_vector = draft.vector_sels[vector_index][i] ;
			//std::cout <<"from pos: " << id_in_vector << ": " 
			//<< bindex->raw_data[ vector_index*STANDARD_VECTOR_SIZE +id_in_vector ].key << std::endl;
			sel.set_index(i,draft.vector_sels[vector_index][i]);
		}
		
		
		//for(idx_t i = 0 ; i < approved_tuple_count ; i++){
		//	idx_t id_in_vector = sel.get_index(i) ;
		//	std::cout <<"after filter: " << id_in_vector << ": " 
		//	<< bindex->raw_data[ vector_index*STANDARD_VECTOR_SIZE +id_in_vector ].key << std::endl;
		//}

		idx_t pos = STANDARD_VECTOR_SIZE * vector_index / 64;
		//std::cout << pos << " : " << pos+4 << " : " << draft.vector_drafts.size() << std::endl ;
		if(  pos + STANDARD_VECTOR_SIZE/32  <= draft.vector_drafts.size()  ){  //pos + 4 <= draft.vector_drafts.size()
			uint64_t* bitset = draft.vector_drafts.data() + pos;

			union U {
				__m256i vec;
				unsigned long arr[4];
			} u;

			idx_t num_entries = STANDARD_VECTOR_SIZE / 256;
			for (idx_t i = 0; i < num_entries; ++i) {
				u.vec = _mm256_loadu_si256((__m256i *)(bitset + i * 4));
				for (idx_t k = 0; k < 4; ++k) {
					unsigned long chunk = u.arr[k];
					while (chunk) {
						unsigned long bit_index = _tzcnt_u64(chunk);
						idx_t bit_pos = i * 256 + k * 64 + bit_index;
						
						new_sel.set_index(approved_tuple_count,bit_pos);
						//std::cout <<"from simd bitmap: " << bit_pos << std::endl;
						approved_tuple_count++;
						
						chunk &= chunk - 1; // Clear the first set bit
					}
				}
			}
		}else{
			for(idx_t i = 0 ; i < STANDARD_VECTOR_SIZE ;i++){
				idx_t rowid = STANDARD_VECTOR_SIZE*vector_index + i;
				idx_t entry_idx = rowid / 64;
				idx_t idx_in_entry = rowid % 64;
				if( entry_idx >= draft.vector_drafts.size() ) break;
				if( draft.vector_drafts[entry_idx] & (1UL << idx_in_entry) ){
					//std::cout <<"from norm bitmap: " << i  << std::endl;
					new_sel.set_index(approved_tuple_count,i);
					approved_tuple_count++;
				}
			}
		}

		//std::cout << std::endl;
		*/
		break;
	}
	
	default:
		throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
	}
	
	sel.Initialize(new_sel);
}

template <class T>
static void FilterSelectionSwitchBitmap(UnifiedVectorFormat &vdata, T predicate, SelectionBitmap &sel_bitmap,
                                  idx_t &approved_tuple_count, ExpressionType comparison_type) {
	
	SelectionBitmap new_sel(STANDARD_VECTOR_SIZE);
	auto &mask = vdata.validity;
	// the inplace loops take the result as the last parameter
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelectionBitmap<T, Equals, false>(vdata, predicate, sel_bitmap, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelectionBitmap<T, Equals, true>(vdata, predicate, sel_bitmap, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_NOTEQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelectionBitmap<T, NotEquals, false>(vdata, predicate, sel_bitmap, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelectionBitmap<T, NotEquals, true>(vdata, predicate, sel_bitmap, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelectionBitmap<T, LessThan, false>(vdata, predicate, sel_bitmap, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelectionBitmap<T, LessThan, true>(vdata, predicate, sel_bitmap, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelectionBitmap<T, GreaterThan, false>(vdata, predicate, sel_bitmap, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelectionBitmap<T, GreaterThan, true>(vdata, predicate, sel_bitmap, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelectionBitmap<T, LessThanEquals, false>(vdata, predicate, sel_bitmap,
			                                                                          approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelectionBitmap<T, LessThanEquals, true>(vdata, predicate, sel_bitmap, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelectionBitmap<T, GreaterThanEquals, false>(vdata, predicate, sel_bitmap,
			                                                                             approved_tuple_count, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelectionBitmap<T, GreaterThanEquals, true>(vdata, predicate, sel_bitmap,
			                                                                            approved_tuple_count, new_sel);
		}
		break;
	}
	default:
		throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
	}
	sel_bitmap.Initialize(new_sel);
}


template <bool IS_NULL>
static idx_t TemplatedNullSelection(UnifiedVectorFormat &vdata, SelectionVector &sel, idx_t &approved_tuple_count) {
	auto &mask = vdata.validity;
	if (mask.AllValid()) {
		// no NULL values
		if (IS_NULL) {
			approved_tuple_count = 0;
			return 0;
		} else {
			return approved_tuple_count;
		}
	} else {
		idx_t result_count = 0;
		SelectionVector result_sel(approved_tuple_count);
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			auto vector_idx = vdata.sel->get_index(idx);
			if (mask.RowIsValid(vector_idx) != IS_NULL) {
				result_sel.set_index(result_count++, idx);
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = result_count;
		return result_count;
	}
}

template <bool IS_NULL>
static idx_t TemplatedNullSelectionBitmap(UnifiedVectorFormat &vdata, SelectionBitmap &sel_bitmap, idx_t &approved_tuple_count) {
	auto &mask = vdata.validity;
	if (mask.AllValid()) {
		// no NULL values
		if (IS_NULL) {
			approved_tuple_count = 0;
			return 0;
		} else {
			return approved_tuple_count;
		}
	} else {
		idx_t result_count = 0;
		SelectionBitmap result_sel(STANDARD_VECTOR_SIZE);
		idx_t now_count = 0;
		for(idx_t i = 0 ; i < STANDARD_VECTOR_SIZE ;i++ ){
			if( sel_bitmap.get_index(i) ){
				now_count++;
				auto vector_idx = vdata.sel->get_index(i);
				if (mask.RowIsValid(vector_idx) != IS_NULL) {
					result_count++;
					result_sel.set_index(i);
				}
			}
			if(now_count >= approved_tuple_count ) break;
		}
		sel_bitmap.Initialize(result_sel);
		approved_tuple_count = result_count;
		return result_count;
	}
}

idx_t ColumnSegment::FilterSelection(SelectionVector &sel, Vector &vector, UnifiedVectorFormat &vdata,
                                     const TableFilter &filter, idx_t scan_count,
									 idx_t &approved_tuple_count ) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_OR: {
		// similar to the CONJUNCTION_AND, but we need to take care of the SelectionVectors (OR all of them)
		idx_t count_total = 0;
		SelectionVector result_sel(approved_tuple_count);
		auto &conjunction_or = filter.Cast<ConjunctionOrFilter>();
		for (auto &child_filter : conjunction_or.child_filters) {
			SelectionVector temp_sel;
			temp_sel.Initialize(sel);
			idx_t temp_tuple_count = approved_tuple_count;
			idx_t temp_count = FilterSelection(temp_sel, vector, vdata, *child_filter, scan_count, temp_tuple_count);
			// tuples passed, move them into the actual result vector
			for (idx_t i = 0; i < temp_count; i++) {
				auto new_idx = temp_sel.get_index(i);
				bool is_new_idx = true;
				for (idx_t res_idx = 0; res_idx < count_total; res_idx++) {
					if (result_sel.get_index(res_idx) == new_idx) {
						is_new_idx = false;
						break;
					}
				}
				if (is_new_idx) {
					result_sel.set_index(count_total++, new_idx);
				}
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = count_total;
		return approved_tuple_count;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = filter.Cast<ConjunctionAndFilter>();
		for (auto &child_filter : conjunction_and.child_filters) {
			FilterSelection(sel, vector, vdata, *child_filter, scan_count, approved_tuple_count);
		}
		return approved_tuple_count;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		// the inplace loops take the result as the last parameter
		switch (vector.GetType().InternalType()) {
		case PhysicalType::UINT8: {
			auto predicate = UTinyIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint8_t>(vdata, predicate, sel, approved_tuple_count,constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT16: {
			auto predicate = USmallIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint16_t>(vdata, predicate, sel, approved_tuple_count,constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT32: {
			auto predicate = UIntegerValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint32_t>(vdata, predicate, sel, approved_tuple_count,constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT64: {
			auto predicate = UBigIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint64_t>(vdata, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT8: {
			auto predicate = TinyIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int8_t>(vdata, predicate, sel, approved_tuple_count, 
					constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT16: {
			auto predicate = SmallIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int16_t>(vdata, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT32: {
			auto predicate = IntegerValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int32_t>(vdata, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT64: {
			auto predicate = BigIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int64_t>(vdata, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT128: {
			auto predicate = HugeIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<hugeint_t>(vdata, predicate, sel, approved_tuple_count,
			                                 constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT128: {
			auto predicate = UhugeIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uhugeint_t>(vdata, predicate, sel, approved_tuple_count,
			                                  constant_filter.comparison_type);
			break;
		}
		case PhysicalType::FLOAT: {
			auto predicate = FloatValue::Get(constant_filter.constant);
			FilterSelectionSwitch<float>(vdata, predicate, sel, approved_tuple_count,
					 constant_filter.comparison_type);
			break;
		}
		case PhysicalType::DOUBLE: {
			auto predicate = DoubleValue::Get(constant_filter.constant);
			FilterSelectionSwitch<double>(vdata, predicate, sel, approved_tuple_count,
						 constant_filter.comparison_type);
			break;
		}
		case PhysicalType::VARCHAR: {
			auto predicate = string_t(StringValue::Get(constant_filter.constant));
			FilterSelectionSwitch<string_t>(vdata, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type);
			break;
		}
		case PhysicalType::BOOL: {
			auto predicate = BooleanValue::Get(constant_filter.constant);
			FilterSelectionSwitch<bool>(vdata, predicate, sel, approved_tuple_count, 
						constant_filter.comparison_type);
			break;
		}
		default:
			throw InvalidTypeException(vector.GetType(), "Invalid type for filter pushed down to table comparison");
		}
		return approved_tuple_count;
	}
	case TableFilterType::IS_NULL:
		return TemplatedNullSelection<true>(vdata, sel, approved_tuple_count);
	case TableFilterType::IS_NOT_NULL:
		return TemplatedNullSelection<false>(vdata, sel, approved_tuple_count);
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		// Apply the filter on the child vector
		auto &child_vec = StructVector::GetEntries(vector)[struct_filter.child_idx];
		UnifiedVectorFormat child_data;
		child_vec->ToUnifiedFormat(scan_count, child_data);
		return FilterSelection(sel, *child_vec, child_data, *struct_filter.child_filter, scan_count,
		                       approved_tuple_count);
	}
	default:
		throw InternalException("FIXME: unsupported type for filter selection");
	}
}

idx_t ColumnSegment::FilterSelectionBindex(SelectionVector &sel, Vector &vector, UnifiedVectorFormat &vdata,
                                     TableFilter &filter, idx_t scan_count,
									 idx_t &approved_tuple_count , shared_ptr<BindexBase> bindex ,idx_t vector_index) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = filter.Cast<ConjunctionAndFilter>();
		for (auto &child_filter : conjunction_and.child_filters) {
			FilterSelectionBindex(sel, vector, vdata, *child_filter, scan_count, approved_tuple_count,bindex,vector_index);
		}
		return approved_tuple_count;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		// the inplace loops take the result as the last parameter
		switch (vector.GetType().InternalType()) {
		case PhysicalType::INT64: {
			auto predicate = BigIntValue::Get(constant_filter.constant);
			FilterSelectionSwitchBindex<int64_t>(vdata, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type,constant_filter,bindex,vector_index);
			break;
		}
		default:
			throw InvalidTypeException(vector.GetType(), "Invalid type for filter pushed down to table comparison");
		}
		return approved_tuple_count;
	}
	case TableFilterType::IS_NOT_NULL:
		return approved_tuple_count;
	
	default:
		throw InternalException("FIXME: unsupported type for filter selection");
	}
}

idx_t ColumnSegment::FilterSelectionBitmap(SelectionBitmap &sel_bitmap, Vector &vector, UnifiedVectorFormat &vdata,
                                     const TableFilter &filter, idx_t scan_count,
									 idx_t &approved_tuple_count ) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_OR: {
		// similar to the CONJUNCTION_AND, but we need to take care of the SelectionVectors (OR all of them)
		idx_t count_total = 0;
		SelectionBitmap result_sel(STANDARD_VECTOR_SIZE);
		auto &conjunction_or = filter.Cast<ConjunctionOrFilter>();
		for (auto &child_filter : conjunction_or.child_filters) {
			SelectionBitmap temp_sel;
			temp_sel.Initialize(sel_bitmap);
			idx_t temp_tuple_count = approved_tuple_count;
			idx_t temp_count = FilterSelectionBitmap(temp_sel, vector, vdata, *child_filter, scan_count, temp_tuple_count);
			idx_t now_count = 0;
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				if( temp_sel.get_index(i) ){
					result_sel.set_index(i);
					now_count++;
					count_total++;
				}
				if(now_count >= temp_count) break;
			}	
		}
		sel_bitmap.Initialize(result_sel);
		approved_tuple_count = count_total;
		return approved_tuple_count;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = filter.Cast<ConjunctionAndFilter>();
		for (auto &child_filter : conjunction_and.child_filters) {
			FilterSelectionBitmap(sel_bitmap, vector, vdata, *child_filter, scan_count, approved_tuple_count);
		}
		return approved_tuple_count;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		// the inplace loops take the result as the last parameter
		switch (vector.GetType().InternalType()) {
		case PhysicalType::UINT8: {
			auto predicate = UTinyIntValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<uint8_t>(vdata, predicate, sel_bitmap, approved_tuple_count,constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT16: {
			auto predicate = USmallIntValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<uint16_t>(vdata, predicate, sel_bitmap, approved_tuple_count,constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT32: {
			auto predicate = UIntegerValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<uint32_t>(vdata, predicate, sel_bitmap, approved_tuple_count,constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT64: {
			auto predicate = UBigIntValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<uint64_t>(vdata, predicate, sel_bitmap, approved_tuple_count,
			                                constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT8: {
			auto predicate = TinyIntValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<int8_t>(vdata, predicate, sel_bitmap, approved_tuple_count, 
					constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT16: {
			auto predicate = SmallIntValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<int16_t>(vdata, predicate, sel_bitmap, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT32: {
			auto predicate = IntegerValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<int32_t>(vdata, predicate, sel_bitmap, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT64: {
			auto predicate = BigIntValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<int64_t>(vdata, predicate, sel_bitmap, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT128: {
			auto predicate = HugeIntValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<hugeint_t>(vdata, predicate, sel_bitmap, approved_tuple_count,
			                                 constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT128: {
			auto predicate = UhugeIntValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<uhugeint_t>(vdata, predicate, sel_bitmap, approved_tuple_count,
			                                  constant_filter.comparison_type);
			break;
		}
		case PhysicalType::FLOAT: {
			auto predicate = FloatValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<float>(vdata, predicate, sel_bitmap, approved_tuple_count,
					 constant_filter.comparison_type);
			break;
		}
		case PhysicalType::DOUBLE: {
			auto predicate = DoubleValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<double>(vdata, predicate, sel_bitmap, approved_tuple_count,
						 constant_filter.comparison_type);
			break;
		}
		case PhysicalType::VARCHAR: {
			auto predicate = string_t(StringValue::Get(constant_filter.constant));
			FilterSelectionSwitchBitmap<string_t>(vdata, predicate, sel_bitmap, approved_tuple_count,
			                                constant_filter.comparison_type);
			break;
		}
		case PhysicalType::BOOL: {
			auto predicate = BooleanValue::Get(constant_filter.constant);
			FilterSelectionSwitchBitmap<bool>(vdata, predicate, sel_bitmap, approved_tuple_count, 
						constant_filter.comparison_type);
			break;
		}
		default:
			throw InvalidTypeException(vector.GetType(), "Invalid type for filter pushed down to table comparison");
		}
		return approved_tuple_count;
	}
	case TableFilterType::IS_NULL:
		return TemplatedNullSelectionBitmap<true>(vdata, sel_bitmap, approved_tuple_count);
	case TableFilterType::IS_NOT_NULL:
		return TemplatedNullSelectionBitmap<false>(vdata, sel_bitmap, approved_tuple_count);
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		// Apply the filter on the child vector
		auto &child_vec = StructVector::GetEntries(vector)[struct_filter.child_idx];
		UnifiedVectorFormat child_data;
		child_vec->ToUnifiedFormat(scan_count, child_data);
		return FilterSelectionBitmap(sel_bitmap, *child_vec, child_data, *struct_filter.child_filter, scan_count,
		                       approved_tuple_count);
	}
	default:
		throw InternalException("FIXME: unsupported type for filter selection");
	}
}

} // namespace duckdb
