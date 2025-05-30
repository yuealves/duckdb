#include "duckdb/storage/table/scan_state.hpp"

#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

#include <iostream>
#include <thread>
#include <memory>

namespace duckdb {

TableScanState::TableScanState() : table_state(*this), local_state(*this) {
}

TableScanState::~TableScanState() {
}

void TableScanState::Initialize(vector<column_t> column_ids_p, optional_ptr<TableFilterSet> table_filters) {
	this->column_ids = std::move(column_ids_p);
	if (table_filters) {
		filters.Initialize(*table_filters, column_ids);
	}
}

const vector<column_t> &TableScanState::GetColumnIds() {
	D_ASSERT(!column_ids.empty());
	return column_ids;
}

ScanFilterInfo::~ScanFilterInfo() {
}

ScanFilterInfo &TableScanState::GetFilterInfo() {
	return filters;
}

ScanFilter::ScanFilter(idx_t index, const vector<column_t> &column_ids, TableFilter &filter)
    : scan_column_index(index), table_column_index(column_ids[index]), filter(filter), always_true(false) {
}

void ScanFilterInfo::Initialize(TableFilterSet &filters, const vector<column_t> &column_ids) {
	D_ASSERT(!filters.filters.empty());
	table_filters = &filters;
	adaptive_filter = make_uniq<AdaptiveFilter>(filters);
	filter_list.reserve(filters.filters.size());
	for (auto &entry : filters.filters) {
		filter_list.emplace_back(entry.first, column_ids, *entry.second);
	}
	column_has_filter.reserve(column_ids.size());
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		bool has_filter = table_filters->filters.find(col_idx) != table_filters->filters.end();
		column_has_filter.push_back(has_filter);
	}
	base_column_has_filter = column_has_filter;
}

bool ScanFilterInfo::ColumnHasFilters(idx_t column_idx) {
	if (column_idx < column_has_filter.size()) {
		return column_has_filter[column_idx];
	} else {
		return false;
	}
}

bool ScanFilterInfo::HasFilters() const {
	if (!table_filters) {
		// no filters
		return false;
	}
	// if we have filters - check if we need to check any of them
	return always_true_filters < filter_list.size();
}

void ScanFilterInfo::CheckAllFilters() {
	always_true_filters = 0;
	// reset the "column_has_filter" bitmask to the original
	for (idx_t col_idx = 0; col_idx < column_has_filter.size(); col_idx++) {
		column_has_filter[col_idx] = base_column_has_filter[col_idx];
	}
	// set "always_true" in the individual filters to false
	for (auto &filter : filter_list) {
		filter.always_true = false;
	}
}

void ScanFilterInfo::SetFilterAlwaysTrue(idx_t filter_idx) {
	auto &filter = filter_list[filter_idx];
	filter.always_true = true;
	column_has_filter[filter.scan_column_index] = false;
	always_true_filters++;
}

optional_ptr<AdaptiveFilter> ScanFilterInfo::GetAdaptiveFilter() {
	return adaptive_filter.get();
}

AdaptiveFilterState ScanFilterInfo::BeginFilter() const {
	if (!adaptive_filter) {
		return AdaptiveFilterState();
	}
	return adaptive_filter->BeginFilter();
}

void ScanFilterInfo::EndFilter(AdaptiveFilterState state) {
	if (!adaptive_filter) {
		return;
	}
	adaptive_filter->EndFilter(state);
}

void ColumnScanState::NextInternal(idx_t count) {
	if (!current) {
		//! There is no column segment
		return;
	}
	row_index += count;
	while (row_index >= current->start + current->count) {
		current = segment_tree->GetNextSegment(current);
		initialized = false;
		segment_checked = false;
		if (!current) {
			break;
		}
	}
	D_ASSERT(!current || (row_index >= current->start && row_index < current->start + current->count));
}

void ColumnScanState::Next(idx_t count) {
	NextInternal(count);
	for (auto &child_state : child_states) {
		child_state.Next(count);
	}
}

const vector<storage_t> &CollectionScanState::GetColumnIds() {
	return parent.GetColumnIds();
}

TableFilterSet &GetFilters();

ScanFilterInfo &CollectionScanState::GetFilterInfo() {
	return parent.GetFilterInfo();
}

TableScanOptions &CollectionScanState::GetOptions() {
	return parent.options;
}

ParallelCollectionScanState::ParallelCollectionScanState()
    : collection(nullptr), current_row_group(nullptr), processed_rows(0) {
}

CollectionScanState::CollectionScanState(TableScanState &parent_p)
    : row_group(nullptr), vector_index(0), max_row_group_row(0), row_groups(nullptr), max_row(0), batch_index(0),
      valid_sel(STANDARD_VECTOR_SIZE), parent(parent_p) {
}

bool CollectionScanState::Scan(DuckTransaction &transaction, DataChunk &result) {
	while (row_group) {
		row_group->Scan(transaction, *this, result);
		if (result.size() > 0) {
			return true;
		} else if (max_row <= row_group->start + row_group->count) {
			row_group = nullptr;
			return false;
		} else {
			do {
				row_group = row_groups->GetNextSegment(row_group);
				if (row_group) {
					if (row_group->start >= max_row) {
						row_group = nullptr;
						break;
					}
					bool scan_row_group = row_group->InitializeScan(*this);
					if (scan_row_group) {
						// scan this row group
						break;
					}
				}
			} while (row_group);
		}
	}
	return false;
}

bool CollectionScanState::ScanCommitted(DataChunk &result, SegmentLock &l, TableScanType type) {
	while (row_group) {
		row_group->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			row_group = row_groups->GetNextSegment(l, row_group);
			if (row_group) {
				row_group->InitializeScan(*this);
			}
		}
	}
	return false;
}

bool CollectionScanState::ScanCommitted(DataChunk &result, TableScanType type) {
	while (row_group) {
		row_group->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			row_group = row_groups->GetNextSegment(row_group);
			if (row_group) {
				row_group->InitializeScan(*this);
			}
		}
	}
	return false;
}

template<typename KeyType>
static void BindexInputData(DataChunk& result, RowGroup* row_group,string tablename,uint64_t col_idx,std::thread::id this_id ){

	//std::cout << "result size: " << result.size() << std::endl;
	if( row_group->bound_bindex == nullptr ){
		row_group->bound_bindex = make_shared_ptr<Bindex<KeyType>>();
		shared_ptr<Bindex<KeyType>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<KeyType>>(row_group->bound_bindex);
		bindex_ptr->Init(tablename,col_idx,row_group->index) ;
		row_group->bound_bindex->create_tid = this_id;
		//std::cout << "tid : " << this_id  <<  " create index for rowgroup : " << row_group->index << std::endl; 
	}
	if( row_group->bound_bindex->finish_read == true){
		//result.Reset();
		//std::cout << "[error] rowgroup : " << row_group->index  << " index has created" << std::endl; 
		return ;  
	}
	UnifiedVectorFormat data0;
	result.data[0].ToUnifiedFormat(result.size(), data0);
	auto input_data0 = UnifiedVectorFormat::GetData<KeyType>(data0);

	// row id 
	UnifiedVectorFormat data1;
	result.data[1].ToUnifiedFormat(result.size(), data1);
	auto input_data1 = UnifiedVectorFormat::GetData<int64_t>(data1);	

	shared_ptr<Bindex<KeyType>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<KeyType>>(row_group->bound_bindex);
	for(idx_t  i = 0 ; i < result.size() ; i++){
		bindex_ptr->insertPair(input_data0[i],input_data1[i] - (row_group->index*122880) );
	}
}


bool CollectionScanState::ScanCommittedBindex(DataChunk &result, TableScanType type) {
	std::thread::id this_id = std::this_thread::get_id();
	//std::cout << "enter scan commmit bindex" << std::endl;
	while (row_group) {
		row_group->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			switch (result.data[0].GetType().InternalType()){
				case PhysicalType::UINT16: {
					BindexInputData<uint16_t>(result,row_group,row_groups->getTableName(),GetColumnIds()[0],this_id);
					break;
				}
				case PhysicalType::UINT32: {
					BindexInputData<uint32_t>(result,row_group,row_groups->getTableName(),GetColumnIds()[0],this_id);
					break;
				}
				case PhysicalType::UINT64: {
					BindexInputData<uint64_t>(result,row_group,row_groups->getTableName(),GetColumnIds()[0],this_id);
					break;
				}
				case PhysicalType::INT16: {
					BindexInputData<int16_t>(result,row_group,row_groups->getTableName(),GetColumnIds()[0],this_id);
					break;
				}
				case PhysicalType::INT32: {
					BindexInputData<int32_t>(result,row_group,row_groups->getTableName(),GetColumnIds()[0],this_id);
					break;
				}
				case PhysicalType::INT64: {
					BindexInputData<int64_t>(result,row_group,row_groups->getTableName(),GetColumnIds()[0],this_id);
					break;
				}
				case PhysicalType::FLOAT: {
					BindexInputData<float>(result,row_group,row_groups->getTableName(),GetColumnIds()[0],this_id);
					break;
				}
				case PhysicalType::DOUBLE: {
					BindexInputData<double>(result,row_group,row_groups->getTableName(),GetColumnIds()[0],this_id);
					break;
				}
				default:
					std::cout << " no match type when input bindex data" << std::endl;
					return false;
			}
			return true;
		} else {
			if( row_group->bound_bindex != nullptr && row_group->bound_bindex->finish_read == false
				&& this_id == row_group->bound_bindex->create_tid ){
				row_group->bound_bindex->finish_read = true;
				switch (result.data[0].GetType().InternalType()){
					case PhysicalType::UINT16: {
						shared_ptr<Bindex<uint16_t>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<uint16_t>>(row_group->bound_bindex);
						bindex_ptr->buildBindex(2048);
						break;
					}
					case PhysicalType::UINT32: {
						shared_ptr<Bindex<uint32_t>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<uint32_t>>(row_group->bound_bindex);
						bindex_ptr->buildBindex(2048);
						break;
					}
					case PhysicalType::UINT64: {
						shared_ptr<Bindex<uint64_t>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<uint64_t>>(row_group->bound_bindex);
						bindex_ptr->buildBindex(2048);
						break;
					}
					case PhysicalType::INT16: {
						shared_ptr<Bindex<int16_t>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<int16_t>>(row_group->bound_bindex);
						bindex_ptr->buildBindex(2048);
						break;
					}
					case PhysicalType::INT32: {
						shared_ptr<Bindex<int32_t>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<int32_t>>(row_group->bound_bindex);
						bindex_ptr->buildBindex(2048);
						break;
					}
					case PhysicalType::INT64: {
						shared_ptr<Bindex<int64_t>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<int64_t>>(row_group->bound_bindex);
						bindex_ptr->buildBindex(2048);
						break;
					}
					case PhysicalType::FLOAT: {
						shared_ptr<Bindex<float>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<float>>(row_group->bound_bindex);
						bindex_ptr->buildBindex(2048);
						break;
					}
					case PhysicalType::DOUBLE: {
						shared_ptr<Bindex<double>> bindex_ptr = shared_ptr_cast<BindexBase,Bindex<double>>(row_group->bound_bindex);
						bindex_ptr->buildBindex(2048);
						break;
					}
					default:
						std::cout << " no match type when build bindex " << std::endl;
						return false;
				}
				//std::cout << row_group->bound_bindex->getInfo() << std::endl;
			}

			row_group = row_groups->GetNextSegment(row_group);
			if (row_group) {
				row_group->InitializeScan(*this);
				//std::cout << "switch to rowgroup : " << row_group->index  << std::endl;
			}
		}
	}
	//std::cout << "finish all row group  " << std::endl;
	return false;
}

PrefetchState::~PrefetchState() {
}

void PrefetchState::AddBlock(shared_ptr<BlockHandle> block) {
	blocks.push_back(std::move(block));
}

} // namespace duckdb
