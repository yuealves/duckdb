//===----------------------------------------------------------------------===//
//                         Cynos-DuckDB
//
// csi_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <string>
#include <vector>

namespace duckdb {
const int CSI_PACK_SIZE = 4;
struct CSIFakePack {
	int *data;
	CSIFakePack(int fake_value_seed) {
		data = new int[CSI_PACK_SIZE];
		for (size_t i = 0; i < CSI_PACK_SIZE; i++) {
			data[i] = i % 20 + fake_value_seed;
		}
	}
	~CSIFakePack() {
		delete data;
	}
};

struct CSIFakeColumn {
	size_t num_packs;
	std::vector<CSIFakePack *> packs;
	CSIFakeColumn(int num_packs):num_packs(num_packs) {
		packs.resize(num_packs, nullptr);
		for (int i = 0; i < num_packs; i++) {
			packs[i] = new CSIFakePack(i * 10/*fake_value_seed*/);
		}
	}
	size_t GetNumPacks() {
		return num_packs;
	}
	~CSIFakeColumn() {
		for (size_t i = 0; i < num_packs; i++) {
			delete packs[i];
		}
		packs.clear();
		packs.shrink_to_fit();
	}
};

struct CSIFakeTable {
	size_t num_cols;
	std::vector<CSIFakeColumn *> columns;
	CSIFakeTable(size_t num_cols): num_cols(num_cols) {
		columns.resize(num_cols, nullptr);
		for (size_t i = 0; i < num_cols; i++) {
			columns[i] = new CSIFakeColumn(10/*num_packs*/);
		}
	}
	size_t GetNumPacks() {
		if (columns.empty()) {
			return 0;
		}
		return columns[0]->GetNumPacks();
	}
	~CSIFakeTable() {
		for (size_t i = 0; i < num_cols; i++) {
			delete columns[i];
		}
		columns.clear();
		columns.shrink_to_fit();
	}
};

struct CSIScanGlobalState : public GlobalTableFunctionState {
	explicit CSIScanGlobalState(shared_ptr<CSIFakeTable> fake_table, const vector<column_t> &column_ids)
	    : column_ids(column_ids), csi_fake_table(fake_table){
		current_pack_id = 0;
		total_pack_num = fake_table ? fake_table->GetNumPacks() : 0;
	}
	const vector<column_t> &column_ids;
	shared_ptr<CSIFakeTable> csi_fake_table;
	std::mutex lock;
	idx_t current_pack_id;
	idx_t total_pack_num;

	int GetFakeCSIValue(idx_t col_idx, idx_t pack_idx, idx_t row_idx) {
		return csi_fake_table->columns[col_idx]->packs[pack_idx]->data[row_idx];
	}

	int GetNextPackIdx() {
		std::lock_guard<std::mutex> guard(lock);
		if (current_pack_id >= total_pack_num) {
			return -1;
		}
		return current_pack_id++;
	}
	idx_t MaxThreads() const override {
		if (csi_fake_table) {
			return csi_fake_table->GetNumPacks();
		}
		return 1;
	}
};

struct CSIScanLocalState : public LocalTableFunctionState {
	explicit CSIScanLocalState() {
		pack_index = 0;
		row_index = 0;
	}
	idx_t GetPackSize() {
		return CSI_PACK_SIZE;
	}
	int pack_index;
	idx_t row_index;
	std::unordered_map<idx_t, TableFilter *> filters;
};
} // namespace duckdb
