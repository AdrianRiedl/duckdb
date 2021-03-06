#include "duckdb/storage/uncompressed_segment.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"

using namespace duckdb;
using namespace std;

UncompressedSegment::UncompressedSegment(BufferManager &manager, TypeId type, index_t row_start)
    : manager(manager), type(type), block_id(INVALID_BLOCK), max_vector_count(0), tuple_count(0), row_start(row_start),
      versions(nullptr) {
}

UncompressedSegment::~UncompressedSegment() {
	if (block_id >= MAXIMUM_BLOCK) {
		// if the uncompressed segment had an in-memory segment, destroy it when the uncompressed segment is destroyed
		manager.DestroyBuffer(block_id);
	}
}

static void CheckForConflicts(UpdateInfo *info, Transaction &transaction, Vector &update, row_t *ids, row_t offset,
                              UpdateInfo *&node) {
	if (info->version_number == transaction.transaction_id) {
		// this UpdateInfo belongs to the current transaction, set it in the node
		node = info;
	} else if (info->version_number > transaction.start_time) {
		// potential conflict, check that tuple ids do not conflict
		// as both ids and info->tuples are sorted, this is similar to a merge join
		index_t i = 0, j = 0;
		while (true) {
			auto id = ids[i] - offset;
			if (id == info->tuples[j]) {
				throw TransactionException("Conflict on update!");
			} else if (id < info->tuples[j]) {
				// id < the current tuple in info, move to next id
				i++;
				if (i == update.count) {
					break;
				}
			} else {
				// id > the current tuple, move to next tuple in info
				j++;
				if (j == info->N) {
					break;
				}
			}
		}
	}
	if (info->next) {
		CheckForConflicts(info->next, transaction, update, ids, offset, node);
	}
}

void UncompressedSegment::Update(ColumnData &column_data, SegmentStatistics &stats, Transaction &transaction,
                                 Vector &update, row_t *ids, row_t offset) {
	// can only perform in-place updates on temporary blocks
	assert(block_id >= MAXIMUM_BLOCK);

	// obtain an exclusive lock
	auto write_lock = lock.GetExclusiveLock();

	assert(!update.sel_vector);
#ifdef DEBUG
	// verify that the ids are sorted and there are no duplicates
	for (index_t i = 1; i < update.count; i++) {
		assert(ids[i] > ids[i - 1]);
	}
#endif

	// create the versions for this segment, if there are none yet
	if (!versions) {
		this->versions = unique_ptr<UpdateInfo *[]>(new UpdateInfo *[max_vector_count]);
		for (index_t i = 0; i < max_vector_count; i++) {
			this->versions[i] = nullptr;
		}
	}

	// get the vector index based on the first id
	// we assert that all updates must be part of the same vector
	auto first_id = update.sel_vector ? ids[update.sel_vector[0]] : ids[0];
	index_t vector_index = (first_id - offset) / STANDARD_VECTOR_SIZE;
	index_t vector_offset = offset + vector_index * STANDARD_VECTOR_SIZE;

	assert(first_id >= offset);
	assert(vector_index < max_vector_count);

	// first check the version chain
	UpdateInfo *node = nullptr;
	if (versions[vector_index]) {
		// there is already a version here, check if there are any conflicts and search for the node that belongs to
		// this transaction in the version chain
		CheckForConflicts(versions[vector_index], transaction, update, ids, vector_offset, node);
	}
	Update(column_data, stats, transaction, update, ids, vector_index, vector_offset, node);
}

UpdateInfo *UncompressedSegment::CreateUpdateInfo(ColumnData &column_data, Transaction &transaction, row_t *ids,
                                                  index_t count, index_t vector_index, index_t vector_offset,
                                                  index_t type_size) {
	auto node = transaction.CreateUpdateInfo(type_size, STANDARD_VECTOR_SIZE);
	node->column_data = &column_data;
	node->segment = this;
	node->vector_index = vector_index;
	node->prev = nullptr;
	node->next = versions[vector_index];
	if (node->next) {
		node->next->prev = node;
	}
	versions[vector_index] = node;

	// set up the tuple ids
	node->N = count;
	for (index_t i = 0; i < count; i++) {
		assert((index_t)ids[i] >= vector_offset && (index_t)ids[i] < vector_offset + STANDARD_VECTOR_SIZE);
		node->tuples[i] = ids[i] - vector_offset;
	};
	return node;
}

void UncompressedSegment::Fetch(ColumnScanState &state, index_t vector_index, Vector &result) {
	auto read_lock = lock.GetSharedLock();

	InitializeScan(state);
	FetchBaseData(state, vector_index, result);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void UncompressedSegment::Scan(Transaction &transaction, ColumnScanState &state, index_t vector_index, Vector &result) {
	auto read_lock = lock.GetSharedLock();

	// first fetch the data from the base table
	FetchBaseData(state, vector_index, result);
	if (versions && versions[vector_index]) {
		// if there are any versions, check if we need to overwrite the data with the versioned data
		FetchUpdateData(state, transaction, versions[vector_index], result);
	}
}

void UncompressedSegment::IndexScan(ColumnScanState &state, index_t vector_index, Vector &result) {
	if (vector_index == 0) {
		// vector_index = 0, obtain a shared lock on the segment that we keep until the index scan is complete
		state.locks.push_back(lock.GetSharedLock());
	}
	if (versions && versions[vector_index]) {
		throw TransactionException("Cannot create index with outstanding updates");
	}
	FetchBaseData(state, vector_index, result);
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
void UncompressedSegment::CleanupUpdate(UpdateInfo *info) {
	if (info->prev) {
		// there is a prev info: remove from the chain
		auto prev = info->prev;
		prev->next = info->next;
		if (prev->next) {
			prev->next->prev = prev;
		}
	} else {
		// there is no prev info: remove from base segment
		info->segment->versions[info->vector_index] = info->next;
		if (info->next) {
			info->next->prev = nullptr;
		}
	}
}

//===--------------------------------------------------------------------===//
// ToTemporary
//===--------------------------------------------------------------------===//
void UncompressedSegment::ToTemporary() {
	auto write_lock = lock.GetExclusiveLock();

	if (block_id >= MAXIMUM_BLOCK) {
		// conversion has already been performed by a different thread
		return;
	}
	// pin the current block
	auto current = manager.Pin(block_id);

	// now allocate a new block from the buffer manager
	auto handle = manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
	// now copy the data over and switch to using the new block id
	memcpy(handle->node->buffer, current->node->buffer, Storage::BLOCK_SIZE);
	this->block_id = handle->block_id;
}
