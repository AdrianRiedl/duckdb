//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/join_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

#include <mutex>



namespace duckdb {

//! RadixHashTable is a linear probing HT that is used for computing joins
/*!
   The RadixHashTable concatenates incoming chunks inside a linked list of
   data ptrs. The storage looks like this internally.
   [SERIALIZED ROW][NEXT POINTER]
   [SERIALIZED ROW][NEXT POINTER]
   There is a separate hash map of pointers that point into this table.
   This is what is used to resolve the hashes.
   [POINTER]
   [POINTER]
   [POINTER]
   The pointers are either NULL
*/
class RadixHashTable {
public:

    void Hash(DataChunk &keys, Vector &hashes);

    private:
	//! Nodes store the actual data of the tuples inside the HT as a linked list
	struct Node {
		index_t count;
		index_t capacity;
		unique_ptr<data_t[]> data;
		unique_ptr<Node> prev;

		Node(index_t tuple_size, index_t capacity) : count(0), capacity(capacity) {
			data = unique_ptr<data_t[]>(new data_t[tuple_size * capacity]);
			memset(data.get(), 0, tuple_size * capacity);
		}
		~Node() {
			if (prev) {
				auto current_prev = move(prev);
				while (current_prev) {
					current_prev = move(current_prev->prev);
				}
			}
		}
	};

	uint8_t *data;
	std::vector<Value> dataStorage;

    public:
	RadixHashTable(vector<JoinCondition> &conditions, vector<TypeId> build_types, JoinType type,
	              index_t initial_capacity = 1, bool parallel = false);
	~RadixHashTable();
	//! Resize the HT to the specified size. Must be larger than the current
	//! size.
	void Resize(index_t size);
	//! Add the given data to the HT
	void Build(DataChunk &keys, DataChunk &input);
	//! Probe the HT with the given input chunk, resulting in the given result
	void Probe(DataChunk &keys, DataChunk &payload, ChunkCollection &ch);

	//! The stringheap of the RadixHashTable
	StringHeap string_heap;

	index_t size() {
		return count;
	}

	//! The types of the keys used in equality comparison
	vector<TypeId> equality_types;
	//! The types of the keys
	vector<TypeId> condition_types;
	//! The types of all conditions
	vector<TypeId> build_types;
	//! The comparison predicates
	vector<ExpressionType> predicates;
	//! Size of condition keys
	index_t equality_size;
	//! Size of condition keys
	index_t condition_size;
	//! Size of build tuple
	index_t build_size;
	//! The size of an entry as stored in the HashTable
	index_t entry_size;
	//! The total tuple size
	index_t tuple_size;
	//! The join type of the HT
	JoinType join_type;
	//! Whether or not any of the key elements contain NULL
	bool has_null;
	//! Bitmask for getting relevant bits from the hashes to determine the position
	uint64_t bitmask;

    //---------------------------------------------------------------------------
    // Bit Twiddling Hacks:
    // https://graphics.stanford.edu/~seander/bithacks.html
    //---------------------------------------------------------------------------
    inline index_t NextPow2_64(index_t v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        v++;
        return v;
    }

	struct {
		//! The types of the duplicate eliminated columns, only used in correlated MARK JOIN for flattening ANY()/ALL()
		//! expressions
		vector<TypeId> correlated_types;
		//! The aggregate expression nodes used by the HT
		vector<unique_ptr<Expression>> correlated_aggregates;
		//! The HT that holds the group counts for every correlated column
		unique_ptr<SuperLargeHashTable> correlated_counts;
		//! Group chunk used for aggregating into correlated_counts
		DataChunk group_chunk;
		//! Payload chunk used for aggregating into correlated_counts
		DataChunk payload_chunk;
		//! Result chunk used for aggregating into correlated_counts
		DataChunk result_chunk;
	} correlated_mark_join_info;

private:
	//! Apply a bitmask to the hashes
	void ApplyBitmask(Vector &hashes);
	//! Insert the given set of locations into the HT with the given set of
	//! hashes. Caller should hold lock in parallel HT.
	void InsertHashes(Vector &hashes, data_ptr_t key_locations[]);
	//! The capacity of the HT. This can be increased using
	//! RadixHashTable::Resize
	index_t capacity;
	//! The amount of entries stored in the HT currently
	index_t count;
	//! The data of the HT
	unique_ptr<Node> head;
	//! The hash map of the HT
	unique_ptr<data_ptr_t[]> hashed_pointers;
	//! Whether or not the HT has to support parallel build
	bool parallel = false;
	//! Mutex used for parallelism
	std::mutex parallel_lock;
	//! Whether or not NULL values are considered equal in each of the comparisons
	vector<bool> null_values_are_equal;

	//! Copying not allowed
	//RadixHashTable(const RadixHashTable &) = delete;
};

} // namespace duckdb
