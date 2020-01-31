//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_hash_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalHashJoin represents a hash loop join between two tables
class PhysicalHashJoin : public PhysicalComparisonJoin {
public:
	PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                 vector<JoinCondition> cond, JoinType join_type);

	unique_ptr<JoinHashTable> hash_table;

	~PhysicalHashJoin();

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

    void BuildHashTable(std::mutex &opLock, std::mutex &hashLock, atomic<bool> &opDone,
                                          ClientContext &context, unique_ptr<PhysicalOperatorState> &right_state);
    void ProbeHashTable(std::mutex &opLock, index_t thread_number, ChunkCollection &result,
                        atomic<bool> &opDone, ClientContext &context,
                        unique_ptr<PhysicalOperatorState> &left_state);

    std::chrono::duration<double> timeBuild;
    std::chrono::duration<double> timeProbe;
    std::vector<ChunkCollection> results;
    index_t output_index = 0;
    index_t output_internal = 0;
};

class PhysicalHashJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalHashJoinOperatorState(PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(left), initialized(false) {
		assert(left && right);
	}

	bool initialized;
	DataChunk join_keys;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
};
} // namespace duckdb
