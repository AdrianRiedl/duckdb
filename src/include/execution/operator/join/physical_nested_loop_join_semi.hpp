//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_nested_loop_join_semi.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/operator/join/physical_join.hpp"
#include "execution/operator/join/physical_nested_loop_join_inner.hpp"

namespace duckdb {

//! PhysicalNestedLoopJoinSemi represents a semi/anti nested loop join between
//! two tables
class PhysicalNestedLoopJoinSemi : public PhysicalJoin {
public:
	PhysicalNestedLoopJoinSemi(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                           unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type);

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState(ExpressionExecutor *parent_executor) override;
};

class PhysicalNestedLoopJoinSemiOperatorState : public PhysicalOperatorState {
public:
	PhysicalNestedLoopJoinSemiOperatorState(PhysicalOperator *left, PhysicalOperator *right,
	                                        ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor) {
		assert(left && right);
	}

	DataChunk left_join_condition;
	ChunkCollection right_chunk;
};
} // namespace duckdb