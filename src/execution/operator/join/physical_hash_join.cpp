#include <iostream>
#include <thread>
#include <atomic>
#include <mutex>
#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type)
        : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type) {
    hash_table = make_unique<JoinHashTable>(conditions, right->GetTypes(), join_type);

    children.push_back(move(left));
    children.push_back(move(right));

    auto start = std::chrono::high_resolution_clock::now();
    timeBuild = start - start;
    timeProbe = start - start;
}

void
PhysicalHashJoin::BuildHashTable(std::mutex &opLock, std::mutex &hashLock, atomic<bool> &opDone, ClientContext &context,
                                 unique_ptr<PhysicalOperatorState> &right_state) {
    DataChunk temp;
    DataChunk keys;
    auto right_types = children[1]->GetTypes();
    temp.Initialize(right_types);
    keys.Initialize(hash_table->condition_types);
    while (1) {
        opLock.lock();
        if (opDone) {
            opLock.unlock();
            break;
        }
        children[1]->GetChunk(context, temp, right_state.get());
        if (temp.size() == 0) {
            opDone = true;
            opLock.unlock();
            break;
        }
        opLock.unlock();
        keys.Reset();
        ExpressionExecutor executor(temp);
        for (index_t i = 0; i < conditions.size(); i++) {
            executor.ExecuteExpression(*conditions[i].right, keys.data[i]);
        }
        hashLock.lock();
        hash_table->Build(keys, temp);
        hashLock.unlock();
    }
}

void PhysicalHashJoin::ProbeHashTable(std::mutex &opLock, index_t thread_number, ChunkCollection &result,
                                      atomic<bool> &opDone, ClientContext &context,
                                      unique_ptr<PhysicalOperatorState> &left_state) {
    DataChunk temp;
    temp.Initialize(children[0]->types);
    unique_ptr<JoinHashTable::ScanStructure> scan_structure;
    DataChunk resultChunk;
    resultChunk.Initialize(result.types);
    DataChunk keys;
    keys.Initialize(hash_table->condition_types);
    //auto scanStructure = make_unique<JoinHashTable::ScanStructure>(hash_table.get());
    while (1) {
        opLock.lock();
        if (opDone) {
            opLock.unlock();
            break;
        }
        children[0]->GetChunk(context, temp, left_state.get());
        if (temp.size() == 0) {
            opDone = true;
            opLock.unlock();
            break;
        }
        opLock.unlock();
        keys.Reset();
        ExpressionExecutor executor(temp);
        for (index_t i = 0; i < conditions.size(); i++) {
            executor.ExecuteExpression(*conditions[i].left, keys.data[i]);
        }
        scan_structure = hash_table->Probe(keys);
        do {
            resultChunk.Reset();
            scan_structure->Next(keys, temp, resultChunk);
            result.Append(resultChunk);
        } while (resultChunk.size() != 0);
    }
}

void PhysicalHashJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
    auto startOp = std::chrono::high_resolution_clock::now();
    auto concurrent = std::thread::hardware_concurrency();
    if (!concurrent) {
        concurrent = 1;
    }
    auto state = reinterpret_cast<PhysicalHashJoinOperatorState *>(state_);
    if (!state->initialized) {
        {
            auto start = std::chrono::high_resolution_clock::now();
//        // build the HT
            auto right_state = children[1]->GetOperatorState();

            std::mutex opLock;
            std::mutex hashLock;
            atomic<bool> opDone;
            opDone = false;

            vector<std::thread> threads;
            for (index_t i = 0; i < concurrent; i++) {
                threads.push_back(
                        std::thread(&PhysicalHashJoin::BuildHashTable, this, std::ref(opLock), std::ref(hashLock),
                                    std::ref(opDone), std::ref(context), std::ref(right_state)));
            }
            for (auto &t : threads) {
                t.join();
            }
            state->initialized = true;
            auto end = std::chrono::high_resolution_clock::now();
            timeBuild += end - start;
        }

        {
            auto rightTypes = children[1]->types;
            auto leftTypes = children[0]->types;
            auto left_state = children[0]->GetOperatorState();
            vector<TypeId> all;
            for (auto t : leftTypes) {
                all.push_back(t);
            }
            for (auto t : rightTypes) {
                all.push_back(t);
            }
            results.resize(concurrent);
            for (auto &r : results) {
                r.types = all;
            }
            std::mutex opLock;
            std::atomic<bool> opDone;

            vector<std::thread> threads;
            for (index_t i = 0; i < concurrent; i++) {
                threads.push_back(
                        std::thread(&PhysicalHashJoin::ProbeHashTable, this, std::ref(opLock), i, std::ref(results[i]),
                                    std::ref(opDone), std::ref(context), std::ref(left_state)));
            }
            for (auto &t : threads) {
                t.join();
            }
            state->initialized = true;
        }
    }
    if (output_index < results.size()) {
        while (results[output_index].count == 0) {
            output_index++;
            if (output_index >= results.size()) {
                goto empty;
            }
        }
        results[output_index].GetChunk(output_internal).Move(chunk);
        output_internal += STANDARD_VECTOR_SIZE;
        if (output_internal >= results[output_index].count) {
            output_index++;
            output_internal = 0;
        }
    } else {
        empty:
        DataChunk nullChunk;
        nullChunk.Move(chunk);
        state->finished = true;
    }
//    if (state->child_chunk.size() > 0 && state->scan_structure) {
//        // still have elements remaining from the previous probe (i.e. we got
//        // >1024 elements in the previous probe)
//        state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
//        if (chunk.size() > 0) {
//            //auto local = std::chrono::high_resolution_clock::now();
//            //timeProbe += local - startOp;
//            return;
//        }
//        state->scan_structure = nullptr;
//    }
//
//    // probe the HT
//    do {
//        // fetch the chunk from the left side
//        children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
//        if (state->child_chunk.size() == 0) {
//            return;
//        }
//        // remove any selection vectors
//        state->child_chunk.Flatten();
//        if (hash_table->size() == 0) {
//            // empty hash table, special case
//            if (hash_table->join_type == JoinType::ANTI) {
//                // anti join with empty hash table, NOP join
//                // return the input
//                assert(chunk.column_count == state->child_chunk.column_count);
//                for (index_t i = 0; i < chunk.column_count; i++) {
//                    chunk.data[i].Reference(state->child_chunk.data[i]);
//                }
//                return;
//            } else if (hash_table->join_type == JoinType::MARK) {
//                // MARK join with empty hash table
//                assert(hash_table->join_type == JoinType::MARK);
//                assert(chunk.column_count == state->child_chunk.column_count + 1);
//                auto &result_vector = chunk.data[state->child_chunk.column_count];
//                assert(result_vector.type == TypeId::BOOLEAN);
//                result_vector.count = state->child_chunk.size();
//                // for every data vector, we just reference the child chunk
//                for (index_t i = 0; i < state->child_chunk.column_count; i++) {
//                    chunk.data[i].Reference(state->child_chunk.data[i]);
//                }
//                // for the MARK vector:
//                // if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
//                // entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
//                // has NULL for every input entry
//                if (!hash_table->has_null) {
//                    auto bool_result = (bool *) result_vector.data;
//                    for (index_t i = 0; i < result_vector.count; i++) {
//                        bool_result[i] = false;
//                    }
//                } else {
//                    result_vector.nullmask.set();
//                }
//                return;
//            }
//        }
//        // resolve the join keys for the left chunk
//        state->join_keys.Reset();
//        ExpressionExecutor executor(state->child_chunk);
//        for (index_t i = 0; i < conditions.size(); i++) {
//            executor.ExecuteExpression(*conditions[i].left, state->join_keys.data[i]);
//        }
//        // perform the actual probe
//        state->scan_structure = hash_table->Probe(state->join_keys);
//        state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
//        auto local = std::chrono::high_resolution_clock::now();
//        timeProbe += local - startOp;
//    } while (chunk.size() == 0);
}

PhysicalHashJoin::~PhysicalHashJoin() {
    std::cerr << "Building took " << timeBuild.count() << std::endl;
    std::cerr << "Probing took " << timeProbe.count() << std::endl;
}

unique_ptr<PhysicalOperatorState> PhysicalHashJoin::GetOperatorState() {
    return make_unique<PhysicalHashJoinOperatorState>(children[0].get(), children[1].get());
}
