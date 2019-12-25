//
// Created by Adrian Riedl on 30.11.19.
//

#include <iostream>
#include <thread>
#include <fstream>

#include "duckdb/execution/operator/join/physical_radix_join.hpp"
//#include "duckdb/compressing/lz4.h"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/static_vector.hpp"

using namespace duckdb;

using namespace std;

PhysicalRadixJoin::PhysicalRadixJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type)
        : PhysicalComparisonJoin(op, PhysicalOperatorType::RADIX_JOIN, move(cond), join_type) {
    dummy_hash_table = make_unique<RadixHashTable>(conditions, right->GetTypes(), join_type);

    children.push_back(move(left));  // Index 0 is left child
    children.push_back(move(right));  // Index 1 is right child
    firstCall = true;
}

void PhysicalRadixJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
    //std::ofstream checkStream("/Users/adrianriedl/Desktop/check.txt",std::fstream::app);
    auto startOp = std::chrono::high_resolution_clock::now();
    auto state = reinterpret_cast<PhysicalRadixJoinOperatorState *>(state_);
    auto left_typesGlobal = children[0]->GetTypes();
    index_t leftSize = 0;
    for(auto &t : left_typesGlobal) {
        leftSize += GetTypeIdSize(t);
    }
    auto right_typesGlobal = children[1]->GetTypes();
    index_t rightSize = 0;
    for(auto &t : right_typesGlobal) {
        rightSize += GetTypeIdSize(t);
    }
    if (!state->initialized) {
        if (firstCall) {
            startOp = std::chrono::high_resolution_clock::now();
            firstCall = false;
        }
        assert(runs <= numberOfBits.size());
        // Calculate the number of partitions which are expected after this partition round.
        size_t numberOfPartitions = (1 << numberOfBits[0]);
        // Calculate the bitmask to partition the data
        size_t hash_bit_mask = numberOfPartitions - 1;

        // Start with the left side
        {
#if TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
            auto left_state = children[0]->GetOperatorState();
            auto left_types = children[0]->GetTypes();

            DataChunk left_chunk;
            DataChunk left_chunk_withHash;
            left_chunk.Initialize(left_types);
            left_types.push_back(TypeId::HASH);
            left_chunk_withHash.Initialize(left_types);

            //! Fetch all the chunks from the left side
            // Generate the histogram for the left side
            state->left_histogram = make_unique<Histogram>(1, 1 << numberOfBits[0]);
            // Initialize the left join keys
            state->left_join_keys.Initialize(dummy_hash_table->condition_types);
            while (true) {
                left_chunk_withHash.Reset();
                left_chunk.Reset();
                children[0]->GetChunk(context, left_chunk, left_state.get());
                if (left_chunk.size() == 0) {
                    break;
                }
                // Remove all the old data from the join keys
                state->left_join_keys.Reset();
                // Execute the same as the hash join does (order the conditions in the right way)
                ExpressionExecutor executor(left_chunk);
                for (index_t i = 0; i < conditions.size(); i++) {
                    executor.ExecuteExpression(*conditions[i].left, state->left_join_keys.data[i]);
                }
                // Calculate the hash
                auto hashes = make_unique<StaticVector<uint64_t>>();
                dummy_hash_table->Hash(state->left_join_keys, *hashes);
                index_t oldOffset = state->left_hash_to_Data.size();
                state->left_hash_to_Data.resize(oldOffset + left_chunk.size(), {0,std::vector<Value>(left_chunk.column_count)});
                // Now fill the left histogram
                for (index_t hashNumber = 0; hashNumber < left_chunk.size(); hashNumber++) {
                    auto hash = hashes->GetValue(hashNumber);
                    size_t pos = hash.value_.hash & hash_bit_mask;
                    state->left_histogram->IncrementBucketCounter(0, pos);
                    state->left_hash_to_Data.at(oldOffset + hashNumber).first = hash.value_.hash;
                    auto &vec = state->left_hash_to_Data.at(oldOffset + hashNumber).second;
                    for (index_t col = 0; col < left_chunk.column_count; col++) {
                        //left_chunk_withHash.GetVector(col).count++;
                        //left_chunk_withHash.GetVector(col).SetValue(hashNumber,
                        //                                            left_chunk.GetVector(col).GetValue(hashNumber));
                        vec[col] = left_chunk.GetVector(col).GetValue(hashNumber);
                    }
                    //left_chunk_withHash.GetVector(left_chunk.column_count).count++;
                    //left_chunk_withHash.GetVector(left_chunk.column_count).SetValue(hashNumber, hash);
                }
                //state->left_data->Append(left_chunk_withHash);
                //state->left_data_partitioned->Append(left_chunk_withHash);
            }

//            state->left_hash_to_pos.resize(state->left_data->count);
//            state->left_hash_to_posSwap.resize(state->left_data->count);
//            for(index_t pos = 0 ; pos < state->left_data->count; pos++) {
//                state->left_hash_to_pos.at(pos).first = state->left_data->GetValue(state->left_data->column_count()-1, pos).value_.hash;
//                state->left_hash_to_pos.at(pos).second = pos;
//            }
#if TIMER
            auto finish = std::chrono::high_resolution_clock::now();
            std::cerr << "Collecting the left side took: "
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count() << "ns!"
                      << std::endl;
#endif
            //! Start the partitioning phase with #runs runs and the bits in numberOfBits
#if TIMER
            start = std::chrono::high_resolution_clock::now();
#endif
            state->left_hash_to_DataSwap.resize(state->left_hash_to_Data.size());


            size_t shift = 0;
            for (size_t r = 0; r < runs; r++) {
                RadixJoinSingleThreadedLeft(state, shift, r);
                shift += numberOfBits[r];

                //auto temp_left_data = state->left_data;
                //state->left_data = state->left_data_partitioned;
                //state->left_data_partitioned = temp_left_data;

                //auto temp = state->left_hash_to_pos;
                //state->left_hash_to_pos = state->left_hash_to_posSwap;
                //state->left_hash_to_posSwap = temp;

                //auto &temp = state->left_hash_to_Data;
                //state->left_hash_to_Data = state->left_hash_to_DataSwap;
                //state->left_hash_to_DataSwap = temp;


                state->left_hash_to_Data.swap(state->left_hash_to_DataSwap);

            }
#if TIMER
            finish = std::chrono::high_resolution_clock::now();
            std::cerr << "Partitioning left side took: "
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count() << "ns!"
                      << std::endl;
#endif
        }

        // Continue with the right side
        {
#if TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif

            auto right_state = children[1]->GetOperatorState();
            auto right_types = children[1]->GetTypes();

            DataChunk right_chunk;
            DataChunk right_chunk_withHash;
            right_chunk.Initialize(right_types);
            right_types.push_back(TypeId::HASH);
            right_chunk_withHash.Initialize(right_types);

            //! Fetch all the chunks from the right side
            // Generate the histogram for the right side
            state->right_histogram = make_unique<Histogram>(1, 1 << numberOfBits[0]);
            // Initialize the right join keys
            state->right_join_keys.Initialize(dummy_hash_table->condition_types);
            while (true) {
                right_chunk_withHash.Reset();
                right_chunk.Reset();
                children[1]->GetChunk(context, right_chunk, right_state.get());
                if (right_chunk.size() == 0) {
                    break;
                }

                // Remove all the old data from the join keys
                state->right_join_keys.Reset();
                // Execute the same as the hash join does (order the conditions in the right way)
                ExpressionExecutor executor(right_chunk);
                for (index_t i = 0; i < conditions.size(); i++) {
                    executor.ExecuteExpression(*conditions[i].right, state->right_join_keys.data[i]);
                }
                // Calculate the hash
                auto hashes = make_unique<StaticVector<uint64_t>>();
                dummy_hash_table->Hash(state->right_join_keys, *hashes);
                index_t oldOffset = state->right_hash_to_Data.size();
                state->right_hash_to_Data.resize(oldOffset + right_chunk.size(), {0,std::vector<Value>(right_chunk.column_count)});
                // Now fill the right histogram
                for (index_t hashNumber = 0; hashNumber < right_chunk.size(); hashNumber++) {
                    auto hash = hashes->GetValue(hashNumber);
                    size_t pos = hash.value_.hash & hash_bit_mask;
                    state->right_histogram->IncrementBucketCounter(0, pos);
                    state->right_hash_to_Data.at(oldOffset + hashNumber).first = hash.value_.hash;
                    auto &vec = state->right_hash_to_Data.at(oldOffset + hashNumber).second;
                    for (index_t col = 0; col < right_chunk.column_count; col++) {
                        //right_chunk_withHash.GetVector(col).count++;
                        //right_chunk_withHash.GetVector(col).SetValue(hashNumber,
                        //                                              right_chunk.GetVector(col).GetValue(hashNumber));
                        vec[col] = right_chunk.GetVector(col).GetValue(hashNumber);

                    }
                    //right_chunk_withHash.GetVector(right_chunk.column_count).count++;
                    //right_chunk_withHash.GetVector(right_chunk.column_count).SetValue(hashNumber, hash);
                }
                //state->right_data->Append(right_chunk_withHash);
                //state->right_data_partitioned->Append(right_chunk_withHash);
            }

            //state->right_hash_to_pos.resize(state->right_data->count);
            //state->right_hash_to_posSwap.resize(state->right_data->count);
            //for(index_t pos = 0 ; pos < state->right_data->count; pos++) {
            //    state->right_hash_to_pos.at(pos).first = state->right_data->GetValue(state->right_data->column_count()-1, pos).value_.hash;
            //    state->right_hash_to_pos.at(pos).second = pos;
            //}

#if TIMER
            auto finish = std::chrono::high_resolution_clock::now();
            std::cerr << "Collecting the right side took: "
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count() << "ns!"
                      << std::endl;
#endif
            //! Start the partitioning phase with #runs runs and the bits in numberOfBits
#if TIMER
            start = std::chrono::high_resolution_clock::now();
#endif

            state->right_hash_to_DataSwap.resize(state->right_hash_to_Data.size());

            //! Start the partitioning phase with #runs runs and the bits in numberOfBits
            size_t shift = 0;
            for (size_t r = 0; r < runs; r++) {
                RadixJoinSingleThreadedRight(state, shift, r);
                shift += numberOfBits[r];

                //auto temp_right_data = state->right_data;
                //state->right_data = state->right_data_partitioned;
                //state->right_data_partitioned = temp_right_data;

                //auto temp = state->right_hash_to_pos;
                //state->right_hash_to_pos = state->right_hash_to_posSwap;
                //state->right_hash_to_posSwap = temp;

                //auto &temp = state->right_hash_to_Data;
                //state->right_hash_to_Data = state->right_hash_to_DataSwap;
                //state->right_hash_to_DataSwap = temp;

                state->right_hash_to_Data.swap(state->right_hash_to_DataSwap);

            }
#if TIMER
            finish = std::chrono::high_resolution_clock::now();
            std::cerr << "Partitioning right side took: "
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count() << "ns!"
                      << std::endl;
#endif
        }

        state->right_hashes.clear();
        state->left_hashes.clear();

        assert(state->old_left_histogram->numberOfPartitions == state->old_right_histogram->numberOfPartitions);
        assert(state->old_left_histogram->numberOfBucketsPerPartition ==
               state->old_right_histogram->numberOfBucketsPerPartition);
        size_t elements = 0;
        if (state->old_left_histogram == nullptr) {
            elements = 1;
        } else {
            elements = state->old_left_histogram->numberOfPartitions *
                       state->old_left_histogram->numberOfBucketsPerPartition;
        }
        partitions.resize(elements);
        // Get the number of hardware threads which are possible at most
        //unsigned concurentThreadsSupported = std::thread::hardware_concurrency();
        //if (concurentThreadsSupported == 0) {
        //    concurentThreadsSupported = 1;
        //}

        //auto size = state->old_left_histogram->numberOfPartitions / concurentThreadsSupported;
        //if (size == 0) { size = 1; }
        //unsigned numberOfNeededRuns = (state->old_left_histogram->numberOfPartitions + size - 1) / size;
        // Get the number of threads which are needed (if more needed just concurentThreadsSupported are triggered)
        //concurentThreadsSupported = std::min(concurentThreadsSupported, numberOfNeededRuns);
        {
            std::vector<std::thread> threads;
            std::atomic<index_t> index;
            index = 0;
#if TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
#if SINGLETHREADED
            FillPartitions(state, index);
#else
            // TODO start as much threads as parititons to fill
            for (size_t thread = 0; thread < concurentThreadsSupported; thread++) {
                threads.push_back(std::thread(&PhysicalRadixJoin::FillPartitions, this, state, std::ref(index)));
            }
#endif

            for (auto &t : threads) {
                t.join();
            }
#if TIMER
            auto finish = std::chrono::high_resolution_clock::now();
            std::cerr << "Setting the partitions took: "
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count() << "ns!"
                      << std::endl;
#endif
        }
        size_t partitionsrelevant = 0;
        vector<std::pair<std::pair<index_t, index_t>, std::pair<index_t, index_t>>> shrinked;
        vector<TypeId> expected;
        {
#if TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif

            for (auto &p : partitions) {
                if (p.first.second - p.first.first != 0 && p.second.second - p.second.first != 0) {
                    partitionsrelevant++;
                    // std::cout << p.second.first << " " << p.second.second << " size: " << (p.second.second - p.second.first) << std::endl;
                    shrinked.push_back(p);
                }
            }
            assert(shrinked.size() == partitionsrelevant);
//            auto typesRight = state->right_data->types;
//            auto typesLeft = state->left_data->types;
//            for (index_t i = 0; i < typesLeft.size() - 1; i++) {
//                expected.push_back(typesLeft.at(i));
//            }
//            for (index_t i = 0; i < typesRight.size() - 1; i++) {
//                expected.push_back(typesRight.at(i));
//            }
#if TIMER
            auto finish = std::chrono::high_resolution_clock::now();
            std::cerr << "Setting the reduced partitions took: "
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count() << "ns!"
                      << std::endl;
#endif
        }
        {
#if TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
            std::atomic<index_t> indexGlob;
            indexGlob = 0;
#if SINGLETHREADED
            // Assume we have at least one pair in shrink
            ///////////////////////////////////////////////////////////////////
//            vector<TypeId> right;
//            for (index_t i = 0; i < state->right_data->types.size() - 1; i++) {
//                right.push_back(state->right_data->types.at(i));
//            }
            DataChunk dataRight;
            dataRight.Initialize(right_typesGlobal);
            // The datachunk for the hashes of this partition
            DataChunk hashes;
            hashes.Initialize(dummy_hash_table->condition_types);
            result.types = expected;
            while (1) {
                auto startI = std::chrono::high_resolution_clock::now();
                auto i = indexGlob++;
                if (i >= shrinked.size()) {
                    break;
                }
                //auto hash_table = make_unique<JoinHashTable>(conditions, right, duckdb::JoinType::RADIX);
                auto hash_table = make_unique<RadixHashTable>(conditions, right_typesGlobal, duckdb::JoinType::INNER, 2 *
                                                                                                          dummy_hash_table->NextPow2_64(
                                                                                                                  shrinked[i].second.second -
                                                                                                                  shrinked[i].second.first));
                //auto scanStructure = make_unique<JoinHashTable::ScanStructure>(*hash_table.get());
                //auto scanStructure = make_unique<RadixHashTable::ScanStructure>(*hash_table.get());
                dataRight.Reset();
                hashes.Reset();
                for (index_t index = shrinked[i].second.first; index < shrinked[i].second.second; index++) {
                    index_t pos = dataRight.size();
                    if (pos == STANDARD_VECTOR_SIZE) {
                        // If the datachunk is full, then insert it into the hashtable
                        // Make the hashes
                        hashes.Reset();
                        ExpressionExecutor executorR(dataRight);
                        for (index_t col = 0; col < conditions.size(); col++) {
                            executorR.ExecuteExpression(*conditions[col].right, hashes.data[col]);
                        }
                        // Insert into the hashtable
                        hash_table->Build(hashes, dataRight);
                        // Reset the datachunk for next iteration
                        dataRight.Reset();
                        pos = dataRight.size();
                    }
                    //index_t posInData = state->right_hash_to_pos.at(index).second;
                    auto data = state->right_hash_to_Data.at(index).second;
                    for (index_t col = 0; col < data.size(); col++) {
                        dataRight.data[col].count += 1;
                        dataRight.data[col].SetValue(pos, data[col]);
                    }
                }
                // After the end of this partition insert into hashtable
                // Reset the hashes

                hashes.Reset();
                ExpressionExecutor executorR(dataRight);
                for (index_t j = 0; j < conditions.size(); j++) {
                    executorR.ExecuteExpression(*conditions[j].right, hashes.data[j]);
                }
                // Insert into the hashtable
                hash_table->Build(hashes, dataRight);

                auto finishI = std::chrono::high_resolution_clock::now();
                timeBuild += std::chrono::duration_cast<std::chrono::nanoseconds>(finishI - startI).count();
                //// Up to this point, the right side is in the hashtable

                // Now continue with the left side
                startI = std::chrono::high_resolution_clock::now();
//                DataChunk tempChunk;
//                tempChunk.Initialize(expected);
                DataChunk dataLeft;
//                vector<TypeId> left;
//                for (index_t i = 0; i < state->left_data->types.size() - 1; i++) {
//                    left.push_back(state->left_data->types.at(i));
//                }
                dataLeft.Initialize(left_typesGlobal);//state->left_data->types);
                hashes.Initialize(hash_table->condition_types);
                vector<TypeId> all;
                for(auto &t : left_typesGlobal) {
                    all.push_back(t);
                }
                for(auto &t : right_typesGlobal) {
                    all.push_back(t);
                }
                result.types = all;
//                DataChunk temp;
//                temp.Initialize(all);
//                temp.Reset();
                for (index_t index = shrinked[i].first.first; index < shrinked[i].first.second; index++) {
                    index_t pos = dataLeft.size();
                    if (dataLeft.size() == STANDARD_VECTOR_SIZE) {
                        hashes.Reset();
                        ExpressionExecutor executorL(dataLeft);
                        for (index_t j = 0; j < conditions.size(); j++) {
                            executorL.ExecuteExpression(*conditions[j].left, hashes.data[j]);
                        }
                        hash_table->Probe(hashes, dataLeft, result);
                        dataLeft.Reset();
                        pos = dataLeft.size();
                    }
                    //index_t posInData = state->left_hash_to_pos.at(index).second;
                    auto data = state->left_hash_to_Data.at(index).second;
                    for (index_t col = 0; col < data.size(); col++) {
                        dataLeft.data[col].count += 1;
                        dataLeft.data[col].SetValue(pos, data[col]);
                    }
                }
                hashes.Reset();
                ExpressionExecutor executorL(dataLeft);
                for (index_t j = 0; j < conditions.size(); j++) {
                    executorL.ExecuteExpression(*conditions[j].left, hashes.data[j]);
                }

                hash_table->Probe(hashes, dataLeft, result);
                finishI = std::chrono::high_resolution_clock::now();
                timeProbe += std::chrono::duration_cast<std::chrono::nanoseconds>(finishI - startI).count();
            }
            ///////////////////////////////////////////////////////////////////
            //PerformBuildAndProbe(state, shrinked, expected, index);
#else
            vector<std::thread> threads;
            for (size_t i = 0; i < concurentThreadsSupported; i++) {
                threads.push_back(std::thread(&PhysicalRadixJoin::PerformBuildAndProbe, this, state, std::ref(shrinked),
                                              std::ref(expected), std::ref(index)));
            }
            for (auto &t : threads) {
                t.join();
            }
#endif
#if TIMER
            auto finish = std::chrono::high_resolution_clock::now();
            std::cerr << "Performing build and probe took: "
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count() << "ns!"
                      << std::endl;
#endif
        }
        state->initialized = true;
#if TIMERWHOLE
        auto finishOp = std::chrono::high_resolution_clock::now();
        std::cerr << "The whole operator took: "
                  << std::chrono::duration_cast<std::chrono::nanoseconds>(finishOp - startOp).count() << "ns!"
                  << std::endl;
#endif
    }

    if (output_index < result.count) {
        result.GetChunk(output_index).Move(chunk);
        output_index += STANDARD_VECTOR_SIZE;
    } else {
        DataChunk nullChunk;
        nullChunk.Move(chunk);
        state->finished = true;
    }
}

//void PhysicalRadixJoin::PerformBuildAndProbe(PhysicalRadixJoinOperatorState *state,
//                                             vector<std::pair<std::pair<index_t, index_t>, std::pair<index_t, index_t>>> &shrinked,
//                                             vector<TypeId> &expected, std::atomic<index_t> &index) {
//    while (1) {
//        auto i = index++;
//        if (i >= shrinked.size()) {
//            break;
//        }
//        vector<TypeId> right;
//        for (index_t i = 0; i < state->right_data->types.size() - 1; i++) {
//            right.push_back(state->right_data->types.at(i));
//        }
//        auto start = std::chrono::high_resolution_clock::now();
//        //auto hash_table = make_unique<JoinHashTable>(conditions, right, duckdb::JoinType::RADIX);
//        auto hash_table = make_unique<RadixHashTable>(conditions, right, duckdb::JoinType::INNER, 2 *
//                                                                                                  dummy_hash_table->NextPow2_64(
//                                                                                                          shrinked[i].second.second -
//                                                                                                          shrinked[i].second.first));
//        //auto scanStructure = make_unique<JoinHashTable::ScanStructure>(*hash_table.get());
//        //auto scanStructure = make_unique<RadixHashTable::ScanStructure>(*hash_table.get());
//        DataChunk dataRight;
//        vector<TypeId> te;
//        for (auto &t : state->right_data->types) {
//            if (t != TypeId::HASH)
//                te.push_back(t);
//        }
//        dataRight.Initialize(te);
//        // The datachunk for the hashes of this partition
//        DataChunk hashes;
//        hashes.Initialize(hash_table->condition_types);
//        for (index_t index = shrinked[i].second.first; index < shrinked[i].second.second; index++) {
//            index_t pos = dataRight.size();
//            if (pos == STANDARD_VECTOR_SIZE) {
//                // If the datachunk is full, then insert it into the hashtable
//                // Make the hashes
//                hashes.Reset();
//                ExpressionExecutor executorR(dataRight);
//                for (index_t col = 0; col < conditions.size(); col++) {
//                    executorR.ExecuteExpression(*conditions[col].right, hashes.data[col]);
//                }
//                // Insert into the hashtable
//                hash_table->Build(hashes, dataRight);
//                // Reset the datachunk for next iteration
//                dataRight.Reset();
//                pos = dataRight.size();
//            }
//            for (index_t col = 0; col < state->right_data->types.size() - 1; col++) {
//                dataRight.data[col].count += 1;
//                dataRight.data[col].SetValue(pos, state->right_data->GetValue(col, index));
//            }
//        }
//        // After the end of this partition insert into hashtable
//        // Reset the hashes
//
//        hashes.Reset();
//        ExpressionExecutor executorR(dataRight);
//        for (index_t j = 0; j < conditions.size(); j++) {
//            executorR.ExecuteExpression(*conditions[j].right, hashes.data[j]);
//        }
//        // Insert into the hashtable
//        hash_table->Build(hashes, dataRight);
//
//        auto finish = std::chrono::high_resolution_clock::now();
//        timeBuild += std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
//        //// Up to this point, the right side is in the hashtable
//
//        // Now continue with the left side
//        start = std::chrono::high_resolution_clock::now();
////        DataChunk tempChunk;
////        tempChunk.Initialize(expected);
//        DataChunk dataLeft;
//        vector<TypeId> left;
//        for (index_t i = 0; i < state->left_data->types.size() - 1; i++) {
//            left.push_back(state->left_data->types.at(i));
//        }
//        dataLeft.Initialize(left);//state->left_data->types);
//        hashes.Initialize(hash_table->condition_types);
//        // left now contains all the information of the types the parent needs
//        for (auto &t : right) {
//            left.push_back(t);
//        }
//        result.types = left;
//        //results[i].types = left;
////        DataChunk temp;
////        temp.Initialize(left);
////        temp.Reset();
//        for (index_t index = shrinked[i].first.first; index < shrinked[i].first.second; index++) {
//            index_t pos = dataLeft.size();
//            if (dataLeft.size() == STANDARD_VECTOR_SIZE) {
//                hashes.Reset();
//                ExpressionExecutor executorL(dataLeft);
//                for (index_t j = 0; j < conditions.size(); j++) {
//                    executorL.ExecuteExpression(*conditions[j].left, hashes.data[j]);
//                }
//                hash_table->Probe(hashes, dataLeft, result);
//                dataLeft.Reset();
//                pos = dataLeft.size();
//            }
//            for (index_t col = 0; col < state->left_data->types.size() - 1; col++) {
//                dataLeft.data[col].count += 1;
//                dataLeft.data[col].SetValue(pos, state->left_data->GetValue(col, index));
//            }
//        }
//        hashes.Reset();
//        ExpressionExecutor executorL(dataLeft);
//        for (index_t j = 0; j < conditions.size(); j++) {
//            executorL.ExecuteExpression(*conditions[j].left, hashes.data[j]);
//        }
//
//        hash_table->Probe(hashes, dataLeft, result);
//        finish = std::chrono::high_resolution_clock::now();
//        timeProbe += std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
//    }
//}

void PhysicalRadixJoin::FillPartitions(PhysicalRadixJoinOperatorState *state, std::atomic<index_t> &index) {
    index_t i = 0;
    while (1) {
        //auto i = index++;
        if (i >= state->old_left_histogram->numberOfPartitions) {
            break;
        }
        for (index_t j = 0; j < state->old_left_histogram->numberOfBucketsPerPartition; j++) {
            //auto rangeLeft = state->left_histogram->getParition(i,j);
            //auto rangeRight = state->right_histogram->getParition(i,j);
            auto rangeLeft = state->old_left_histogram->getRangeAndPosition(i, j);
            auto rangeRight = state->old_right_histogram->getRangeAndPosition(i, j);
#if PREFETCH
            if(j == 0 || j == 1) {
                    __builtin_prefetch(&state->left_data->GetChunk(std::get<0>(rangeLeft)));
                    __builtin_prefetch(&state->left_data->GetChunk(std::get<1>(rangeLeft)));
                    __builtin_prefetch(&state->left_data->GetChunk(std::get<0>(rangeRight)));
                    __builtin_prefetch(&state->left_data->GetChunk(std::get<1>(rangeRight)));
            }
#endif
            partitions[i * state->old_left_histogram->numberOfBucketsPerPartition + j].first.first = std::get<0>(
                    rangeLeft);
            partitions[i * state->old_left_histogram->numberOfBucketsPerPartition + j].first.second = std::get<1>(
                    rangeLeft);
            partitions[i * state->old_right_histogram->numberOfBucketsPerPartition + j].second.first = std::get<0>(
                    rangeRight);
            partitions[i * state->old_right_histogram->numberOfBucketsPerPartition + j].second.second = std::get<1>(
                    rangeRight);
        }
        i++;
    }
}

void PhysicalRadixJoin::RadixJoinPartitionWorkerLeft(PhysicalRadixJoinOperatorState *state, index_t startOfPartitions,
                                                     index_t endOfPartitions, size_t bitmask, size_t bitMaskNextRun,
                                                     size_t shift, size_t run, size_t partitionNumber) {
    // Iterate over the data given to this thread
    for (index_t toOrder = startOfPartitions; toOrder < endOfPartitions; toOrder++) {
        // Get the hash of the element
        auto hash = state->left_hash_to_Data.at(toOrder).first;//state->left_hash_to_pos.at(toOrder).first;//state->left_data->GetValue(state->left_data->column_count() - 1,
                    //                           toOrder);//state->left_hashes[toOrder / STANDARD_VECTOR_SIZE]->GetValue(toOrder % STANDARD_VECTOR_SIZE);
        // Get the partition the element belongs to
       // auto partition = ((hash.value_.hash & bitmask) >> shift);
        auto partition = ((hash & bitmask) >> shift);
        assert(partition >= 0 && partition < state->old_left_histogram->numberOfPartitions *
                                             state->old_left_histogram->numberOfBucketsPerPartition);
        if (run < runs - 1) {
            // Get the partition in the next run
            //auto partitionNextRun = (hash.value_.hash & bitMaskNextRun) >> (shift + numberOfBits[run]);
            auto partitionNextRun = (hash & bitMaskNextRun) >> (shift + numberOfBits[run]);
            // Increment the counter for the partitionNextRun in partition
            state->left_histogram->IncrementBucketCounter(
                    partitionNumber * state->old_left_histogram->numberOfBucketsPerPartition + partition,
                    partitionNextRun);
        }
        // Get the position where to insert from the old left histogram
        index_t position = state->old_left_histogram->getInsertPlace(partitionNumber, partition);
        state->left_hash_to_DataSwap.at(position) = state->left_hash_to_Data.at(toOrder);
        //state->left_hash_to_posSwap.at(position) = state->left_hash_to_pos.at(toOrder);
//        for (index_t col = 0; col < state->left_data->types.size(); col++) {
//            state->left_data_partitioned->SetValue(col, position, state->left_data->GetValue(col, toOrder));
//        }
    }
}

void PhysicalRadixJoin::RadixJoinPartitionWorkerRight(PhysicalRadixJoinOperatorState *state, index_t startOfPartitions,
                                                      index_t endOfPartitions, size_t bitmask, size_t bitMaskNextRun,
                                                      size_t shift, size_t run, size_t partitionNumber) {
    for (index_t toOrder = startOfPartitions; toOrder < endOfPartitions; toOrder++) {
        auto hash = state->right_hash_to_Data.at(toOrder).first;//state->right_hash_to_pos.at(toOrder).first;//state->right_data->GetValue(state->right_data->column_count() - 1,
                    //                            toOrder);//state->right_hashes[toOrder / STANDARD_VECTOR_SIZE]->GetValue(toOrder % STANDARD_VECTOR_SIZE);
        auto partition = ((hash & bitmask) >> shift);
        assert(partition >= 0 && partition < state->old_right_histogram->numberOfPartitions *
                                             state->old_right_histogram->numberOfBucketsPerPartition);
        if (run < runs - 1) {
            // Get the partition in the next run
            auto partitionNextRun = (hash & bitMaskNextRun) >> (shift + numberOfBits[run]);
            // Increment the counter for the partitionNextRun in partition
            state->right_histogram->IncrementBucketCounter(
                    partitionNumber * state->old_right_histogram->numberOfBucketsPerPartition + partition,
                    partitionNextRun);
        }
        // Get the position where to insert from the old right histogram
        index_t position = state->old_right_histogram->getInsertPlace(partitionNumber, partition);
        //state->right_hash_to_posSwap.at(position) = state->right_hash_to_pos.at(toOrder);
        state->right_hash_to_DataSwap.at(position) = state->right_hash_to_Data.at(toOrder);
        //for (index_t col = 0; col < state->right_data->types.size(); col++) {
        //    state->right_data_partitioned->SetValue(col, position, state->right_data->GetValue(col, toOrder));
        //}
    }
}

void PhysicalRadixJoin::RadixJoinSingleThreadedLeft(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run) {
    // Get the bitmask for this partition
    size_t bitmask = ((1 << numberOfBits[run]) - 1) << shift;
    // Get the bitmask for the next run
    size_t bitMaskNextRun = ((1 << numberOfBits[run + 1]) - 1) << (shift + numberOfBits[run]);
    // move the perviously built histograms to the old histogram pointer to store them for this run
    state->old_left_histogram = std::move(state->left_histogram);
    if (run < runs - 1) {
        size_t amountOfPartitionsInNextRun = (1 << numberOfBits[run + 1]);
        state->left_histogram = make_unique<Histogram>(1 << (shift + numberOfBits[run]), amountOfPartitionsInNextRun);
    }
    // Calculate the range of the left and right histograms
    state->old_left_histogram->Range();
    vector<std::thread> threads;
    // Iterate over all the partitions in the old histogram (as this contains the actual information)
    for (index_t part = 0; part < state->old_left_histogram->numberOfPartitions; part++) {
        // Get the start of the partition
        auto back = state->old_left_histogram->getRangeOfSuperpartition(part);
#if SINGLETHREADED
        RadixJoinPartitionWorkerLeft(state, back.first, back.second, bitmask, bitMaskNextRun, shift, run, part);
#if PREFETCH
        if(part < state->old_left_histogram->numberOfPartitions-1) {
            __builtin_prefetch(&state->left_data->GetChunk(state->old_left_histogram->getRangeOfSuperpartition(part+1).first));
            __builtin_prefetch(&state->left_data->GetChunk(state->old_left_histogram->getRangeOfSuperpartition(part+1).second));
        }
#endif
#else
        threads.push_back(
                std::thread(&PhysicalRadixJoin::RadixJoinPartitionWorkerLeft, this, state, back.first, back.second,
                            bitmask, shift, run, part));
#endif
    }
    for (auto &t : threads) {
        t.join();
    }
}

void PhysicalRadixJoin::RadixJoinSingleThreadedRight(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run) {
    // Get the bitmask for this partition
    size_t bitmask = ((1 << numberOfBits[run]) - 1) << shift;
    // Get the bitmask for the next run
    size_t bitMaskNextRun = ((1 << numberOfBits[run + 1]) - 1) << (shift + numberOfBits[run]);
    // move the perviously built histograms to the old histogram pointer to store them for this run
    state->old_right_histogram = std::move(state->right_histogram);
    if (run < runs - 1) {
        size_t amountOfPartitionsInNextRun = (1 << numberOfBits[run + 1]);
        state->right_histogram = make_unique<Histogram>(1 << (shift + numberOfBits[run]), amountOfPartitionsInNextRun);
    }
    // Calculate the range of the right and right histograms
    state->old_right_histogram->Range();
    vector<std::thread> threads;
    {
        // Right side
        // Iterate over all the partitions in the old histogram (as this contains the actual information)
        for (index_t part = 0; part < state->old_right_histogram->numberOfPartitions; part++) {
            // Get the start of the partition
            auto back = state->old_right_histogram->getRangeOfSuperpartition(part);
#if SINGLETHREADED
//            for (index_t toOrder = back.first; toOrder < back.second; toOrder++) {
//                auto hash = state->right_data->GetValue(state->right_data->column_count() - 1,
//                                                        toOrder);//state->right_hashes[toOrder / STANDARD_VECTOR_SIZE]->GetValue(toOrder % STANDARD_VECTOR_SIZE);
//                auto partition = ((hash.value_.hash & bitmask) >> shift);
//                assert(partition >= 0 && partition < state->old_right_histogram->numberOfPartitions *
//                                                     state->old_right_histogram->numberOfBucketsPerPartition);
//                if (run < runs - 1) {
//                    // Get the partition in the next run
//                    auto partitionNextRun = (hash.value_.hash & bitMaskNextRun) >> (shift + numberOfBits[run]);
//                    // Increment the counter for the partitionNextRun in partition
//                    state->right_histogram->IncrementBucketCounter(
//                            part * state->old_right_histogram->numberOfBucketsPerPartition + partition,
//                            partitionNextRun);
//                }
//                // Get the position where to insert from the old right histogram
//                index_t position = state->old_right_histogram->getInsertPlace(part, partition);
//                for (index_t col = 0; col < state->right_data->types.size(); col++) {
//                    state->right_data_partitioned->SetValue(col, position, state->right_data->GetValue(col, toOrder));
//                }
//            }
            RadixJoinPartitionWorkerRight(state, back.first, back.second, bitmask, bitMaskNextRun, shift, run, part);
#if PREFETCH
            if(part < state->old_right_histogram->numberOfPartitions-1) {
                __builtin_prefetch(&state->right_data->GetChunk(state->old_right_histogram->getRangeOfSuperpartition(part+1).first));
                __builtin_prefetch(&state->right_data->GetChunk(state->old_right_histogram->getRangeOfSuperpartition(part+1).second));
            }
#endif
#else
            threads.push_back(
                    std::thread(&PhysicalRadixJoin::RadixJoinPartitionWorkerRight, this, state, back.first, back.second,
                                bitmask, shift, run, part));
#endif
        }
    }
    for (auto &t : threads) {
        t.join();
    }
}

/*void PhysicalRadixJoin::RadixJoinSingleThreaded(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run) {
    // Get the bitmask for this partition
    size_t bitmask = ((1 << numberOfBits[run]) - 1) << shift;
    // move the perviously built histograms to the old histogram pointer to store them for this run
    state->old_left_histogram = std::move(state->left_histogram);
    state->old_right_histogram = std::move(state->right_histogram);
    // If there is a run after the current one, we get memory for the new histograms
    if (run < runs - 1) {
        size_t amountOfPartitionsInNextRun = (1 << numberOfBits[run + 1]);
        state->left_histogram = make_unique<Histogram>(1 << (shift + numberOfBits[run]), amountOfPartitionsInNextRun);
        state->right_histogram = make_unique<Histogram>(1 << (shift + numberOfBits[run]), amountOfPartitionsInNextRun);
    }

    // Calculate the range of the left and right histograms
    state->old_left_histogram->Range();
    state->old_right_histogram->Range();

    // start iterating over the collections
    // These variables describe the start of the partitions we are about to split
    // Working on the left side of the join
    vector<std::thread> threads;
    {
        // Left side
        // Iterate over all the partitions in the old histogram (as this contains the actual information)
        for (index_t part = 0; part < state->old_left_histogram->numberOfPartitions; part++) {
            // Get the start of the partition
            auto back = state->old_left_histogram->getRangeOfSuperpartition(part);
#if SINGLETHREADED
            RadixJoinPartitionWorkerLeft(state, back.first, back.second, bitmask, shift, 0, run, part);
#else
            threads.push_back(
                    std::thread(&PhysicalRadixJoin::RadixJoinPartitionWorkerLeft, this, state, back.first, back.second,
                                bitmask, shift, run, part));
#endif
        }

        // Right side
        for (index_t part = 0; part < state->old_right_histogram->numberOfPartitions; part++) {
            auto back = state->old_right_histogram->getRangeOfSuperpartition(part);
#if SINGLETHREADED
            RadixJoinPartitionWorkerRight(state, back.first, back.second, bitmask, 0, shift, run, part);
#else
            threads.push_back(
                    std::thread(&PhysicalRadixJoin::RadixJoinPartitionWorkerRight, this, state, back.first, back.second,
                                bitmask, shift, run, part));
#endif
        }

    }
    for (auto &t : threads) {
        t.join();
    }
}*/

unique_ptr<PhysicalOperatorState> PhysicalRadixJoin::GetOperatorState() {
    return make_unique<PhysicalRadixJoinOperatorState>(children[0].get(), children[1].get());
}
