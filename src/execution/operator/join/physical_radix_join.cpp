//
// Created by Adrian Riedl on 30.11.19.
//

#include <iostream>
#include <thread>
#include <fstream>
#include <atomic>

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

    std::chrono::time_point<std::chrono::high_resolution_clock> start;
    start = std::chrono::high_resolution_clock::now();
    timeBuild = start - start;
    timeProbe = start - start;

    orderinghashBuild = start - start;
    extractingValBuild = start - start;
    writingDataBuild = start - start;
    gettingHashtable = start - start;
    gettingDChunk = start - start;
    orderinghashProbe = start - start;;
    extractingValProbe = start - start;;
    writingDataProbe = start - start;
    remaining = start - start;
}

void PhysicalRadixJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
    auto startOp = std::chrono::high_resolution_clock::now();
    auto state = reinterpret_cast<PhysicalRadixJoinOperatorState *>(state_);
    auto left_typesGlobal = children[0]->GetTypes();
    index_t leftSize = 0;
    for (auto &t : left_typesGlobal) {
        leftSize += GetTypeIdSize(t);
    }
    auto right_typesGlobal = children[1]->GetTypes();
    index_t rightSize = 0;
    for (auto &t : right_typesGlobal) {
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
            auto left_typesLocal = left_types;

            size_t size = 0;
            for (auto &t : left_types) {
                size += GetTypeIdSize(t);
            }

            left_typesLocal.push_back(TypeId::HASH);
            left_chunk_withHash.Initialize(left_typesLocal);

            //! Fetch all the chunks from the left side
            // Generate the histogram for the left side
            state->left_histogram = make_unique<Histogram>(1, 1 << numberOfBits[0]);
            // Initialize the left join keys
            state->left_join_keys.Initialize(dummy_hash_table->condition_types);
            while (true) {
                left_chunk.Reset();
                left_chunk_withHash.Reset();
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
                // Now fill the left histogram
                for (index_t hashNumber = 0; hashNumber < left_chunk.size(); hashNumber++) {
                    auto hash = hashes->GetValue(hashNumber);
                    size_t pos = hash.value_.hash & hash_bit_mask;
                    state->left_histogram->IncrementBucketCounter(0, pos);
                    for (index_t i = 0; i < left_types.size(); i++) {
                        left_chunk_withHash.GetVector(i).count++;
                        left_chunk_withHash.GetVector(i).SetValue(hashNumber,
                                                                  left_chunk.GetVector(i).GetValue(hashNumber));
                    }
                    left_chunk_withHash.GetVector(left_types.size()).count++;
                    left_chunk_withHash.GetVector(left_types.size()).SetValue(hashNumber, hash);
                }
                state->left_data->Append(left_chunk_withHash);
                state->left_data_partitioned->Append(left_chunk_withHash);
            }
#if TIMER
            auto finish = std::chrono::high_resolution_clock::now();
            elapsed_seconds = finish - start;
            std::cerr << "Collecting the left side took: " << elapsed_seconds.count() << "s!" << std::endl;
#endif
            //! Start the partitioning phase with #runs runs and the bits in numberOfBits
#if TIMER
            start = std::chrono::high_resolution_clock::now();
#endif


            size_t shift = 0;
            for (size_t r = 0; r < runs; r++) {
                RadixJoinSingleThreadedLeft(state, shift, r);
                shift += numberOfBits[r];

                auto temp = std::move(state->left_data);
                state->left_data = std::move(state->left_data_partitioned);
                state->left_data_partitioned = std::move(temp);
            }
#if TIMER
            finish = std::chrono::high_resolution_clock::now();
            elapsed_seconds = finish - start;
            std::cerr << "Partitioning left side took: " << elapsed_seconds.count() << "s!" << std::endl;
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
            auto right_typesLocal = right_types;

            size_t size = 0;
            for (auto &t : right_types) {
                size += GetTypeIdSize(t);
            }
            right_typesLocal.push_back(TypeId::HASH);
            right_chunk_withHash.Initialize(right_typesLocal);

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
                // Now fill the right histogram
                for (index_t hashNumber = 0; hashNumber < right_chunk.size(); hashNumber++) {
                    auto hash = hashes->GetValue(hashNumber);
                    size_t pos = hash.value_.hash & hash_bit_mask;
                    state->right_histogram->IncrementBucketCounter(0, pos);
                    for (index_t i = 0; i < right_types.size(); i++) {
                        right_chunk_withHash.GetVector(i).count++;
                        right_chunk_withHash.GetVector(i).SetValue(hashNumber,
                                                                   right_chunk.GetVector(i).GetValue(hashNumber));
                    }
                    right_chunk_withHash.GetVector(right_types.size()).count++;
                    right_chunk_withHash.GetVector(right_types.size()).SetValue(hashNumber, hash);
                }
                state->right_data->Append(right_chunk_withHash);
                state->right_data_partitioned->Append(right_chunk_withHash);
            }

#if TIMER
            auto finish = std::chrono::high_resolution_clock::now();
            elapsed_seconds = finish - start;
            std::cerr << "Collecting the right side took: " << elapsed_seconds.count() << "s!" << std::endl;
#endif
            //! Start the partitioning phase with #runs runs and the bits in numberOfBits
#if TIMER
            start = std::chrono::high_resolution_clock::now();
#endif


            //! Start the partitioning phase with #runs runs and the bits in numberOfBits
            size_t shift = 0;
            for (size_t r = 0; r < runs; r++) {
                RadixJoinSingleThreadedRight(state, shift, r);
                shift += numberOfBits[r];

                auto temp = std::move(state->right_data);
                state->right_data = std::move(state->right_data_partitioned);
                state->right_data_partitioned = std::move(temp);
            }
#if TIMER
            finish = std::chrono::high_resolution_clock::now();
            std::cerr << "Partitioning right side took: " << elapsed_seconds.count() << "s!" << std::endl;
#endif
        }

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
        {
            std::atomic<index_t> index;
            index = 0;
#if TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
            FillPartitions(state, index);
#if TIMER
            auto finish = std::chrono::high_resolution_clock::now();
            elapsed_seconds = finish - start;
            std::cerr << "Setting the partitions took: " << elapsed_seconds.count() << "ns!" << std::endl;
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
                    shrinked.push_back(p);
                }
            }
            assert(shrinked.size() == partitionsrelevant);

#if TIMER
            auto finish = std::chrono::high_resolution_clock::now();
            elapsed_seconds = finish - start;
            std::cerr << "Setting the reduced partitions took: " << elapsed_seconds.count() << "ns!" << std::endl;
#endif
        }
        {
#if TIMER
            auto startPerfBuildAndProbe = std::chrono::high_resolution_clock::now();
#endif

            std::atomic<index_t> indexGlob;
            indexGlob = 0;
            // Assume we have at least one pair in shrink
            ///////////////////////////////////////////////////////////////////
#if TIMER
            auto startGetDChunk = std::chrono::high_resolution_clock::now();
#endif
            DataChunk dataRight;
            dataRight.Initialize(right_typesGlobal);
            // The datachunk for the hashes of this partition
            DataChunk hashes;
            hashes.Initialize(dummy_hash_table->condition_types);
#if TIMER
            auto endGetDChunk = std::chrono::high_resolution_clock::now();
            gettingDChunk += endGetDChunk - startGetDChunk;
#endif
            result.types = expected;
            while (1) {
#if TIMER
                auto startBuild = std::chrono::high_resolution_clock::now();
#endif
                auto i = indexGlob++;
                if (i >= shrinked.size()) {
                    break;
                }
#if TIMER
                auto startGettingHT = std::chrono::high_resolution_clock::now();
#endif
                auto hash_table = make_unique<JoinHashTable>(conditions, right_typesGlobal, duckdb::JoinType::INNER);
                auto scanStructure = make_unique<JoinHashTable::ScanStructure>(*hash_table.get());
#if TIMER
                auto endGettingHT = std::chrono::high_resolution_clock::now();
                gettingHashtable += endGettingHT - startGettingHT;
#endif
                dataRight.Reset();
                hashes.Reset();
                for (index_t index = shrinked[i].second.first; index < shrinked[i].second.second; index++) {
                    index_t pos = dataRight.size();
                    if (pos == STANDARD_VECTOR_SIZE) {
                        // If the datachunk is full, then insert it into the hashtable
                        // Make the hashes
#if TIMER
                        auto startOrderingHash = std::chrono::high_resolution_clock::now();
#endif
                        hashes.Reset();
                        ExpressionExecutor executorR(dataRight);
                        for (index_t col = 0; col < conditions.size(); col++) {
                            executorR.ExecuteExpression(*conditions[col].right, hashes.data[col]);
                        }
#if TIMER
                        auto endOrderingHash = std::chrono::high_resolution_clock::now();
                        orderinghashBuild += endOrderingHash - startOrderingHash;
#endif
                        // Insert into the hashtable
                        try {
                            hash_table->Build(hashes, dataRight);
                        } catch (ConversionException &e) {
                            std::cout << "An exception in HT build" << std::endl;
                        }
                        // Reset the datachunk for next iteration
                        dataRight.Reset();
                        pos = dataRight.size();
                    }
#if TIMER
                    auto startExtract = std::chrono::high_resolution_clock::now();
#endif
                    auto data = state->right_data->GetRow(index);
#if TIMER
                    auto endExtract = std::chrono::high_resolution_clock::now();
                    extractingValBuild += endExtract - startExtract;
                    startExtract = std::chrono::high_resolution_clock::now();
#endif
                    for (index_t col = 0; col < data.size() - 1; col++) {
                        dataRight.data[col].count += 1;
                        dataRight.data[col].SetValue(pos, data[col]);
                    }
#if TIMER
                    endExtract = std::chrono::high_resolution_clock::now();
                    writingDataBuild += endExtract - startExtract;
#endif
                }
                // After the end of this partition insert into hashtable
                // Reset the hashes

#if TIMER
                auto startOrderingHash = std::chrono::high_resolution_clock::now();
#endif
                hashes.Reset();
                ExpressionExecutor executorR(dataRight);
                for (index_t j = 0; j < conditions.size(); j++) {
                    executorR.ExecuteExpression(*conditions[j].right, hashes.data[j]);
                }
#if TIMER
                auto endOrderingHash = std::chrono::high_resolution_clock::now();
                orderinghashBuild += endOrderingHash - startOrderingHash;
#endif
                // Insert into the hashtable
                try {
                    hash_table->Build(hashes, dataRight);
                } catch (ConversionException &e) {
                    std::cout << "An exception in HT build" << std::endl;
                }

#if TIMER
                auto finishBuild = std::chrono::high_resolution_clock::now();
                timeBuild += finishBuild - startBuild;
                //// Up to this point, the right side is in the hashtable

                // Now continue with the left side
                auto startProbe = std::chrono::high_resolution_clock::now();
                startGetDChunk = std::chrono::high_resolution_clock::now();
#endif
                DataChunk dataLeft;

                dataLeft.Initialize(left_typesGlobal);
                hashes.Initialize(hash_table->condition_types);
#if TIMER
                endGetDChunk = std::chrono::high_resolution_clock::now();
                gettingDChunk += endGetDChunk - startGetDChunk;
#endif

#if TIMER
                auto startRemaining = std::chrono::high_resolution_clock::now();
#endif
                vector<TypeId> all;
                for (auto &t : left_typesGlobal) {
                    all.push_back(t);
                }
                for (auto &t : right_typesGlobal) {
                    all.push_back(t);
                }
                result.types = all;
                DataChunk tempStorage;
                tempStorage.Initialize(all);
#if TIMER
                auto endRemaining = std::chrono::high_resolution_clock::now();
                remaining += endRemaining - startRemaining;
#endif
                for (index_t index = shrinked[i].first.first; index < shrinked[i].first.second; index++) {
                    index_t pos = dataLeft.size();
                    if (dataLeft.size() == STANDARD_VECTOR_SIZE) {
#if TIMER
                        auto startOrderingHash = std::chrono::high_resolution_clock::now();
#endif
                        hashes.Reset();
                        ExpressionExecutor executorL(dataLeft);
                        for (index_t j = 0; j < conditions.size(); j++) {
                            executorL.ExecuteExpression(*conditions[j].left, hashes.data[j]);
                        }
#if TIMER
                        auto endOrderingHash = std::chrono::high_resolution_clock::now();
                        orderinghashProbe += endOrderingHash - startOrderingHash;
#endif
                        try {
                            scanStructure = hash_table->Probe(hashes);
                        } catch (ConversionException &e) {
                            std::cout << "An exception in HT probe" << std::endl;
                        }
                        dataLeft.Reset();
                        pos = dataLeft.size();
                        do {
                            tempStorage.Reset();
                            scanStructure->Next(hashes, dataLeft, tempStorage);
#if TIMER
                            auto appendTimerStart = std::chrono::high_resolution_clock::now();
#endif
                            result.Append(tempStorage);
#if TIMER
                            auto appendTimerEnd = std::chrono::high_resolution_clock::now();
                            timeForAppending += appendTimerEnd - appendTimerStart;
#endif
                        } while (tempStorage.size() > 0);
                    }
#if TIMER
                    auto startExtract = std::chrono::high_resolution_clock::now();
#endif
                    auto data = state->left_data->GetRow(index);
#if TIMER
                    auto endExtract = std::chrono::high_resolution_clock::now();
                    extractingValProbe += endExtract - startExtract;
                    startExtract = std::chrono::high_resolution_clock::now();
#endif
                    for (index_t col = 0; col < data.size() - 1; col++) {
                        dataLeft.data[col].count += 1;
                        dataLeft.data[col].SetValue(pos, data[col]);
                    }
#if TIMER
                    endExtract = std::chrono::high_resolution_clock::now();
                    writingDataProbe += endExtract - startExtract;
#endif
                }
                hashes.Reset();
#if TIMER
                startOrderingHash = std::chrono::high_resolution_clock::now();
#endif
                ExpressionExecutor executorL(dataLeft);
                for (index_t j = 0; j < conditions.size(); j++) {
                    executorL.ExecuteExpression(*conditions[j].left, hashes.data[j]);
                }
#if TIMER
                endOrderingHash = std::chrono::high_resolution_clock::now();
                orderinghashProbe += endOrderingHash - startOrderingHash;
#endif

                try {
                    scanStructure = hash_table->Probe(hashes);
                } catch (ConversionException &e) {
                    std::cout << "An exception in HT probe" << std::endl;
                }
                do {
                    tempStorage.Reset();
                    scanStructure->Next(hashes, dataLeft, tempStorage);
#if TIMER
                    auto appendTimerStart = std::chrono::high_resolution_clock::now();
#endif
                    result.Append(tempStorage);
#if TIMER
                    auto appendTimerEnd = std::chrono::high_resolution_clock::now();
                    timeForAppending += appendTimerEnd - appendTimerStart;
#endif
                } while (tempStorage.size() > 0);


#if TIMER
                auto finishProbe = std::chrono::high_resolution_clock::now();
                timeProbe += finishProbe - startProbe;
#endif
            }
#if TIMER
            auto finishPerfBuildAndProbe = std::chrono::high_resolution_clock::now();
            elapsed_seconds = finishPerfBuildAndProbe - startPerfBuildAndProbe;
            std::cerr << "Performing build and probe took: " << elapsed_seconds.count() << "s!" << std::endl;
            std::cerr << "Performing orderingHashBuild took: " << orderinghashBuild.count() << "s!" << std::endl;
            std::cerr << "Performing gettingHashtable took: " << gettingHashtable.count() << "s!" << std::endl;
            std::cerr << "Performing extractingValBuild took: " << extractingValBuild.count() << "s!" << std::endl;
            std::cerr << "Performing writingDataBuild took: " << writingDataBuild.count() << "s!" << std::endl;
            std::cerr << "Performing gettingDChunk took: " << gettingDChunk.count() << "s!" << std::endl;
            std::cerr << "Performing orderingHashProbe took: " << orderinghashProbe.count() << "s!" << std::endl;
            std::cerr << "Performing extractingValProbe took: " << extractingValProbe.count() << "s!" << std::endl;
            std::cerr << "Performing writingDataProbe took: " << writingDataProbe.count() << "s!" << std::endl;
            std::cerr << "Performing remaining took: " << remaining.count() << "s!" << std::endl;
#endif
        }
        state->initialized = true;
#if TIMERWHOLE
        auto finishOp = std::chrono::high_resolution_clock::now();
        elapsed_seconds = finishOp - startOp;
        std::cerr << "The whole operator took: " << elapsed_seconds.count() << "s!" << std::endl;
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

void __attribute__((always_inline))
PhysicalRadixJoin::FillPartitions(PhysicalRadixJoinOperatorState *state, std::atomic<index_t> &index) {
    index_t i = 0;
    while (1) {
        //auto i = index++;
        if (i >= state->old_left_histogram->numberOfPartitions) {
            break;
        }
        for (index_t j = 0; j < state->old_left_histogram->numberOfBucketsPerPartition; j++) {
            auto rangeLeft = state->old_left_histogram->getRangeAndPosition(i, j);
            auto rangeRight = state->old_right_histogram->getRangeAndPosition(i, j);
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
        auto values = state->left_data->GetRow(toOrder);
        auto hash = values[values.size() - 1].value_.hash;
        //auto entry = state->left_tuples->GetEntry(toOrder);
        auto partition = ((hash & bitmask) >> shift);
        assert(partition >= 0 && partition < state->old_left_histogram->numberOfPartitions *
                                             state->old_left_histogram->numberOfBucketsPerPartition);
        if (run < runs - 1) {
            // Get the partition in the next run
            auto partitionNextRun = (hash & bitMaskNextRun) >> (shift + numberOfBits[run]);
            // Increment the counter for the partitionNextRun in partition
            state->left_histogram->IncrementBucketCounter(
                    partitionNumber * state->old_left_histogram->numberOfBucketsPerPartition + partition,
                    partitionNextRun);
        }
        // Get the position where to insert from the old left histogram
        index_t position = state->old_left_histogram->getInsertPlace(partitionNumber, partition);
        for (index_t i = 0; i < values.size(); i++) {
            state->left_data_partitioned->SetValue(i, position, values[i]);
        }
    }
}

void PhysicalRadixJoin::RadixJoinPartitionWorkerRight(PhysicalRadixJoinOperatorState *state, index_t startOfPartitions,
                                                      index_t endOfPartitions, size_t bitmask, size_t bitMaskNextRun,
                                                      size_t shift, size_t run, size_t partitionNumber) {
    for (index_t toOrder = startOfPartitions; toOrder < endOfPartitions; toOrder++) {
        auto values = state->right_data->GetRow(toOrder);
        auto hash = values[values.size() - 1].value_.hash;
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
        for (index_t i = 0; i < values.size(); i++) {
            state->right_data_partitioned->SetValue(i, position, values[i]);
        }
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
    // Iterate over all the partitions in the old histogram (as this contains the actual information)
    for (index_t part = 0; part < state->old_left_histogram->numberOfPartitions; part++) {
        // Get the start of the partition
        auto back = state->old_left_histogram->getRangeOfSuperpartition(part);
        RadixJoinPartitionWorkerLeft(state, back.first, back.second, bitmask, bitMaskNextRun, shift, run, part);
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
    {
        // Right side
        // Iterate over all the partitions in the old histogram (as this contains the actual information)
        for (index_t part = 0; part < state->old_right_histogram->numberOfPartitions; part++) {
            // Get the start of the partition
            auto back = state->old_right_histogram->getRangeOfSuperpartition(part);
            RadixJoinPartitionWorkerRight(state, back.first, back.second, bitmask, bitMaskNextRun, shift, run, part);
        }
    }
}

unique_ptr<PhysicalOperatorState> PhysicalRadixJoin::GetOperatorState() {
    return make_unique<PhysicalRadixJoinOperatorState>(children[0].get(), children[1].get());
}
