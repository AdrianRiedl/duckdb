//
// Created by Adrian Riedl on 30.11.19.
//

#include <iostream>
#include <thread>
#include <atomic>

#include "duckdb/execution/operator/join/physical_radix_join.hpp"
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
    auto concurrent = std::thread::hardware_concurrency();
    if (!concurrent) {
        concurrent = 1;
    }
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

            size_t size = 0;
            for (auto &t : left_types) {
                size += GetTypeIdSize(t);
            }

            state->left_tuples = make_unique<EntryStorage>(CHUNKSIZE, size, left_types, concurrent);
            state->left_tuplesSwap = make_unique<EntryStorage>(CHUNKSIZE, size, left_types, concurrent);

            //! Fetch all the chunks from the left side
            // Generate the histogram for the left side
            state->left_histogram = make_unique<Histogram>(1, 1 << numberOfBits[0]);
            // Initialize the left join keys
            state->left_join_keys.Initialize(dummy_hash_table->condition_types);
            while (true) {
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
                // Now fill the left histogram
                for (index_t hashNumber = 0; hashNumber < left_chunk.size(); hashNumber++) {
                    auto hash = hashes->GetValue(hashNumber);
                    size_t pos = hash.value_.hash & hash_bit_mask;
                    state->left_histogram->IncrementBucketCounter(0, pos);
                }
                state->left_tuples->insert(hashes, left_chunk);
                state->left_tuplesSwap->insert(hashes, left_chunk);
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
                RadixJoinSingleThreadedLeft(state, shift, r, concurrent);
                shift += numberOfBits[r];

                auto temp = std::move(state->left_tuples);
                state->left_tuples = std::move(state->left_tuplesSwap);
                state->left_tuplesSwap = std::move(temp);
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

            size_t size = 0;
            for (auto &t : right_types) {
                size += GetTypeIdSize(t);
            }

            state->right_tuples = make_unique<EntryStorage>(CHUNKSIZE, size, right_types, concurrent);
            state->right_tuplesSwap = make_unique<EntryStorage>(CHUNKSIZE, size, right_types, concurrent);

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
                }
                state->right_tuples->insert(hashes, right_chunk);
                state->right_tuplesSwap->insert(hashes, right_chunk);
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
                RadixJoinSingleThreadedRight(state, shift, r, concurrent);
                shift += numberOfBits[r];

                auto temp = std::move(state->right_tuples);
                state->right_tuples = std::move(state->right_tuplesSwap);
                state->right_tuplesSwap = std::move(temp);
            }
#if TIMER
            finish = std::chrono::high_resolution_clock::now();
            elapsed_seconds = finish - start;
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
        for (auto t : left_typesGlobal) {
            expected.push_back(t);
        }
        for (auto t : right_typesGlobal) {
            expected.push_back(t);
        }
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

            std::atomic<uint64_t> indexGlob;
            std::mutex lock;
            indexGlob = 0;


            std::vector<std::thread> threads;
            results.resize(concurrent);
            for (auto &r : results) {
                r.types = expected;
            }



            for (index_t i = 0; i < concurrent; i++) {
                threads.push_back(std::thread(&PhysicalRadixJoin::PerformBAP, this, state, std::ref(left_typesGlobal),
                                              std::ref(right_typesGlobal), std::ref(shrinked), std::ref(indexGlob),
                                              std::ref(results[i]), std::ref(lock), i));
            }
            for(auto &t : threads) {
                t.join();
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

    if (output_index < results.size()) {
        while(results[output_index].count == 0) {
            output_index++;
            if(output_index >= results.size()) {
                goto empty;
            }
        }
        results[output_index].GetChunk(output_internal).Move(chunk);
        output_internal += STANDARD_VECTOR_SIZE;
        if(output_internal >= results[output_index].count) {
            output_index++;
            output_internal = 0;
        }
    } else {
        empty:
        DataChunk nullChunk;
        nullChunk.Move(chunk);
        state->finished = true;
    }
}

void PhysicalRadixJoin::PerformBAP(PhysicalRadixJoinOperatorState *state, vector<TypeId> &left_typesGlobal,
                                   vector<TypeId> &right_typesGlobal,
                                   vector<std::pair<std::pair<index_t, index_t>, std::pair<index_t, index_t>>> &shrinked,
                                   std::atomic<uint64_t> &indexGlob, ChunkCollection &result, std::mutex &lock, index_t threadNumber) {
    DataChunk dataRight;
    dataRight.Initialize(right_typesGlobal);
    // The datachunk for the hashes of this partition
    DataChunk hashes;
    hashes.Initialize(dummy_hash_table->condition_types);
    while (1) {
        auto i = indexGlob++;
        if (i >= shrinked.size()) {
            break;
        }
        auto hash_table = make_unique<RadixHashTable>(conditions, right_typesGlobal, duckdb::JoinType::INNER, 2 *
                                                                                                              dummy_hash_table->NextPow2_64(
                                                                                                                      shrinked[i].second.second -
                                                                                                                      shrinked[i].second.first));
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
                try {
                    hash_table->Build(hashes, dataRight);
                } catch (ConversionException &e) {
                    std::cout << "An exception in HT build" << std::endl;
                }
                // Reset the datachunk for next iteration
                dataRight.Reset();
                pos = dataRight.size();
            }
            auto data = state->right_tuples->ExtractValues(index, threadNumber);
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
        try {
            hash_table->Build(hashes, dataRight);
        } catch (ConversionException &e) {
            std::cout << "An exception in HT build" << std::endl;
        }

        DataChunk dataLeft;

        dataLeft.Initialize(left_typesGlobal);
        hashes.Initialize(hash_table->condition_types);

        for (index_t index = shrinked[i].first.first; index < shrinked[i].first.second; index++) {
            index_t pos = dataLeft.size();
            if (dataLeft.size() == STANDARD_VECTOR_SIZE) {
                hashes.Reset();
                ExpressionExecutor executorL(dataLeft);
                for (index_t j = 0; j < conditions.size(); j++) {
                    executorL.ExecuteExpression(*conditions[j].left, hashes.data[j]);
                }
                try {
                    hash_table->Probe(hashes, dataLeft, result);
                } catch (ConversionException &e) {
                    std::cout << "An exception in HT probe" << std::endl;
                }
                dataLeft.Reset();
                pos = dataLeft.size();
            }
            auto data = state->left_tuples->ExtractValues(index, threadNumber);
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

        try {
            hash_table->Probe(hashes, dataLeft, result);
        } catch (ConversionException &e) {
            std::cout << "An exception in HT probe" << std::endl;
        }
    }
}

void PhysicalRadixJoin::FillPartitions(PhysicalRadixJoinOperatorState *state, std::atomic<index_t> &index) {
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

void PhysicalRadixJoin::RadixJoinSingleThreadedLeft(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run, index_t concurrent) {
    // move the perviously built histograms to the old histogram pointer to store them for this run
    state->old_left_histogram = std::move(state->left_histogram);
    if (run < runs - 1) {
        size_t amountOfPartitionsInNextRun = (1 << numberOfBits[run + 1]);
        state->left_histogram = make_unique<Histogram>(1 << (shift + numberOfBits[run]), amountOfPartitionsInNextRun);
    }
    // Calculate the range of the left and right histograms
    state->old_left_histogram->Range();
    // Iterate over all the partitions in the old histogram (as this contains the actual information)
    atomic<index_t> partitionGlobal;
    partitionGlobal = 0;
    std::vector<std::thread> threads;
    for(index_t i = 0; i< concurrent; i++) {
        threads.push_back(std::thread(&PhysicalRadixJoin::PartitonLeftThreaded, this, state, shift, run, i, std::ref(partitionGlobal)));
    }
    for(auto &t : threads) {
        t.join();
    }
}

void PhysicalRadixJoin::PartitonLeftThreaded(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run, index_t threadNumber, atomic<index_t> &partitionGlobal) {
    // Get the bitmask for this partition
    size_t bitmask = ((1 << numberOfBits[run]) - 1) << shift;
    // Get the bitmask for the next run
    size_t bitMaskNextRun = ((1 << numberOfBits[run + 1]) - 1) << (shift + numberOfBits[run]);
    while(1) {
        auto partitionLocal = partitionGlobal++;
        if(partitionLocal >= state->old_left_histogram->numberOfPartitions) {
            break;
        }
        auto back = state->old_left_histogram->getRangeOfSuperpartition(partitionLocal);
        // Iterate over the data given to this thread
        for (index_t toOrder = back.first; toOrder < back.second; toOrder++) {
            auto entry = state->left_tuples->GetEntry(toOrder, threadNumber);
            auto partition = ((std::get<0>(entry) & bitmask) >> shift);
            assert(partition >= 0 && partition < state->old_left_histogram->numberOfPartitions *
                                                 state->old_left_histogram->numberOfBucketsPerPartition);
            if (run < runs - 1) {
                // Get the partition in the next run
                auto partitionNextRun = (std::get<0>(entry) & bitMaskNextRun) >> (shift + numberOfBits[run]);
                // Increment the counter for the partitionNextRun in partition
                state->left_histogram->IncrementBucketCounter(
                        partitionLocal * state->old_left_histogram->numberOfBucketsPerPartition + partition,
                        partitionNextRun);
            }
            // Get the position where to insert from the old left histogram
            index_t position = state->old_left_histogram->getInsertPlace(partitionLocal, partition);
            state->left_tuplesSwap->SetEntry(entry, position);
        }
    }
}

void PhysicalRadixJoin::RadixJoinSingleThreadedRight(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run, index_t concurrent) {
    // move the perviously built histograms to the old histogram pointer to store them for this run
    state->old_right_histogram = std::move(state->right_histogram);
    if (run < runs - 1) {
        size_t amountOfPartitionsInNextRun = (1 << numberOfBits[run + 1]);
        state->right_histogram = make_unique<Histogram>(1 << (shift + numberOfBits[run]), amountOfPartitionsInNextRun);
    }
    // Calculate the range of the left and right histograms
    state->old_right_histogram->Range();
    // Iterate over all the partitions in the old histogram (as this contains the actual information)
    atomic<index_t> partitionGlobal;
    partitionGlobal = 0;
    std::vector<std::thread> threads;
    for(index_t i = 0; i< concurrent; i++) {
        threads.push_back(std::thread(&PhysicalRadixJoin::PartitonRightThreaded, this, state, shift, run, i, std::ref(partitionGlobal)));
    }
    for(auto &t : threads) {
        t.join();
    }
}

void PhysicalRadixJoin::PartitonRightThreaded(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run, index_t threadNumber, atomic<index_t> &partitionGlobal) {
    // Get the bitmask for this partition
    size_t bitmask = ((1 << numberOfBits[run]) - 1) << shift;
    // Get the bitmask for the next run
    size_t bitMaskNextRun = ((1 << numberOfBits[run + 1]) - 1) << (shift + numberOfBits[run]);
    while(1) {
        auto partitionLocal = partitionGlobal++;
        if(partitionLocal >= state->old_right_histogram->numberOfPartitions) {
            break;
        }
        auto back = state->old_right_histogram->getRangeOfSuperpartition(partitionLocal);
        // Iterate over the data given to this thread
        for (index_t toOrder = back.first; toOrder < back.second; toOrder++) {
            auto entry = state->right_tuples->GetEntry(toOrder, threadNumber);
            auto partition = ((std::get<0>(entry) & bitmask) >> shift);
            assert(partition >= 0 && partition < state->old_right_histogram->numberOfPartitions *
                                                 state->old_right_histogram->numberOfBucketsPerPartition);
            if (run < runs - 1) {
                // Get the partition in the next run
                auto partitionNextRun = (std::get<0>(entry) & bitMaskNextRun) >> (shift + numberOfBits[run]);
                // Increment the counter for the partitionNextRun in partition
                state->right_histogram->IncrementBucketCounter(
                        partitionLocal * state->old_right_histogram->numberOfBucketsPerPartition + partition,
                        partitionNextRun);
            }
            // Get the position where to insert from the old right histogram
            index_t position = state->old_right_histogram->getInsertPlace(partitionLocal, partition);
            state->right_tuplesSwap->SetEntry(entry, position);
        }
    }
}

unique_ptr<PhysicalOperatorState> PhysicalRadixJoin::GetOperatorState() {
    return make_unique<PhysicalRadixJoinOperatorState>(children[0].get(), children[1].get());
}