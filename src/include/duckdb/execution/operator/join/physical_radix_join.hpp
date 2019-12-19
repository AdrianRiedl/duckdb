//
// Created by Adrian Riedl on 30.11.19.
//


#pragma once

#include <iostream>
#include "duckdb/execution/radix_hashtable.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/common/types/static_vector.hpp"

#define SINGLETHREADED 1
#define TIMER 1
#define TIMERWHOLE 1
#define PREFETCH 0

namespace duckdb {

class Histogram {
    public:
    explicit Histogram(size_t numberOfPartitions_, size_t numberOfBuckets_) : numberOfPartitions(numberOfPartitions_),
                                                                              numberOfBucketsPerPartition(
                                                                                      numberOfBuckets_),
                                                                              initialized(false) {
        histogram = static_cast<index_t **>(calloc(numberOfPartitions, sizeof(index_t *)));
        partitionEnds = static_cast<index_t **>(calloc(numberOfPartitions, sizeof(index_t *)));
        for (index_t i = 0; i < numberOfPartitions; i++) {
            histogram[i] = static_cast<index_t *>(calloc(numberOfBucketsPerPartition, sizeof(index_t)));
            partitionEnds[i] = static_cast<index_t *>(calloc(numberOfBucketsPerPartition, sizeof(index_t)));
        }
    }

    explicit Histogram() {}

    ~Histogram() {
        for (index_t i = 0; i < numberOfPartitions; i++) {
            free(histogram[i]);
            free(partitionEnds[i]);
        }
        free(histogram);
        free(partitionEnds);
    }

    index_t numberOfPartitions;
    index_t numberOfBucketsPerPartition;

    index_t **histogram;
    index_t **partitionEnds;

    bool initialized;
    //vector<std::pair<index_t, index_t>> range;


    void IncrementBucketCounter(index_t partition, index_t bucket) {
        assert(partition < numberOfPartitions && bucket < numberOfBucketsPerPartition);
        histogram[partition][bucket] += 1;
    }

    string ToString() {
        string retval = "";
        for (index_t i = 0; i < numberOfPartitions; i++) {
            retval += "Histogram for partition range " + std::to_string(i) + "\n";
            for (index_t j = 0; j < numberOfBucketsPerPartition; j++) {
                retval += "- " + std::to_string(j) + " " + std::to_string(partitionEnds[i][j]) + "  free " +
                          std::to_string(histogram[i][j]);
            }
            retval += "\n";
        }
        return retval;
    }

    // This method returns the place, where to insert the data in the bucket.
    index_t getInsertPlace(index_t partition, index_t bucket) {
        auto back = getRangeAndPosition(partition, bucket);
        histogram[partition][bucket] -= 1;
        //Print();
        return std::get<0>(back) + std::get<2>(back) - 1;
    }

    // start, end and the remaining space is given back
    std::tuple<index_t, index_t, index_t> getRangeAndPosition(index_t partition, index_t bucket) {
        assert(partition >= 0 && partition < numberOfPartitions);
        assert(bucket >= 0 && bucket < numberOfBucketsPerPartition);
        if (!initialized) {
            std::cout << "Should never happen" << std::endl;
            Range();
        }
        auto[start, end] = getParition(partition, bucket);
        return {start, end, histogram[partition][bucket]};
    }

    // This method returns the start index and the end index of the given bucket in the given partition
    std::pair<index_t, index_t> getParition(index_t partition, index_t bucket) {
        assert(partition >= 0 && partition < numberOfPartitions);
        assert(bucket >= 0 && bucket < numberOfBucketsPerPartition);
        index_t startL = 0, endL = 0;
        if (partition == 0 && bucket == 0) {
            startL = 0;
        } else {
            if (bucket == 0) {
                startL = partitionEnds[partition - 1][numberOfBucketsPerPartition - 1];
            } else {
                startL = partitionEnds[partition][bucket - 1];
            }
        }
        endL = partitionEnds[partition][bucket];
        return {startL, endL};
    }

    // This method returns the start and end index of a partition
    std::pair<index_t, index_t> getRangeOfSuperpartition(index_t partition) {
        assert(partition >= 0 && partition < numberOfPartitions);
        index_t start = 0;
        if (partition == 0) {
            start = 0;
        } else {
            auto back = getParition(partition - 1, numberOfBucketsPerPartition - 1);
            start = back.second;
        }
        index_t end = start;
        for (index_t j = 0; j < numberOfBucketsPerPartition; j++) {
            end += histogram[partition][j];
        }
        return {start, end};
    }

    void Range() {
        if (!initialized) {
            index_t start = 0;
            for (index_t i = 0; i < numberOfPartitions; i++) {
                for (index_t j = 0; j < numberOfBucketsPerPartition; j++) {
                    start += histogram[i][j];
                    partitionEnds[i][j] = start;
                }
            }
            initialized = true;
        }
    }

    void Print() {
        Printer::Print(ToString());
    }
};

class PhysicalRadixJoinOperatorState : public PhysicalOperatorState {
    public:
    PhysicalRadixJoinOperatorState(PhysicalOperator *left, PhysicalOperator *right) : PhysicalOperatorState(left),
                                                                                      initialized(false) {
        assert(left && right);
        left_data = new ChunkCollection();
        right_data = new ChunkCollection();
        left_data_partitioned = new ChunkCollection();
        right_data_partitioned = new ChunkCollection();
    }

    /// Left side
    //! Temporary storage for the actual extraction of the join keys on the left side
    DataChunk left_join_keys;
    //! Collection of all data chunks from the left side
    ChunkCollection *left_data;
    //! Collection of all data chunks from the left side to make the partitions
    ChunkCollection *left_data_partitioned;
    //! Vector of the hashes
    std::vector<unique_ptr<StaticVector<uint64_t>>> left_hashes;
    //! Histogram
    unique_ptr<Histogram> left_histogram;
    unique_ptr<Histogram> old_left_histogram = nullptr;

    /// Right side
    //! Temporary storage for the actual extraction of the join keys on the right side
    DataChunk right_join_keys;
    //! Collection of all data chunks from the right side
    ChunkCollection *right_data;
    //! Collection of all data chunks from the right side
    ChunkCollection *right_data_partitioned;
    //! Vector of the hashes
    std::vector<unique_ptr<StaticVector<uint64_t>>> right_hashes;
    //!Histogram
    unique_ptr<Histogram> right_histogram;
    unique_ptr<Histogram> old_right_histogram = nullptr;

    //! Whether or not the operator has already started
    bool initialized;
};

//! PhysicalRadixJoin represents a radix join between two tables
class PhysicalRadixJoin : public PhysicalComparisonJoin {
    public:
    PhysicalRadixJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
                      vector<JoinCondition> cond, JoinType join_type);
    ~PhysicalRadixJoin() {
        std::cerr << "Building ht took: " << timeBuild << std::endl;
        std::cerr << "Probing ht took: " << timeProbe << std::endl;
    }
    // TODO
    //! As in PhysicalHashJoin (to initialize the important stuff
    unique_ptr<RadixHashTable> dummy_hash_table;
    //! A vector of all the partitions after pre-hashes. Pair of left and corresponding right side
    //! partition at index 0 for hashtable at index 0 ...
    //! Size should be 2^bitsInFirstRound * 2^bitsInSecondRound ...
    vector<std::pair<std::pair<index_t, index_t>, std::pair<index_t, index_t>>> partitions;

    //! Vector which contains the number of bits used for the index+1 split
    vector<size_t> numberOfBits = {7, 7, 4, 5};
    //! How often to partition
    size_t runs = 2;
    //! The collection to store all the results
    vector<ChunkCollection> results;
    //! The output datachunk
    index_t output_index = 0;
    //! Counter of the internal datachunk
    index_t output_internal = 0;

    bool firstCall;
    std::chrono::nanoseconds::rep timeBuild = 0;
    std::chrono::nanoseconds::rep timeProbe = 0;

    public:
    void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

    unique_ptr<PhysicalOperatorState> GetOperatorState() override;

    private:
    //! This function is called in Partition to make the single partitions and put them to the temporaryPartitions.
    //! In the last run, the partitions are set in the global partitions vector.
    void RadixJoinSingleThreaded(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run);

    void PerformBuildAndProbe(PhysicalRadixJoinOperatorState *state,
                              vector<std::pair<std::pair<index_t, index_t>, std::pair<index_t, index_t>>> &shrinked,
                              vector<TypeId> &expected, std::atomic<index_t> &index);

    void FillPartitions(PhysicalRadixJoinOperatorState *state, std::atomic<index_t> &index);

    void RadixJoinSingleThreadedLeft(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run);

    void RadixJoinSingleThreadedRight(PhysicalRadixJoinOperatorState *state, size_t shift, size_t run);

    void RadixJoinPartitionWorkerLeft(PhysicalRadixJoinOperatorState *state, index_t startOfPartitions,
                                      index_t endOfPartitions, size_t bitmask, size_t bitmaskNextRun, size_t shift,
                                      size_t run, size_t partitionNumber);

    void RadixJoinPartitionWorkerRight(PhysicalRadixJoinOperatorState *state, index_t startOfPartitions,
                                       index_t endOfPartitions, size_t bitmask, size_t bitmaskNextRun, size_t shift,
                                       size_t run, size_t partitionNumber);

    /*void Partition(vector<StaticVector<uint64_t> *> &left_hashes, vector<StaticVector<uint64_t> *> &right_hashes,
                   Histogram &left_histogram, Histogram &right_histogram);

    void PartitionSingleChunk(std::pair<ChunkCollection, ChunkCollection> &pair,
                              vector<std::pair<ChunkCollection, ChunkCollection>> &newPartitions, size_t amountOfBits,
                              size_t shift);*/

    /*void PerformBuildAndProbe(std::pair<ChunkCollection, ChunkCollection> &pair, std::vector<TypeId> &expected,
                              size_t counter);*/

    /*void
    PartitionSingleChunkSide(ChunkCollection pair, vector<std::pair<ChunkCollection, ChunkCollection>> &newPartitions,
                             size_t amountOfBits, size_t shift, bool right);*/
};
} // namespace duckdb
