//
// Created by Adrian Riedl on 30.11.19.
//


#pragma once

#include <iostream>
#include <utility>
#include "duckdb/execution/radix_hashtable.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/common/types/static_vector.hpp"

#define SINGLETHREADED 1
#define TIMERWHOLE 1
#define PREFETCH 0
#define CHUNKSIZE 1024*1024

namespace duckdb {

class Entry {
    public:
    uint64_t hash;
    // I decided to use a storage bigger than the data I use
    uint64_t data[4];
};

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


    void __attribute__((always_inline)) IncrementBucketCounter(index_t partition, index_t bucket) {
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
    index_t __attribute__((always_inline)) getInsertPlace(index_t partition, index_t bucket) {
        auto back = getRangeAndPosition(partition, bucket);
        histogram[partition][bucket] -= 1;
        //Print();
        return std::get<0>(back) + std::get<2>(back) - 1;
    }

    // start, end and the remaining space is given back
    std::tuple<index_t, index_t, index_t> __attribute__((always_inline))
    getRangeAndPosition(index_t partition, index_t bucket) {
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
    std::pair<index_t, index_t> __attribute__((always_inline)) getParition(index_t partition, index_t bucket) {
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
    std::pair<index_t, index_t> __attribute__((always_inline)) getRangeOfSuperpartition(index_t partition) {
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

    void __attribute__((always_inline)) Range() {
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

class EntryStorage {
    public:
    explicit EntryStorage(size_t chunkSize_, size_t payloadLength_, std::vector<TypeId> &types_) : chunkSize(
            chunkSize_), payloadLength(payloadLength_), types(types_), containedElements(0), tuplesOnActChunk(0),
                                                                                                   lastChunk(0) {
        assert(payloadLength_ <= 4 * sizeof(uint64_t));
        auto *dataChunk = static_cast<uint8_t *>(calloc(chunkSize, 8 + payloadLength));
        data_chunks.push_back(dataChunk);
        for (auto &t : types) {
            Value v;
            res.push_back(v);
        }
    }

    ~EntryStorage() {
        for (auto d : data_chunks) {
            free(d);
        }
    }

    // We need the hashes of the tuples and the data. The histogram is filled before.
    void insert(unique_ptr<StaticVector<uint64_t>> &hashes, DataChunk &chunk) {
        uint8_t *lastChunkPointer = data_chunks[lastChunk];
        // set the pointer to the newest writing position
        lastChunkPointer += tuplesOnActChunk * (8 + payloadLength);
        for (index_t chunkIterator = 0; chunkIterator < chunk.size(); chunkIterator++) {
            // First check, if there is still some space left to put the entry
            if (tuplesOnActChunk == chunkSize) {
                // In this case the actual chunk is full
                // Allocate a new chunk for the data
                auto *dataChunk = static_cast<uint8_t *>(calloc(chunkSize, sizeof(Entry)));
                data_chunks.push_back(dataChunk);
                tuplesOnActChunk = 0;
                lastChunk = data_chunks.size() - 1;
                lastChunkPointer = data_chunks[lastChunk];
                lastChunkPointer += tuplesOnActChunk * (8 + payloadLength);
            }
            auto hash = hashes->GetValue(chunkIterator);
            // chunk.GetVector(chunk.column_count - 1).GetValue(chunkIterator);
            // set the hashvalue
            memcpy(lastChunkPointer, &hash.value_.hash, 8);
            lastChunkPointer += 8;
            //lastChunkPointer->hash = hash.value_.hash;
            // set the values
            for (index_t col = 0; col < chunk.column_count; col++) {
                auto value = chunk.GetVector(col).GetValue(chunkIterator);
                auto type = types[col];
                auto size = GetTypeIdSize(type);
                switch (type) {
                    // todo
                    case TypeId::SMALLINT: {
                        memcpy(lastChunkPointer, &value.value_.smallint, size);
                        lastChunkPointer += size;
                        break;
                    }
                    case TypeId::INTEGER: {
                        memcpy(lastChunkPointer, &value.value_.integer, size);
                        lastChunkPointer += size;
                        break;
                    }
                    case TypeId::BIGINT: {
                        memcpy(lastChunkPointer, &value.value_.bigint, size);
                        lastChunkPointer += size;
                        break;
                    }
                    default:
                        throw "NotImplementedYet - Default";
                }
                //memcpy(lastChunkPointer, &chunk.GetVector(col).GetValue(chunkIterator).value_.bigint, )
                //lastChunkPointer->data[col] = chunk.GetVector(col).GetValue(chunkIterator).value_.bigint;
            }
            tuplesOnActChunk++;
            containedElements++;
            //lastChunkPointer++;
        }
    }

    std::pair<uint64_t, std::vector<Value> &> GetEntry(size_t pos) {
        size_t chunkNumber = pos / chunkSize;
        size_t offset = pos % chunkSize;
        uint8_t *blockStart = data_chunks[chunkNumber];
        uint64_t hash;
        blockStart += offset * (8 + payloadLength);
        memcpy(&hash, blockStart, 8);
        blockStart += 8;
        for (index_t col = 0; col < types.size(); col++) {//auto &type : types) {
            auto &type = types[col];
            auto size = GetTypeIdSize(type);
            Value &v = res[col];
            v.type = type;
            switch (type) {
                // todo
                case TypeId::SMALLINT: {
                    memcpy(&v.value_.smallint, blockStart, size);
                    v.is_null = false;
                    //res.push_back(v);
                    blockStart += size;
                    break;
                }
                case TypeId::INTEGER: {
                    memcpy(&v.value_.integer, blockStart, size);
                    v.is_null = false;
                    //res.push_back(v);
                    blockStart += size;
                    break;
                }
                case TypeId::BIGINT: {
                    memcpy(&v.value_.bigint, blockStart, size);
                    v.is_null = false;
                    //res.push_back(v);
                    blockStart += size;
                    break;
                }
                default:
                    throw "NotImplementedYet - Default";
            }
        }
        return {hash, res};
    }

    void SetEntry(std::pair<uint64_t, std::vector<Value> &> &e, size_t pos) {
        size_t chunkNumber = pos / chunkSize;
        size_t offset = pos % chunkSize;
        uint8_t *blockStart = data_chunks[chunkNumber];
        blockStart += offset * (8 + payloadLength);
        memcpy(blockStart, &std::get<0>(e), 8);
        blockStart += 8;
        for (index_t col = 0; col < types.size(); col++) {
            auto type = types[col];
            auto size = GetTypeIdSize(type);
            switch (type) {
                // todo
                case TypeId::SMALLINT: {
                    memcpy(blockStart, &std::get<1>(e)[col].value_.smallint, size);
                    blockStart += size;
                    break;
                }
                case TypeId::INTEGER: {
                    memcpy(blockStart, &std::get<1>(e)[col].value_.integer, size);
                    blockStart += size;
                    break;
                }
                case TypeId::BIGINT: {
                    memcpy(blockStart, &std::get<1>(e)[col].value_.bigint, size);
                    blockStart += size;
                    break;
                }
                default:
                    throw "NotImplementedYet - Default";
            }
        }
    }

    string ToString() {
        string s;
        s += to_string(chunkSize) + " " + to_string(payloadLength) + " " + to_string(containedElements) + "\n";
        for (size_t i = 0; i < containedElements; i++) {
            auto[hash, values] = GetEntry(i);
            s += to_string(hash) + " ";
            for (auto &v : values) {
                s += to_string(v.value_.bigint) + " ";
            }
            s += "\n";
        }
        return s;
    }

    void Print() {
        Printer::Print(ToString());
    }

    std::vector<Value> &ExtractValues(size_t pos) {
        auto entry = GetEntry(pos);
        return std::get<1>(entry);
    }

    size_t Size() {
        return containedElements;
    }

    private:
    // The amount of Entry for each chunk in this EntryStorage
    size_t chunkSize;
    // The length of the types the tuples has as attributes (until now just for check)
    size_t payloadLength;
    // The types to enable the correct reinterpretation
    std::vector<TypeId> types;
    //
    std::vector<Value> res;
    // Number of the elements stored in the EntryStorage
    size_t containedElements;
    // Index of the next free entry to get faster inserts
    size_t tuplesOnActChunk;
    // Last chunk in vector (index starting at 0)
    size_t lastChunk;

    // Collection of the datachunks where I store the Entrys to
    std::vector<uint8_t *> data_chunks;
};

class PhysicalRadixJoinOperatorState : public PhysicalOperatorState {
    public:
    PhysicalRadixJoinOperatorState(PhysicalOperator *left, PhysicalOperator *right) : PhysicalOperatorState(left),
                                                                                      initialized(false) {
        assert(left && right);
        left_data = make_unique<ChunkCollection>();
        right_data = make_unique<ChunkCollection>();
        left_data_partitioned = make_unique<ChunkCollection>();
        right_data_partitioned = make_unique<ChunkCollection>();
    }

    /// Left side
    //! Temporary storage for the actual extraction of the join keys on the left side
    DataChunk left_join_keys;
    //! Collection of all data chunks from the left side
    unique_ptr<ChunkCollection> left_data;
    //! Collection of all data chunks from the left side to make the partitions
    unique_ptr<ChunkCollection> left_data_partitioned;
    //! Histogram
    unique_ptr<Histogram> left_histogram;
    unique_ptr<Histogram> old_left_histogram = nullptr;

    /// Right side
    //! Temporary storage for the actual extraction of the join keys on the right side
    DataChunk right_join_keys;
    //! Collection of all data chunks from the right side
    unique_ptr<ChunkCollection> right_data;
    //! Collection of all data chunks from the right side
    unique_ptr<ChunkCollection> right_data_partitioned;
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
#if TIMER
        std::cerr << "Building ht took: " << timeBuild.count() << std::endl;
        std::cerr << "Probing ht took: " << timeProbe.count() << std::endl;
#endif
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
    //! The collection to store all the result
    ChunkCollection result;
    //! The output datachunk
    index_t output_index = 0;
    //! Counter of the internal datachunk
    index_t output_internal = 0;

    bool firstCall;
    std::chrono::duration<double> timeBuild;
    std::chrono::duration<double> timeProbe;
    std::chrono::duration<double> elapsed_seconds;
    std::chrono::duration<double> orderinghashBuild;
    std::chrono::duration<double> extractingValBuild;
    std::chrono::duration<double> writingDataBuild;
    std::chrono::duration<double> gettingHashtable;
    std::chrono::duration<double> gettingDChunk;
    std::chrono::duration<double> remaining;
    std::chrono::duration<double> orderinghashProbe;
    std::chrono::duration<double> extractingValProbe;
    std::chrono::duration<double> timeForAppending;
    std::chrono::duration<double> writingDataProbe;

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
