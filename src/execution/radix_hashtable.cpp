#include <iostream>
#include "duckdb/execution/radix_hashtable.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/static_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;


static void SerializeChunk(DataChunk &source, data_ptr_t targets[]) {
    Vector target_vector(TypeId::POINTER, (data_ptr_t) targets);
    target_vector.count = source.size();
    target_vector.sel_vector = source.sel_vector;

    index_t offset = 0;
    for (index_t i = 0; i < source.column_count; i++) {
        VectorOperations::Scatter::SetAll(source.data[i], target_vector, true, offset);
        offset += GetTypeIdSize(source.data[i].type);
    }
}

static void DeserializeChunk(DataChunk &result, data_ptr_t source[], index_t count) {
    Vector source_vector(TypeId::POINTER, (data_ptr_t) source);
    source_vector.count = count;

    index_t offset = 0;
    for (index_t i = 0; i < result.column_count; i++) {
        VectorOperations::Gather::Set(source_vector, result.data[i], false, offset);
        offset += GetTypeIdSize(result.data[i].type);
    }
}

RadixHashTable::RadixHashTable(vector<JoinCondition> &conditions, vector<TypeId> build_types, JoinType type,
                               index_t initial_capacity, bool parallel) : build_types(build_types), equality_size(0),
                                                                          condition_size(0), build_size(0),
                                                                          entry_size(0), tuple_size(0), join_type(type),
                                                                          has_null(false), capacity(0), count(0),
                                                                          parallel(parallel) {
    bitmask = initial_capacity - 1;
    for (auto &condition : conditions) {
        assert(condition.left->return_type == condition.right->return_type);
        auto type = condition.left->return_type;
        auto type_size = GetTypeIdSize(type);
        if (condition.comparison == ExpressionType::COMPARE_EQUAL) {
            // all equality conditions should be at the front
            // all other conditions at the back
            // this assert checks that
            assert(equality_types.size() == condition_types.size());
            equality_types.push_back(type);
            equality_size += type_size;
        }
        predicates.push_back(condition.comparison);
        null_values_are_equal.push_back(condition.null_values_are_equal);
        assert(!condition.null_values_are_equal ||
               (condition.null_values_are_equal && condition.comparison == ExpressionType::COMPARE_EQUAL));

        condition_types.push_back(type);
        condition_size += type_size;
    }
    // at least one equality is necessary
    assert(equality_types.size() > 0);

    if (type == JoinType::ANTI || type == JoinType::SEMI || type == JoinType::MARK) {
        // for ANTI, SEMI and MARK join, we only need to store the keys
        build_size = 0;
    } else {
        // otherwise we need to store the entire build side for reconstruction
        // purposes
        for (index_t i = 0; i < build_types.size(); i++) {
            build_size += GetTypeIdSize(build_types[i]);
        }
    }
    tuple_size = condition_size + build_size;
    entry_size = tuple_size + sizeof(void *);
    //std::cout << "The initial cap is " << initial_capacity << " c" << condition_size << " " << build_size << std::endl;
    //Resize(initial_capacity);
    //std::cout << tuple_size << std::endl;

    // Malloc the corresponding size of the tuples with the initial capacity
    data = static_cast<uint8_t *>(calloc(initial_capacity, tuple_size + 1));
    dataStorage.resize(build_types.size());
    // Memory chunk to store all the conditions to compare
    //probeMem = static_cast<uint8_t *>(calloc(1, condition_size));
}

void RadixHashTable::ApplyBitmask(Vector &hashes) {
    auto indices = (index_t *) hashes.data;
    VectorOperations::Exec(hashes, [&](index_t i, index_t k) { indices[i] = indices[i] & bitmask; });
}

void RadixHashTable::InsertHashes(Vector &hashes, data_ptr_t key_locations[]) {
    assert(hashes.type == TypeId::HASH);

    // use bitmask to get position in array
    ApplyBitmask(hashes);

    auto pointers = hashed_pointers.get();
    auto indices = (index_t *) hashes.data;
    // now fill in the entries
    VectorOperations::Exec(hashes, [&](index_t i, index_t k) {
        auto index = indices[i];
        // set prev in current key to the value (NOTE: this will be nullptr if
        // there is none)
        auto prev_pointer = (data_ptr_t *) (key_locations[i] + tuple_size);
        *prev_pointer = pointers[index];

        // set pointer to current tuple
        pointers[index] = key_locations[i];
    });
}

void RadixHashTable::Resize(index_t size) {
    //std::cout << "Calling resize " << size << std::endl;
    if (size <= capacity) {
        throw Exception("Cannot downsize a hash table!");
    }
    capacity = size;

    // size needs to be a power of 2
    assert((size & (size - 1)) == 0);
    bitmask = size - 1;

    hashed_pointers = unique_ptr<data_ptr_t[]>(new data_ptr_t[capacity]);
    memset(hashed_pointers.get(), 0, capacity * sizeof(data_ptr_t));

    if (count > 0) {
        // we have entries, need to rehash the pointers
        // first reset all chain pointers to the nullptr
        // we could do this by actually following the chains in the
        // hashed_pointers as well might be more or less efficient depending on
        // length of chains?
        auto node = head.get();
        while (node) {
            // scan all the entries in this node
            auto entry_pointer = (data_ptr_t) node->data.get() + tuple_size;
            for (index_t i = 0; i < node->count; i++) {
                // reset chain pointer
                auto prev_pointer = (data_ptr_t *) entry_pointer;
                *prev_pointer = nullptr;
                // move to next entry
                entry_pointer += entry_size;
            }
            node = node->prev.get();
        }

        // now rehash the entries
        DataChunk keys;
        keys.Initialize(equality_types);

        data_ptr_t key_locations[STANDARD_VECTOR_SIZE];

        node = head.get();
        while (node) {
            // scan all the entries in this node
            auto dataptr = node->data.get();
            for (index_t i = 0; i < node->count; i++) {
                // key is stored at the start
                key_locations[i] = dataptr;
                // move to next entry
                dataptr += entry_size;
            }

            // reconstruct the keys chunk from the stored entries
            // we only reconstruct the keys that are part of the equality
            // comparison as these are the ones that are used to compute the
            // hash
            DeserializeChunk(keys, key_locations, node->count);

            // create the hash
            StaticVector<uint64_t> hashes;
            keys.Hash(hashes);

            // re-insert the entries
            InsertHashes(hashes, key_locations);

            // move to the next node
            node = node->prev.get();
        }
    }
}

void RadixHashTable::Hash(DataChunk &keys, Vector &hashes) {
    VectorOperations::Hash(keys.data[0], hashes);
    for (index_t i = 1; i < equality_types.size(); i++) {
        VectorOperations::CombineHash(hashes, keys.data[i]);
    }
}

static index_t CreateNotNullSelVector(DataChunk &keys, sel_t *not_null_sel_vector) {
    sel_t *sel_vector = keys.data[0].sel_vector;
    index_t result_count = keys.size();
    // first we loop over all the columns and figure out where the
    for (index_t i = 0; i < keys.column_count; i++) {
        keys.data[i].sel_vector = sel_vector;
        keys.data[i].count = result_count;
        result_count = Vector::NotNullSelVector(keys.data[i], not_null_sel_vector, sel_vector, nullptr);
    }
    // now assign the final count and selection vectors
    for (index_t i = 0; i < keys.column_count; i++) {
        keys.data[i].sel_vector = sel_vector;
        keys.data[i].count = result_count;
    }
    keys.sel_vector = sel_vector;
    return result_count;
}

void RadixHashTable::Build(DataChunk &keys, DataChunk &payload) {
    assert(keys.size() == payload.size());
    if (keys.size() == 0) {
        return;
    }
    for (index_t i = 0; i < keys.size(); i++) {
        auto bucket = payload.GetVector(payload.column_count - 1).GetValue(i).value_.hash & bitmask;
        // Iterate over all the buckets until there is one free
        while (*(data + bucket * (tuple_size + 1))) {
            bucket++;
            bucket &= bitmask;
        }
        auto writer = data + bucket * (tuple_size + 1);
        // Signal for a taken bucket
        writer[0] = 0xff;
        writer++;
        for (index_t keyI = 0; keyI < keys.column_count; keyI++) {
            auto value = keys.GetVector(keyI).GetValue(i);
            auto &type = keys.GetTypes()[keyI];
            auto size = GetTypeIdSize(type);
            switch (type) {
                case TypeId::SMALLINT: {
                    memcpy(writer, &value.value_.smallint, size);
                    writer += size;
                    break;
                }
                case TypeId::INTEGER: {
                    memcpy(writer, &value.value_.integer, size);
                    writer += size;
                    break;
                }
                case TypeId::BIGINT: {
                    memcpy(writer, &value.value_.bigint, size);
                    writer += size;
                    break;
                }
                default:
                    throw "Not implemented in RadixJoinHashTable";
            }
        }

        for (index_t payloadI = 0; payloadI < payload.column_count; payloadI++) {
            auto value = payload.GetVector(payloadI).GetValue(i);
            auto &type = payload.GetTypes()[payloadI];
            auto size = GetTypeIdSize(type);
            switch (type) {
                case TypeId::SMALLINT: {
                    memcpy(writer, &value.value_.smallint, size);
                    writer += size;
                    break;
                }
                case TypeId::INTEGER: {
                    memcpy(writer, &value.value_.integer, size);
                    writer += size;
                    break;
                }
                case TypeId::BIGINT: {
                    memcpy(writer, &value.value_.bigint, size);
                    writer += size;
                    break;
                }
                default:
                    throw "Not implemented in RadixJoinHashTable";
            }
        }
    }
}

void RadixHashTable::Probe(DataChunk &keys, DataChunk &payload, ChunkCollection &result) {
    DataChunk temp;
    temp.Initialize(result.types);
    for (index_t i = 0; i < keys.size(); i++) {
        // Start bucket
        auto bucket = payload.GetVector(payload.column_count - 1).GetValue(i).value_.hash & bitmask;
        // Iterate as long as there is no empty bucket
        while (*(data + bucket * (tuple_size + 1))) {
            bool checkKey = true;
            uint8_t *reader = data + bucket * (tuple_size + 1);
            // Skip the 0xff
            reader++;
            for (index_t keyI = 0; keyI < keys.column_count; keyI++) {
                auto value = keys.GetVector(keyI).GetValue(i);
                auto &type = keys.GetTypes()[keyI];
                auto size = GetTypeIdSize(type);
                switch (type) {
                    case TypeId::SMALLINT: {
                        if(memcmp(reader, &value.value_.smallint, size) != 0) {
                            checkKey = false;
                        }
                        reader += size;
                        break;
                    }
                    case TypeId::INTEGER: {
                        if(memcmp(reader, &value.value_.integer, size) != 0) {
                            checkKey = false;
                        }
                        reader += size;
                        break;
                    }
                    case TypeId::BIGINT: {
                        if(memcmp(reader, &value.value_.bigint, size) != 0) {
                            checkKey = false;
                        }
                        reader += size;
                        break;
                    }
                    default:
                        throw "Not yet implemented here";
                }
            }
            // Match found
            // reader is now at the position with the data
            if(checkKey) {
                index_t pos = temp.size();
                for (index_t payloadI = 0; payloadI < payload.column_count; payloadI++) {
                    temp.GetVector(payloadI).count++;
                    temp.GetVector(payloadI).SetValue(pos, payload.GetVector(payloadI).GetValue(i));
                }
                index_t offset = payload.column_count;
                for (index_t storedI = 0; storedI < build_types.size(); storedI++) {
                    dataStorage[storedI].is_null = false;
                    auto &type = build_types[storedI];
                    auto size = GetTypeIdSize(type);
                    switch (type) {
                        case TypeId::SMALLINT: {
                            memcpy(&dataStorage[storedI].value_.smallint, reader, size);
                            reader += size;
                            break;
                        }
                        case TypeId::INTEGER: {
                            memcpy(&dataStorage[storedI].value_.integer, reader, size);
                            reader += size;
                            break;
                        }
                        case TypeId::BIGINT: {
                            memcpy(&dataStorage[storedI].value_.bigint, reader, size);
                            reader += size;
                            break;
                        }
                        default:
                            throw "Not yet implememnted here";
                    }
                }
                for(index_t storedI = 0; storedI< build_types.size(); storedI++) {
                    temp.GetVector(offset + storedI).count++;
                    temp.GetVector(offset + storedI).SetValue(pos, dataStorage[storedI]);
                }
                if (temp.size() == STANDARD_VECTOR_SIZE) {
                    result.Append(temp);
                    temp.Reset();
                }
            }
            bucket++;
            bucket = bucket & (bitmask);
        }
        result.Append(temp);
        temp.Reset();
    }
}

RadixHashTable::~RadixHashTable() {
    free(data);
}

namespace duckdb {
void ConstructMarkJoinResultRadix(DataChunk &join_keys, DataChunk &child, DataChunk &result, bool found_match[],
                                  bool right_has_null) {
    // for the initial set of columns we just reference the left side
    for (index_t i = 0; i < child.column_count; i++) {
        result.data[i].Reference(child.data[i]);
    }
    // create the result matching vector
    auto &result_vector = result.data[child.column_count];
    result_vector.count = child.size();
    // first we set the NULL values from the join keys
    // if there is any NULL in the keys, the result is NULL
    if (join_keys.column_count > 0) {
        result_vector.nullmask = join_keys.data[0].nullmask;
        for (index_t i = 1; i < join_keys.column_count; i++) {
            result_vector.nullmask |= join_keys.data[i].nullmask;
        }
    }
    // now set the remaining entries to either true or false based on whether a match was found
    auto bool_result = (bool *) result_vector.data;
    for (index_t i = 0; i < result_vector.count; i++) {
        bool_result[i] = found_match[i];
    }
    // if the right side contains NULL values, the result of any FALSE becomes NULL
    if (right_has_null) {
        for (index_t i = 0; i < result_vector.count; i++) {
            if (!bool_result[i]) {
                result_vector.nullmask[i] = true;
            }
        }
    }
}
} // namespace duckdb