// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_STREAMING_HASH_MAP_H
#define STARROCKS_STREAMING_HASH_MAP_H


#include <algorithm>
#include <any>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <queue>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/vectorized/aggregate/agg_hash_variant.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "gen_cpp/QueryPlanExtra_constants.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/lru_cache.h"

namespace starrocks::vectorized {

using EvictedHashMap = ShardedLRUCache;

using AggStateKey = Slice;
using AggStateValue = AggDataPtr;

class StreamingHashMap {
public:
    StreamingHashMap();
    ~StreamingHashMap() = default;
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, Buffer<AggDataPtr>* agg_states);

    uint32_t get_max_serialize_size(const Columns& key_columns) {
        uint32_t max_size = 0;
        for (const auto& key_column : key_columns) {
            max_size += key_column->max_one_element_serialize_size();
        }
        return max_size;
    }
private:
    static void delete_agg_state_value(const CacheKey& key, void* value) {
        auto* state_value = (AggStateValue)value;
        delete[] state_value;
    }

    AggStateValue allocate_agg_state_value() {
        // TODO: how to avoid OOM?
        AggStateValue data = new uint8_t[_aggregate_keys_size];
        return data;
    }

private:
    // Map that can be evicted adaptively.
    EvictedHashMap _map;
    // Current epoch which is used as the watermark, the entry(old state) less than the epoch can be evicted.
    Epoch _cur_epoch;
    // Store buffers which can be reused in the incremental compute.
    std::unique_ptr<MemPool> _mem_pool;

    // Max serialized size for all group_by keys.
    uint32_t _max_keys_size = 8;
    // Buffer which is used to store group_by keys that can be reused for each chunk.
    uint8_t* _keys_buffer;
    // Group_by keys' serialized sizes for each chunk.
    Buffer<uint32_t> _keys_slice_sizes;

    // Total aggregate keys' size.
    int _aggregate_keys_size = 0;
};

} // namespace starrocks

#endif //STARROCKS_STREAMING_HASH_MAP_H
