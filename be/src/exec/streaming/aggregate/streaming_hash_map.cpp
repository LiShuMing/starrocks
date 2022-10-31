// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "streaming_hash_map.h"

namespace starrocks::vectorized {
// record: changed group keys to flush
Status StreamingHashMap::compute_agg_states(size_t chunk_size,
                                            const Columns& key_columns,
                                            Buffer<AggDataPtr>* agg_states) {
    // serialize keys
    serialize_keys(chunk_size, key_columns);

    // compute agg state
    for (size_t i = 0; i < chunk_size; ++i) {
        auto key = get_serialized_key(i);

        // Make as changed group by key.
        _changed_keys.insert(key);

        auto* handle = _map.lookup(key.get_data());
        if (!handle) {
            // find key in CacheTable
            CacheKey cache_key(key.get_data());
            auto state_value = allocate_agg_state_value();
            handle = _map.insert(cache_key, state_value, _aggregate_keys_size, &delete_agg_state_value, CachePriority::NORMAL);
            if (!handle) {
                // insert failed!!!
                return Status::MemoryLimitExceeded("Streaming agg insert cache failed.");
            } else {
                (*agg_states)[i] = state_value;
            }
        } else {
            // find key in CacheState
            (*agg_states)[i] = reinterpret_cast<AggStateValue>(_map.value(handle));
        }
    }
    return Status::OK();
}

AggStateValue StreamingHashMap::get(AggStateKey key) {
    auto* handle = _map.lookup(key.get_data());
    return reinterpret_cast<AggStateValue>(_map.value(handle));
}

void StreamingHashMap::reset_by_epoch(){
    _changed_keys.clear();
}

} // namespace starrocks