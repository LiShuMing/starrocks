// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "streaming_hash_map.h"

namespace starrocks::streaming {

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

        auto* handle = _state_cache->lookup(key);
        if (!handle) {
            // convert chunk to DatumRow

            // serialize DatumRow to slice

            // find key in Cache
            auto state_value = allocate_agg_state_value();
            handle = _state_cache->insert(key, state_value, _agg_states_total_size,
                                          &delete_agg_state_value, CachePriority::NORMAL);
            if (!handle) {
                // insert failed!!!
                return Status::MemoryLimitExceeded("Streaming agg insert cache failed.");
            } else {
                (*agg_states)[i] = state_value;
            }
        } else {
            // find key in CacheState
            (*agg_states)[i] = reinterpret_cast<AggDataPtr>(_state_cache->value(handle));
        }
    }
    return Status::OK();
}

Status StreamingHashMap::get(const Slice& key, AggDataPtr* value) {
    auto* handle = _state_cache->lookup(key);
    DCHECK(handle);
    *value = reinterpret_cast<AggDataPtr>(_state_cache->value(handle));
    return Status::OK();
}

Status StreamingHashMap::reset_by_barrier(){
    _changed_keys.clear();
    return Status::OK();
}

} // namespace starrocks