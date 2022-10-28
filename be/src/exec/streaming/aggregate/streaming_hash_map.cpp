// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "streaming_hash_map.h"

namespace starrocks::vectorized {
void StreamingHashMap::compute_agg_states(size_t chunk_size,
                                          const Columns& key_columns,
                                          Buffer<AggDataPtr>* agg_states) {

    _keys_slice_sizes.assign(chunk_size, 0);
    uint32_t cur_max_one_row_size = get_max_serialize_size(key_columns);
    if (UNLIKELY(cur_max_one_row_size > _max_keys_size)) {
        _max_keys_size = cur_max_one_row_size;
        _mem_pool->clear();
        _keys_buffer = _mem_pool->allocate(_max_keys_size * chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
    }
    for (const auto& key_column : key_columns) {
        key_column->serialize_batch(_keys_buffer, _keys_slice_sizes, chunk_size, _max_keys_size);
    }
    for (size_t i = 0; i < chunk_size; ++i) {
        AggStateKey key = {_keys_buffer + i * _max_keys_size, _keys_slice_sizes[i]};
        // find key in CacheTable
        auto* handle = _map.lookup(key.get_data());
        if (!handle) {
            CacheKey cache_key(key.get_data());
            auto state_value = allocate_agg_state_value();
            handle = _map.insert(cache_key, state_value, _aggregate_keys_size, &delete_agg_state_value, CachePriority::NORMAL);
            if (!handle) {
                // insert failed!!!
            } else {
                (*agg_states)[i] = state_value;
            }

        }
        // find key in CacheState
    }
}


} // namespace starrocks