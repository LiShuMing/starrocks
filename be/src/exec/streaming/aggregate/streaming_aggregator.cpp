// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/streaming/aggregate/streaming_aggregator.h"

namespace starrocks::streaming {

StreamingAggregator::StreamingAggregator(const TPlanNode& tnode): Aggregator(tnode) {
    _state_cache = std::make_unique<StateCache>();
    _agg_map = std::make_unique<StreamingHashMap>(_state_cache.get());
}

Status StreamingAggregator::process_chunk(vectorized::Chunk* chunk) {
    size_t chunk_size = chunk->num_rows();
    RETURN_IF_ERROR(_evaluate_exprs(chunk));

    Buffer<AggGroupStatePtr> agg_group_states;
    std::vector<int8_t> not_found;
    agg_group_states.resize(chunk_size);
    not_found.resize(chunk_size);
    std::vector<DatumRow> not_find_keys;
    RETURN_IF_ERROR(_compute_agg_states(chunk_size, _group_by_columns, &not_found, &not_find_keys, &agg_group_states));
    // assert not_find_keys' size is equal to not_found's size

    // batch fetch intermediate results from imt table
    if (_agg_states_with_result.size() > 0) {
        std::vector<DatumRowOpt> result_outputs = _result_state_table->get_rows(not_find_keys);
        for (int i = 0; i < _agg_states_with_result.size(); i++) {
            auto& agg_state_data = _agg_states_with_result[i];
            agg_state_data->allocate_state(chunk_size, &not_found, result_outputs, agg_group_states);
        }
    }
    if (_agg_states_with_intermediate.size() > 0 || _agg_states_with_detail.size() > 0) {
        std::vector<DatumRowOpt> intermediate_result_outputs = _intermediate_state_table->get_rows(not_find_keys);
        for (int i = 0; i < _agg_states_with_result.size(); i++) {
            auto& agg_state_data = _agg_states_with_result[i];
            agg_state_data->allocate_state(chunk_size, &not_found, intermediate_result_outputs, agg_group_states);
        }
        for (int i = 0; i < _agg_states_with_detail.size(); i++) {
            auto& agg_state_data = _agg_states_with_detail[i];
            agg_state_data->allocate_state(chunk_size, &not_found, intermediate_result_outputs, agg_group_states);
        }
    }
    std::vector<RowOp> ops;
    if (_agg_states_with_result.size() > 0) {
        for (int i = 0; i < _agg_states_with_result.size(); i++) {
            auto& agg_state_data = _agg_states_with_result[i];
            agg_state_data->process_chunk(chunk_size, ops, _agg_input_raw_columns[i], agg_group_states);
        }
    }
    if (_agg_states_with_intermediate.size() > 0) {
        for (int i = 0; i < _agg_states_with_intermediate.size(); i++) {
            auto& agg_state_data = _agg_states_with_intermediate[i];
            agg_state_data->process_chunk(chunk_size, ops, _agg_input_raw_columns[i], agg_group_states);
        }
    }
    for (int i = 0; i < _agg_states_with_detail.size(); i++) {
        auto& agg_state_data = _agg_states_with_detail[i];
        agg_state_data->process_chunk(chunk_size, ops, _agg_input_raw_columns[i], agg_group_states);
    }

    return Status::OK();
}

Status StreamingAggregator::_compute_agg_states(size_t chunk_size,
                                                const Columns& key_columns,
                                                std::vector<int8_t>* not_found,
                                                std::vector<DatumRow>* not_found_keys,
                                                Buffer<AggGroupStatePtr>* agg_states) {
    // serialize keys
    _keys_slice_sizes.assign(chunk_size, 0);
    not_found->assign(chunk_size, 0);
    uint32_t cur_max_one_row_size = _get_max_serialize_size(key_columns);
    if (UNLIKELY(cur_max_one_row_size > _max_keys_size)) {
        _max_keys_size = cur_max_one_row_size;
        _mem_pool->clear();
        _keys_buffer = _mem_pool->allocate(_max_keys_size * chunk_size + vectorized::SLICE_MEMEQUAL_OVERFLOW_PADDING);
    }
    for (const auto& key_column : key_columns) {
        key_column->serialize_batch(_keys_buffer, _keys_slice_sizes, chunk_size, _max_keys_size);
    }

    // compute agg state
    for (size_t i = 0; i < chunk_size; ++i) {
        Slice key = {_keys_buffer + i * _max_keys_size, _keys_slice_sizes[i]};

        // Make as changed group by key.
        _changed_keys.insert(key);

        auto* handle = _state_cache->lookup(key);
        AggGroupCacheValue* agg_group_state;
        if (!handle) {
            (*not_found)[i] = 1;
            agg_group_state = new AggGroupCacheValue(_agg_states_total_size);
            handle = _state_cache->insert(key, agg_group_state, _agg_states_total_size,
                                          &delete_agg_group_cache_value, CachePriority::NORMAL);
            if (!handle) {
                // insert failed!!!
                return Status::MemoryLimitExceeded("Streaming agg insert cache failed.");
            }

            // construct datum row keys to find in the imt table.
            DatumRow keys_row;
            keys_row.resize(key_columns.size());
            for (int j = 0; j < key_columns.size(); j++) {
                auto& column = key_columns[j];
                keys_row[i] = column->get(i);
            }
            not_found_keys->emplace_back(std::move(keys_row));
        } else {
            // find key in CacheState
            agg_group_state = reinterpret_cast<AggGroupCacheValue*>(_state_cache->value(handle));
        }
        (*agg_states)[i] = agg_group_state->agg_group_state;
    }
    return Status::OK();
}

Status StreamingAggregator::process_barrier(const StreamBarrier& barrier) {
    // update epoch
    _state_cache->evict_by_epoch(_prev_epoch);
    _state_cache->update_epoch(barrier.epoch);
    _agg_map->reset_by_barrier();
    _prev_epoch = barrier.epoch;

    return Status::OK();
}

Status StreamingAggregator::_build_changes(int32_t chunk_size, vectorized::ChunkPtr* chunk) {
    // TODO: support split it.
    auto changed_keys_size = _changed_keys.size();
    Columns group_by_columns = _create_group_by_columns(changed_keys_size);
    Columns agg_result_columns = _create_agg_result_columns(changed_keys_size);

    AggGroupStatePtr agg_state;
    for (auto it = _changed_keys.begin(); it != _changed_keys.end(); ++it) {
        // group by keys
        auto serialized_key = *it;
        // deserialized group by keys
        for (const auto& key_column: group_by_columns) {
            key_column->deserialize_and_append(reinterpret_cast<const uint8_t*>(serialized_key.get_data()));
        }

        // aggregate values
        RETURN_IF_ERROR(_agg_map->get(serialized_key, &agg_state));
        DCHECK(agg_state);
        // TODO: how to check agg's count is zero?
        // TODO: how to generate retract message?
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            _agg_functions[i]->serialize_to_column(_agg_fn_ctxs[i], agg_state + _agg_states_offsets[i],
                                                   agg_result_columns[i].get());
        }
    }
    auto result_chunk = _build_output_chunk(group_by_columns, agg_result_columns);
    *chunk = std::move(result_chunk);
    return Status::OK();
}

void StreamingAggregator::close(RuntimeState* state) {

}

} // namespace starrocks::vectorized
