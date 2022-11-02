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
    RETURN_IF_ERROR(_build_hash_map(chunk_size));
    for (int i = 0; i < chunk_size; i++) {
        // switch input' ops
        _accumulate_row(i);
    }
    return Status::OK();
}

// If input row is INSERT/UPDATE_AFTER, need accumulate the input.
void StreamingAggregator::_accumulate_row(int32_t row_idx) {
    DCHECK_LT(row_idx, _tmp_agg_states.size());
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->update(_agg_fn_ctxs[i], _agg_input_raw_columns[i].data(),
                                  _tmp_agg_states[row_idx] + _agg_states_offsets[i], i);
    }
}

// If input row is DELETE/UPDATE_BEFORE, need accumulate the input.
void StreamingAggregator::_retract_row(int32_t row_idx) {
    // NOT IMPLEMENT.
    DCHECK_LT(row_idx, _tmp_agg_states.size());
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->retract(_agg_fn_ctxs[i], _agg_input_raw_columns[i].data(),
                                   _tmp_agg_states[row_idx] + _agg_states_offsets[i], i);
    }
}

Status StreamingAggregator::_build_hash_map(size_t chunk_size, bool agg_group_by_with_limit) {
    RETURN_IF_ERROR(_agg_map->compute_agg_states(chunk_size, _group_by_columns, &_tmp_agg_states));
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
    auto& changed_keys = _agg_map->changed_keys();
    auto changed_keys_size = changed_keys.size();
    Columns group_by_columns = _create_group_by_columns(changed_keys_size);
    Columns agg_result_columns = _create_agg_result_columns(changed_keys_size);

    AggDataPtr agg_state;
    for (auto it = changed_keys.begin(); it != changed_keys.end(); ++it) {
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
