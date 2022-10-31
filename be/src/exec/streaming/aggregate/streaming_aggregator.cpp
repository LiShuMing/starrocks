// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "streaming_aggregator.h"

namespace starrocks::vectorized {

Status StreamingAggregator::process_chunk(vectorized::Chunk* chunk) {
    size_t chunk_size = chunk->num_rows();
    RETURN_IF_ERROR(_evaluate_exprs(chunk));
    RETURN_IF_ERROR(_build_hash_map(chunk_size));
    for (int i = 0; i < chunk_size; i++) {
        // switch input' ops
        _accumulate_row(i);
    }
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
}

Status StreamingAggregator::_evaluate_exprs(vectorized::Chunk* chunk) {
    // compute group by functions' columns.
    for (size_t i = 0; i < _group_by_expr_ctxs.size(); i++) {
        ASSIGN_OR_RETURN(_group_by_columns[i], _group_by_expr_ctxs[i]->evaluate(chunk));
        DCHECK(_group_by_columns[i] != nullptr);

        // TODO: support constant column in AggFunction.
        if (_group_by_columns[i]->is_constant()) {
            if (!_group_by_columns[i]->only_null()) {
                vectorized::ConstColumn* const_column =
                        static_cast<vectorized::ConstColumn*>(_group_by_columns[i].get());
                const_column->data_column()->assign(chunk->num_rows(), 0);
                _group_by_columns[i] = const_column->data_column();
            }
        }

        // Scalar function compute will return non-nullable column
        // for nullable column when the real whole chunk data all not-null.
        if (_group_by_types[i].is_nullable && !_group_by_columns[i]->is_nullable()) {
            // TODO: optimized the memory usage
            _group_by_columns[i] = vectorized::NullableColumn::create(
                    _group_by_columns[i], vectorized::NullColumn::create(_group_by_columns[i]->size(), 0));
        }
    }

    // compute agg functions' columns.
    DCHECK(_agg_expr_ctxs.size() == _agg_input_columns.size());
    DCHECK(_agg_expr_ctxs.size() == _agg_fn_ctxs.size());
    for (size_t i = 0; i < _agg_expr_ctxs.size(); i++) {
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); j++) {
            // For simplicity and don't change the overall processing flow,
            // We handle const column as normal data column
            // TODO: support constant column in AggFunction.
            if (j == 0) {
                ASSIGN_OR_RETURN(auto&& col, _agg_expr_ctxs[i][j]->evaluate(chunk));
                _agg_input_columns[i][j] =
                        vectorized::ColumnHelper::unpack_and_duplicate_const_column(chunk->num_rows(), std::move(col));
            } else {
                ASSIGN_OR_RETURN(auto&& col, _agg_expr_ctxs[i][j]->evaluate(chunk));
                _agg_input_columns[i][j] = std::move(col);
            }
            _agg_input_raw_columns[i][j] = _agg_input_columns[i][j].get();
        }
    }
    return Status::OK();
}

Status StreamingAggregator::_build_hash_map(size_t chunk_size, bool agg_group_by_with_limit) {
        RETURN_IF_ERROR(_agg_map.compute_agg_states(chunk_size, _group_by_columns, &_tmp_agg_states));
}

Status StreamingAggregator::process_epoch() {
    return Status::OK();
}

void StreamingAggregator::_build_changes(int32_t chunk_size, vectorized::ChunkPtr* chunk) {
    // TODO: support split it.
    auto& changed_keys = _agg_map.changed_keys();
    const auto changed_keys_size = changed_keys.size();
    vectorized::Columns group_by_columns = _create_group_by_columns(changed_keys_size);
    vectorized::Columns agg_result_columns = _create_agg_result_columns(changed_keys_size);

    for (auto it = changed_keys.begin(); it != changed_keys.end(); ++it) {
        // group by keys
        auto serialized_key = *it;
        // deserialized group by keys
        for (const auto& key_column: group_by_columns) {
            key_column->deserialize_and_append(reinterpret_cast<const uint8_t*>(serialized_key.get_data()));
        }

        // aggregate values
        auto agg_state = _agg_map.get(serialized_key);
        DCHECK(agg_state);
        // TODO: how to check agg's count is zero?
        // TODO: how to generate retract message?
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            _agg_functions[i]->serialize_to_column(_agg_fn_ctxs[i], agg_state + _agg_states_offsets[i], agg_result_columns[i].get());
        }
    }}

void StreamingAggregator::close(RuntimeState* state) {

}

} // namespace starrocks::vectorized
