#include "exec/stream/aggregate/agg_state_data.h"

#include "exprs/agg/stream/detail_retractable.h"

namespace starrocks::stream {

Status AggStateData::allocate_intermediate_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                                 const std::vector<ChunkPtrOr>& result_chunks,
                                                 const Buffer<AggGroupStatePtr>& agg_group_state) const {
    int32_t j = 0;
    auto agg_func_idx = agg_func_id();
    auto table_idx = intermediate_table_idx();
    for (auto i = 0; i < chunk_size; i++) {
        // skip if keys are already existed in map(cache)
        if (keys_not_in_map[i]) {
            DCHECK_LT(j, chunk_size);
            // replacement new
            _agg_function->create(_agg_fn_ctx, agg_group_state[i] + _agg_state_offset);
            auto& result_chunk_or = result_chunks[j++];
            if (result_chunk_or.ok()) {
                // deserialize result row and allocate it to agg_state
                auto result_chunk = result_chunk_or.value();
                DCHECK_LT(table_idx, result_chunk->num_columns());
                // id or index?
                auto column = result_chunk->get_column_by_index(table_idx);
                DCHECK_EQ(1, column->size());
                _agg_function->merge(_agg_fn_ctx, column.get(), agg_group_state[i] + _agg_state_offset, 0);
            } else {
                // TODO?
                auto status = result_chunk_or.status();
                DCHECK(status.is_end_of_file());
                if (!status.is_end_of_file()) {
                    return status;
                }
            }
        }
    }
    DCHECK(j == result_chunks.size());
    return Status::OK();
}

Status AggStateData::allocate_retract_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                            const std::vector<ChunkIteratorPtrOr>& result_chunks,
                                            const Buffer<AggGroupStatePtr>& agg_group_state) const {
    int32_t j = 0;
    auto table_idx = retract_table_idx();
    for (auto i = 0; i < chunk_size; i++) {
        // skip if keys are already existed in map(cache)
        if (keys_not_in_map[i]) {
            DCHECK_LT(j, chunk_size);
            auto& result_chunk_or = result_chunks[j++];
            if (result_chunk_or.ok()) {
                // TODO: There are some reasons that all incremental keys' detail
                //  states should be restored at first:
                // - all keys' states should be refreshed and flushed in the end (maybe we can aggregate mode to
                //     implement incremental count compute in storage layer later)
                // - state tables' incremental results are not seen by operators, so here operators manage the
                //     incremental states itself.
                // BE ATTENTION:
                // this will cause a memory pressure when the incremental states are big!
                auto result_chunk_iter = result_chunk_or.value();
                auto& chunk_iter = result_chunk_or.value();
                do {
                    auto t_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1024);
                    auto status = chunk_iter->get_next(t_chunk.get());
                    if (status.is_end_of_file()) {
                        break;
                    }
                    if (!status.ok()) {
                        return status;
                    }

                    DCHECK_LT(table_idx, t_chunk->num_columns());
                    const auto& columns = t_chunk->columns();
                    _agg_function->restore_detail(_agg_fn_ctx, t_chunk->num_rows(), columns,
                                                  agg_group_state[i] + _agg_state_offset);
                } while (true);
                chunk_iter->close();

            } else {
                // TODO?
                auto status = result_chunk_or.status();
                DCHECK(status.is_end_of_file());
                if (!status.is_end_of_file()) {
                    return status;
                }
            }
        }
    }
    DCHECK(j == result_chunks.size());
    return Status::OK();
}

Status AggStateData::process_chunk(size_t chunk_size, const std::vector<ChunkPtrOr>& result_chunks,
                                   const Buffer<uint8_t>& keys_not_in_map, const StreamRowOp* ops,
                                   std::vector<std::vector<const vectorized::Column*>>& raw_columns,
                                   const Buffer<vectorized::AggDataPtr>& agg_group_state) const {
    auto* columns = (raw_columns[agg_func_id()]).data();
    for (int i = 0; i < chunk_size; i++) {
        auto& op = ops[i];
        switch (op) {
        case StreamRowOp::INSERT:
        case StreamRowOp::UPDATE_BEFORE:
            _agg_function->update(_agg_fn_ctx, columns, agg_group_state[i] + _agg_state_offset, i);
            break;
        case StreamRowOp::DELETE:
        case StreamRowOp::UPDATE_AFTER:
            _agg_function->retract(_agg_fn_ctx, columns, agg_group_state[i] + _agg_state_offset, i);
            break;
        }
    }
    return Status::OK();
}

Status AggStateData::output_result(size_t chunk_size, const Buffer<vectorized::AggDataPtr>& agg_group_data,
                                   vectorized::Column* to) const {
    if (_agg_state_kind == AggStateTableKind::Detail_Intermediate ||
        _agg_state_kind == AggStateTableKind::Detail_Result) {
        // if need sync, query data from detail table.
        for (size_t i = 0; i < chunk_size; i++) {
            _agg_function->sync_detail(_agg_fn_ctx, agg_group_data[i] + _agg_state_offset);
        }
    }

    _agg_function->batch_finalize(_agg_fn_ctx, chunk_size, agg_group_data, _agg_state_offset, to);
    return Status::OK();
}

Status AggStateData::output_detail(size_t chunk_size, const Buffer<vectorized::AggDataPtr>& agg_group_data,
                                   const vectorized::Columns& to, vectorized::Column* count) const {
    for (size_t i = 0; i < chunk_size; i++) {
        _agg_function->output_detail(_agg_fn_ctx, agg_group_data[i] + _agg_state_offset, to, count);
    }
    return Status::OK();
}

Status IntermediateAggGroupState::process_chunk(size_t chunk_size, const Buffer<DatumRow>& non_found_keys,
                                                const Buffer<uint8_t>& keys_not_in_map, const StreamRowOp* ops,
                                                std::vector<std::vector<const vectorized::Column*>>& raw_columns,
                                                const Buffer<vectorized::AggDataPtr>& agg_group_state) const {
    DCHECK(!_agg_states.empty());
    DCHECK(_state_table);
    auto result_chunks = _state_table->get_chunks(non_found_keys);
    for (auto& agg_state : _agg_states) {
        // Allocate state by using intermediate states.
        RETURN_IF_ERROR(
                agg_state->allocate_intermediate_state(chunk_size, keys_not_in_map, result_chunks, agg_group_state));
        // Allocate state by using intermediate states.
        RETURN_IF_ERROR(agg_state->process_chunk(chunk_size, result_chunks, keys_not_in_map, ops, raw_columns,
                                                 agg_group_state));
    }
    return Status::OK();
}

// TODO: Support const raw_columns
Status DetailAggGroupState::process_chunk(size_t chunk_size, const Buffer<DatumRow>& non_found_keys,
                                          const Buffer<uint8_t>& keys_not_in_map, const StreamRowOp* ops,
                                          std::vector<std::vector<const vectorized::Column*>>& raw_columns,
                                          const Buffer<vectorized::AggDataPtr>& agg_group_state) const {
    DCHECK(!_agg_states.empty());
    // TODO: How to reuse detail_result_chunks later?
    std::vector<ChunkPtrOr> intermediate_result_chunks;
    std::vector<ChunkPtrOr> result_chunks;
    for (size_t i = 0; i < _agg_states.size(); i++) {
        auto& agg_state = _agg_states[i];
        auto& detail_state_table = _detail_state_tables[i];

        // Allocate state by using intermediate states.
        if (agg_state->state_table_kind() == AggStateTableKind::Detail_Result) {
            // Restore agg intermediate states.
            if (result_chunks.empty()) {
                result_chunks = _result_state_table->get_chunks(non_found_keys);
            }
            RETURN_IF_ERROR(agg_state->allocate_intermediate_state(chunk_size, keys_not_in_map, result_chunks,
                                                                   agg_group_state));

            // Restore retract state from detail table: find by keys which are the prefix of state tables' primary keys.
            auto detail_result_chunks = detail_state_table->get_chunk_iters(non_found_keys);
            RETURN_IF_ERROR(agg_state->allocate_retract_state(chunk_size, keys_not_in_map, detail_result_chunks,
                                                              agg_group_state));

            // Process input chunks.
            RETURN_IF_ERROR(agg_state->process_chunk(chunk_size, result_chunks, keys_not_in_map, ops, raw_columns,
                                                     agg_group_state));
        } else {
            DCHECK(agg_state->state_table_kind() == AggStateTableKind::Detail_Intermediate);

            // Restore agg intermediate states.
            DCHECK(_intermediate_state_table);
            if (intermediate_result_chunks.empty()) {
                intermediate_result_chunks = _intermediate_state_table->get_chunks(non_found_keys);
            }
            RETURN_IF_ERROR(agg_state->allocate_intermediate_state(chunk_size, keys_not_in_map,
                                                                   intermediate_result_chunks, agg_group_state));

            // Restore retract state from detail table: find by keys which are the prefix of state tables' primary keys.
            auto detail_result_chunks = detail_state_table->get_chunk_iters(non_found_keys);
            RETURN_IF_ERROR(agg_state->allocate_retract_state(chunk_size, keys_not_in_map, detail_result_chunks,
                                                              agg_group_state));

            // Process input chunks.
            RETURN_IF_ERROR(agg_state->process_chunk(chunk_size, intermediate_result_chunks, keys_not_in_map, ops,
                                                     raw_columns, agg_group_state));
        }
    }
    return Status::OK();
}

//Status DetailAggGroupState::flush(size_t chunk_size,
//                                  const Buffer<vectorized::AggDataPtr>& agg_group_state,
//                                  vectorized::Columns* to) const {
//    for (auto& agg_state : _agg_states) {
//        agg_state->output_detail(chunk_size, agg_group_state, to);
//    }
//    return Status::OK();
//}

} // namespace starrocks::stream