// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/aggregate/stream_aggregator.h"

#include "column/column_helper.h"
#include "exec/stream/state/mem_state_table.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"

namespace starrocks::stream {

StreamAggregator::StreamAggregator(std::shared_ptr<AggregatorParams>&& params) : Aggregator(std::move(params)) {
    _count_agg_idx = _params->count_agg_idx;
}

Status StreamAggregator::_prepare_state_tables(RuntimeState* state) {
    auto key_size = _group_by_expr_ctxs.size();
    auto agg_size = _agg_fn_ctxs.size();

    DCHECK_EQ(_agg_functions.size(), agg_size);
    // TODO: state table's index should be deduced from PB.
    int32_t result_idx = 0;
    int32_t intermediate_idx = 0;
    int32_t detail_idx = 0;
    std::vector<AggStateData*> result_agg_states;
    std::vector<AggStateData*> intermediate_agg_states;
    std::vector<AggStateData*> detail_agg_states;
    for (int32_t i = 0; i < agg_size; i++) {
        auto agg_state_kind = _agg_functions[i]->agg_state_table_kind(_params->is_append_only);
        switch (agg_state_kind) {
        case AggStateTableKind::Result: {
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            agg_state_kind, _agg_states_offsets[i],
                                                            AggStateDataParams{i, result_idx, result_idx});
            result_agg_states.emplace_back(agg_state.get());
            _agg_func_states.emplace_back(std::move(agg_state));
            break;
        }
        case AggStateTableKind::Intermediate: {
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            agg_state_kind, _agg_states_offsets[i],
                                                            AggStateDataParams{i, intermediate_idx, intermediate_idx});
            intermediate_agg_states.emplace_back(agg_state.get());
            _intermediate_agg_func_ids.emplace_back(i);
            _agg_func_states.emplace_back(std::move(agg_state));
            intermediate_idx++;
            break;
        }
        case AggStateTableKind::Detail_Result: {
            // For detail agg funcs, just use intermediate agg_state when no need generate retracts.
            DCHECK(!_params->is_append_only);
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            AggStateTableKind::Detail_Result, _agg_states_offsets[i],
                                                            AggStateDataParams{i, result_idx, detail_idx++});
            detail_agg_states.emplace_back(agg_state.get());
            _agg_func_states.emplace_back(std::move(agg_state));
            break;
        }
        case AggStateTableKind::Detail_Intermediate: {
            auto agg_state = std::make_unique<AggStateData>(
                    _agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i], AggStateTableKind::Detail_Intermediate,
                    _agg_states_offsets[i], AggStateDataParams{i, intermediate_idx++, detail_idx++});
            _intermediate_agg_func_ids.emplace_back(i);
            intermediate_agg_states.emplace_back(agg_state.get());
            _agg_func_states.emplace_back(std::move(agg_state));
            break;
        }
        default:
            return Status::NotSupported("unsupported state kind");
        }
        result_idx++;
    }
    // initialize state tables
    if (_params->is_testing) {
        // result state table must be made!
        auto output_slots = _output_tuple_desc->slots();
        auto output_slots_without_op = std::vector<SlotDescriptor*>({output_slots.begin(), output_slots.end()});
        _result_state_table = std::make_unique<MemStateTable>(output_slots_without_op, key_size);

        // intermediate agg_state is created when intermediate/detail agg states are not empty.
        if (!_intermediate_agg_func_ids.empty()) {
            std::vector<SlotDescriptor*> intermediate_slots;
            for (int32_t i = 0; i < key_size; i++) {
                intermediate_slots.push_back(_intermediate_tuple_desc->slots()[i]);
            }
            for (auto& agg_func_id : _intermediate_agg_func_ids) {
                DCHECK_LT(agg_func_id + key_size, _intermediate_tuple_desc->slots().size());
                intermediate_slots.push_back(_intermediate_tuple_desc->slots()[agg_func_id + key_size]);
            }
            _intermediate_state_table = std::make_unique<MemStateTable>(intermediate_slots, key_size);
        }

        // TODO: Each agg func has one state table? How to use one table to cover all agg states?
        if (!detail_agg_states.empty()) {
            auto input_desc = state->desc_tbl().get_tuple_descriptor(0);
            auto input_slots = input_desc->slots();
            for (auto& agg_state : detail_agg_states) {
                // detail state table schema:
                // group_by_keys + agg_key -> count
                std::vector<SlotDescriptor*> detail_table_slots;
                for (auto i = 0; i < key_size; i++) {
                    detail_table_slots.push_back(input_slots[i]);
                }
                auto agg_func_idx = agg_state->agg_func_id();
                detail_table_slots.push_back(_output_tuple_desc->slots()[key_size + agg_func_idx]);
                detail_table_slots.push_back(_output_tuple_desc->slots()[key_size + _count_agg_idx]);
                DCHECK_EQ(detail_table_slots.size(), key_size + 2);
                auto detail_state_table = std::make_unique<MemStateTable>(detail_table_slots, key_size + 1);
                _detail_state_tables.emplace_back(std::move(detail_state_table));
            }
        }
    } else {
        // TODO: make imt state tables
    }
    DCHECK(_result_state_table);
    if (!result_agg_states.empty()) {
        _result_agg_group =
                std::make_unique<IntermediateAggGroupState>(std::move(result_agg_states), _result_state_table.get());
    }
    if (!intermediate_agg_states.empty()) {
        DCHECK(_intermediate_state_table);
        _intermediate_agg_group = std::make_unique<IntermediateAggGroupState>(std::move(intermediate_agg_states),
                                                                              _intermediate_state_table.get());
    }
    if (!detail_agg_states.empty()) {
        // DetailAggGroup relies on both intermediate and detail state tables.
        DCHECK(!_detail_state_tables.empty());
        _detail_agg_group =
                std::make_unique<DetailAggGroupState>(std::move(detail_agg_states), _result_state_table.get(),
                                                      _intermediate_state_table.get(), _detail_state_tables);
    }
    return Status::OK();
}

Status StreamAggregator::process_chunk(vectorized::StreamChunk* chunk) {
    size_t chunk_size = chunk->num_rows();
    RETURN_IF_ERROR(_evaluate_exprs(chunk));

    {
        SCOPED_TIMER(agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(build_hash_map_with_selection(chunk_size));
    }

    // Deduce non found keys
    auto non_found_size = SIMD::count_nonzero(_streaming_selection.data(), chunk_size);
    _non_found_keys.resize(non_found_size);
#ifdef BE_TEST
    VLOG_ROW << "[process_chunk] streaming_selection size:" << _streaming_selection.size()
             << ", non_found size:" << non_found_size << ", chunk_size:" << chunk_size;
#endif
    DCHECK_EQ(_streaming_selection.size(), chunk_size);
    for (size_t i = 0; i < _streaming_selection.size(); i++) {
        if (_streaming_selection[i]) {
            _non_found_keys[i] = _convert_to_datum_row(_group_by_columns, i);
        }
    }
    DCHECK_GE(_non_found_keys.size(), non_found_size);

    // TODO(lism): Maybe we can compact build_hash_map and build_hash_map_with_selection into one method.
    {
        SCOPED_TIMER(agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(build_hash_map(chunk_size));
    }

    // batch fetch intermediate results from imt table
    auto ops = chunk->ops();
    if (_result_agg_group) {
        _result_agg_group->process_chunk(chunk_size, _non_found_keys, _streaming_selection, ops, _agg_input_raw_columns,
                                         _tmp_agg_states);
    }
    if (_intermediate_agg_group) {
        _intermediate_agg_group->process_chunk(chunk_size, _non_found_keys, _streaming_selection, ops,
                                               _agg_input_raw_columns, _tmp_agg_states);
    }
    if (_detail_agg_group) {
        _detail_agg_group->process_chunk(chunk_size, _non_found_keys, _streaming_selection, ops, _agg_input_raw_columns,
                                         _tmp_agg_states);
    }

    return Status::OK();
}

DatumRow StreamAggregator::_convert_to_datum_row(const vectorized::Columns& columns, size_t row_idx) {
    DatumRow keys_row;
    keys_row.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); i++) {
        keys_row.emplace_back(columns[i]->get(row_idx));
    }
    return keys_row;
}

Status StreamAggregator::output_changes(int32_t chunk_size, vectorized::StreamChunkPtr* result_chunk,
                                        vectorized::ChunkPtr* intermediate_chunk,
                                        std::vector<vectorized::ChunkPtr>& detail_chunks) {
    SCOPED_TIMER(agg_compute_timer());
    Status status;
    TRY_CATCH_BAD_ALLOC(hash_map_variant().visit([&](auto& hash_map_with_key) {
        status = _output_changes(*hash_map_with_key, chunk_size, result_chunk, intermediate_chunk, detail_chunks);
    }));
    RETURN_IF_ERROR(status);

    // update intermediate table
    if (_intermediate_state_table) {
        _intermediate_state_table->flush(_state, (*intermediate_chunk).get());
    }
    // update result table
    DCHECK(_result_state_table);
    _result_state_table->flush(_state, (*result_chunk).get());
    if (_detail_agg_group) {
        DCHECK_EQ(_detail_agg_group->agg_states().size(), detail_chunks.size());
        for (size_t i = 0; i < _detail_agg_group->detail_state_tables().size(); i++) {
            DCHECK_LT(i, detail_chunks.size());
#ifdef BE_TEST
            VLOG_ROW << "detail_chunk:" << detail_chunks[i]->debug_columns();
            for (auto col : detail_chunks[i]->columns()) {
                VLOG_ROW << "detail_chunk col:" << col->debug_string();
            }
#endif
            auto& detail_state_table = _detail_agg_group->detail_state_tables()[i];
            detail_state_table->flush(_state, detail_chunks[i].get());
        }
    }

    return status;
}

Status StreamAggregator::reset_state(RuntimeState* state) {
    RETURN_IF_ERROR(_reset_state(state));
    return Status::OK();
}

template <typename HashMapWithKey>
Status StreamAggregator::_output_changes(HashMapWithKey& hash_map_with_key, int32_t chunk_size,
                                         vectorized::StreamChunkPtr* result_chunk,
                                         vectorized::ChunkPtr* intermediate_chunk,
                                         std::vector<vectorized::ChunkPtr>& detail_chunks) {
    SCOPED_TIMER(_agg_stat->get_results_timer);

    // initialize _it_hash
    if (!_it_hash.has_value()) {
        hash_map_variant().visit([&](auto& hash_map_with_key) { _it_hash = _state_allocator.begin(); });
    }

    auto it = std::any_cast<RawHashTableIterator>(_it_hash);
    auto end = _state_allocator.end();

    const auto hash_map_size = _hash_map_variant.size();
    auto num_rows = std::min<size_t>(hash_map_size - _num_rows_processed, chunk_size);
    vectorized::Columns group_by_columns = _create_group_by_columns(num_rows);
    int32_t read_index = 0;
    {
        SCOPED_TIMER(_agg_stat->iter_timer);
        hash_map_with_key.results.resize(chunk_size);
        // get key/value from hashtable
        while ((it != end) & (read_index < chunk_size)) {
            auto* value = it.value();
            hash_map_with_key.results[read_index] = *reinterpret_cast<typename HashMapWithKey::KeyType*>(value);
            // Reuse _tmp_agg_states to store state pointer address
            _tmp_agg_states[read_index] = value;
            ++read_index;
            it.next();
        }
    }

    { hash_map_with_key.insert_keys_to_columns(hash_map_with_key.results, group_by_columns, read_index); }

    {
        SCOPED_TIMER(_agg_stat->agg_append_timer);
        // only output intermediate result if intermediate agg group is not empty
        if (!_intermediate_agg_func_ids.empty()) {
            RETURN_IF_ERROR(_output_intermediate_changes(read_index, group_by_columns, intermediate_chunk));
        }
        // always output result
        RETURN_IF_ERROR(_output_result_changes(read_index, group_by_columns, result_chunk));
        // only output detail result if detail agg group is not empty
        if (!_detail_state_tables.empty()) {
            RETURN_IF_ERROR(
                    _detail_agg_group->output_changes(read_index, group_by_columns, _tmp_agg_states, detail_chunks));
        }
    }

    // NOTE: StreamAggregate do not support output NULL keys which is different from OLAP Engine.
    _is_ht_eos = (it == end);
    _it_hash = it;

    _num_rows_returned += read_index;
    _num_rows_processed += read_index;

    return Status::OK();
}

Status StreamAggregator::_output_intermediate_changes(int32_t chunk_size, const vectorized::Columns& group_by_columns,
                                                      vectorized::ChunkPtr* intermediate_chunk) {
    vectorized::Columns agg_intermediate_columns;
    _intermediate_agg_group->output_changes(chunk_size, group_by_columns, _tmp_agg_states, agg_intermediate_columns);
    *intermediate_chunk = _build_output_chunk(group_by_columns, agg_intermediate_columns, true);
    return Status::OK();
}

Status StreamAggregator::_output_result_changes(int32_t chunk_size, const vectorized::Columns& group_by_columns,
                                                vectorized::StreamChunkPtr* result_chunk) {
    if (_params->is_generate_retract) {
        vectorized::StreamChunkPtr prev_result_chunk;
        vectorized::Buffer<uint8_t> prev_existence;
        auto agg_count_column =
                vectorized::ColumnHelper::create_column(_agg_fn_types[_count_agg_idx].result_type, false);
        RETURN_IF_ERROR(_output_post_count_column(chunk_size, agg_count_column.get()));
        RETURN_IF_ERROR(_output_result_prev_retract_changes(chunk_size, group_by_columns, agg_count_column.get(),
                                                            &prev_result_chunk, &prev_existence));
        RETURN_IF_ERROR(_output_result_post_retract_changes(chunk_size, group_by_columns, agg_count_column.get(),
                                                            prev_existence, prev_result_chunk, result_chunk));
    } else {
        RETURN_IF_ERROR(_output_result_changes_without_retract(chunk_size, group_by_columns, result_chunk));
    }

    return Status::OK();
}

Status StreamAggregator::_output_post_count_column(size_t chunk_size, vectorized::Column* agg_count_column) {
    // In stream mv, use `count agg` to decide whether output the row or not:
    // when agg_count=0, skip output this row, otherwise, output this row.
    auto& count_agg_state = _agg_func_states[_count_agg_idx];
    RETURN_IF_ERROR(count_agg_state->finalize_result(chunk_size, _tmp_agg_states, agg_count_column));
#ifdef BE_TEST
    VLOG_ROW << "[output_post_count_column]:" << agg_count_column->debug_string();
#endif
    return Status::OK();
}

Status StreamAggregator::_output_result_post_retract_changes(size_t chunk_size,
                                                             const vectorized::Columns& group_by_columns,
                                                             const vectorized::Column* count_column,
                                                             const vectorized::Buffer<uint8_t>& prev_existence,
                                                             const vectorized::StreamChunkPtr& prev_result_chunk,
                                                             vectorized::StreamChunkPtr* result_chunk) {
    DCHECK(prev_result_chunk);
    vectorized::Columns post_agg_result_columns = _create_agg_result_columns(chunk_size, false);
    UInt8ColumnPtr post_ops = vectorized::UInt8Column::create();

    // In stream mv, use `count agg` to decide whether output the row or not:
    // when agg_count=0, skip output this row, otherwise, output this row.
    auto agg_count_column = reinterpret_cast<const vectorized::Int64Column*>(count_column);
    auto agg_count_column_data = agg_count_column->get_data();
    DCHECK_EQ(prev_existence.size(), chunk_size);
    vectorized::Buffer<uint8_t> filter(chunk_size, 0);
    for (size_t i = 0; i < chunk_size; i++) {
        if (agg_count_column_data[i] == 0) {
            filter[i] = 0;
        } else {
            filter[i] = 1;
        }
    }

    auto selected_count = SIMD::count_nonzero(filter.data(), chunk_size);
    if (selected_count == 0) {
        *result_chunk = prev_result_chunk;
        return Status::OK();
    }

    // TODO: Maybe we can use `batch_finalize_with_selectivity` if we can
    // delete prev result rows other than reinsert it.
    for (auto& agg_state : _agg_func_states) {
        if (agg_state->agg_func_id() == _count_agg_idx) {
            post_agg_result_columns[agg_state->agg_func_id()]->append(*count_column);
        } else {
            auto* to = post_agg_result_columns[agg_state->agg_func_id()].get();
            if (agg_state->is_detail_state_table()) {
                auto detail_state_table =
                        _detail_agg_group->detail_state_tables()[agg_state->retract_table_idx()].get();
                RETURN_IF_ERROR(agg_state->finalize_detail_result(chunk_size, group_by_columns, _tmp_agg_states,
                                                                  detail_state_table, to));
            } else {
                RETURN_IF_ERROR(agg_state->finalize_result(chunk_size, _tmp_agg_states, to));
            }
        }
    }

    // filter group by columns, clone old columns to avoid affecting original group by columns.
    vectorized::Columns post_group_by_columns;
    for (auto& key_col : group_by_columns) {
        post_group_by_columns.push_back(key_col->clone());
    }
    // filter group by keys
    for (auto& key_col : post_group_by_columns) {
        key_col->filter(filter, chunk_size);
    }
    // filter agg cols
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        post_agg_result_columns[i]->filter(filter, chunk_size);
    }
    // filter op col
    for (size_t i = 0; i < chunk_size; i++) {
        if (!filter[i]) {
            continue;
        }
        // If prev_result exists and count > 0, generate UPDATE_AFTER, otherwise generate INSERT
        if (prev_existence[i]) {
            post_ops->append(UPDATE_AFTER_OP);
        } else {
            post_ops->append(INSERT_OP);
        }
    }
#ifdef BE_TEST
    for (auto& key_col : post_group_by_columns) {
        VLOG_ROW << " output post group by :"
                 << ", debug_string:" << key_col->debug_string();
    }

    // filter agg columns
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        VLOG_ROW << " output post aggregate:  i: " << i
                 << ", debug_string:" << post_agg_result_columns[i]->debug_string();
    }
    VLOG_ROW << " output post ops:"
             << ", debug_string:" << post_ops->debug_string();
#endif

    auto post_chunk_result =
            _build_output_chunk_with_ops(post_group_by_columns, post_agg_result_columns, std::move(post_ops), false);
    DCHECK_EQ(selected_count, post_chunk_result->num_rows());

    prev_result_chunk->append_safe(*post_chunk_result);
    auto& post_op_col = *(post_chunk_result->ops_col());
    prev_result_chunk->ops_col()->append(post_op_col, 0, selected_count);
    *result_chunk = prev_result_chunk;

    return Status::OK();
}

Status StreamAggregator::_output_result_prev_retract_changes(size_t chunk_size,
                                                             const vectorized::Columns& group_by_columns,
                                                             const vectorized::Column* count_column,
                                                             vectorized::StreamChunkPtr* prev_result_chunk,
                                                             vectorized::Buffer<uint8_t>* prev_existence) {
    vectorized::Columns prev_group_by_columns = _create_group_by_columns(chunk_size);
    vectorized::Columns prev_agg_result_columns = _create_agg_result_columns(chunk_size, false);
    UInt8ColumnPtr prev_ops = vectorized::UInt8Column::create();

    std::vector<DatumRow> keys;
    keys.reserve(chunk_size);
    for (size_t i = 0; i < chunk_size; i++) {
        keys.emplace_back(_convert_to_datum_row(group_by_columns, i));
    }
    auto result_ors = _result_state_table->seek_keys(keys);
    DCHECK_EQ(result_ors.size(), chunk_size);

    // NOTE: Always retract old agg results first.
    // Should we all keep UPDATE_BEFOER/UDPATE_AFTER serially?
    vectorized::Buffer<uint32_t> permutation;
    prev_existence->assign(chunk_size, 0);

    auto agg_count_column = reinterpret_cast<const vectorized::Int64Column*>(count_column);
    auto agg_count_column_data = agg_count_column->get_data();
    for (size_t i = 0; i < chunk_size; i++) {
        auto result = result_ors[i];
        if (!result.ok()) continue;
        auto prev_result = result.value();
        DCHECK_EQ(prev_result->num_rows(), 1);
        // TODO: prev_result_table should keep ops column?
        DCHECK_LE(prev_agg_result_columns.size(), prev_result->num_columns());

        // append agg columns
        for (size_t idx = 0; idx < prev_agg_result_columns.size(); idx++) {
            auto& prev_col = prev_result->get_column_by_index(idx);
            prev_agg_result_columns[idx]->append(*prev_col);
        }
        permutation.push_back(i);
        (*prev_existence)[i] = 1;

        // generate op
        if (agg_count_column_data[i] > 0) {
            prev_ops->append(UPDATE_BEFORE_OP);
        } else {
            prev_ops->append(DELETE_OP);
        }
    }

    auto prev_count = SIMD::count_nonzero(prev_existence->data(), chunk_size);
    if (prev_count == 0) {
        // generate empty chunk
        *prev_result_chunk = _build_output_chunk_with_ops(prev_group_by_columns, prev_agg_result_columns,
                                                          std::move(prev_ops), false);
    } else {
        // append group by columns
        for (size_t idx = 0; idx < prev_group_by_columns.size(); idx++) {
            // TODO: add append_by_filter method.
            prev_group_by_columns[idx]->append_selective(*(group_by_columns[idx]), permutation.data(), 0,
                                                         permutation.size());
        }
#ifdef BE_TEST
        // append group by columns
        for (size_t idx = 0; idx < prev_group_by_columns.size(); idx++) {
            VLOG_ROW << " output prev group by column:"
                     << ", debug_string:" << prev_group_by_columns[idx]->debug_string();
        }
        for (auto& agg_col : prev_agg_result_columns) {
            VLOG_ROW << " output prev agg column:"
                     << ", debug_string:" << agg_col->debug_string();
        }

        VLOG_ROW << " output prev ops:"
                 << ", debug_string:" << prev_ops->debug_string();
#endif

        *prev_result_chunk = _build_output_chunk_with_ops(prev_group_by_columns, prev_agg_result_columns,
                                                          std::move(prev_ops), false);
    }
    return Status::OK();
}

Status StreamAggregator::_output_result_changes_without_retract(size_t chunk_size,
                                                                const vectorized::Columns& group_by_columns,
                                                                vectorized::StreamChunkPtr* result_chunk) {
    // agg result
    vectorized::Columns agg_result_columns = _create_agg_result_columns(chunk_size, false);
    for (auto& agg_state : _agg_func_states) {
        auto* to = agg_result_columns[agg_state->agg_func_id()].get();
        RETURN_IF_ERROR(agg_state->finalize_result(chunk_size, _tmp_agg_states, to));
    }

    // op col
    UInt8ColumnPtr ops = vectorized::UInt8Column::create();
    ops->append_value_multiple_times(&INSERT_OP, chunk_size);

    *result_chunk = _build_output_chunk_with_ops(group_by_columns, agg_result_columns, std::move(ops), false);

    return Status::OK();
}

vectorized::StreamChunkPtr StreamAggregator::_build_output_chunk_with_ops(const vectorized::Columns& group_by_columns,
                                                                          const vectorized::Columns& agg_result_columns,
                                                                          UInt8ColumnPtr&& ops,
                                                                          bool is_intermediate_result) {
    auto result_chunk = std::make_shared<vectorized::Chunk>();
    // For different agg phase, we should use different TupleDescriptor
    if (!is_intermediate_result) {
        for (size_t i = 0; i < group_by_columns.size(); i++) {
            result_chunk->append_column(group_by_columns[i], _output_tuple_desc->slots()[i]->id());
        }
        for (size_t i = 0; i < agg_result_columns.size(); i++) {
            size_t id = group_by_columns.size() + i;
            result_chunk->append_column(agg_result_columns[i], _output_tuple_desc->slots()[id]->id());
        }
    } else {
        for (size_t i = 0; i < group_by_columns.size(); i++) {
            result_chunk->append_column(group_by_columns[i], _intermediate_tuple_desc->slots()[i]->id());
        }
        for (size_t i = 0; i < agg_result_columns.size(); i++) {
            size_t id = group_by_columns.size() + i;
            result_chunk->append_column(agg_result_columns[i], _intermediate_tuple_desc->slots()[id]->id());
        }
    }
    //        // TODO: Add a new type of stream chunk?
    //        result_chunk->append_column(ops, (SlotId)(group_by_columns.size() + agg_result_columns.size()));
    return std::make_shared<vectorized::StreamChunk>(std::move(result_chunk), std::move(ops));
}

void StreamAggregator::close(RuntimeState* state) {
    Aggregator::close(state);
}

} // namespace starrocks::stream
