// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "local_partition_pre_aggregate.h"

namespace starrocks::pipeline {

Status LocalPartitionPreAggregate::prepare(RuntimeState* state) {
    size_t agg_size = _t_pre_agg_exprs.size();
    _agg_fn_ctxs.resize(agg_size);
    _agg_functions.resize(agg_size);
    _agg_expr_ctxs.resize(agg_size);
    _agg_input_columns.resize(agg_size);
    _agg_input_raw_columns.resize(agg_size);
    _agg_fn_types.resize(agg_size);
    _agg_states_offsets.resize(agg_size);
    for (int i = 0; i < agg_size; ++i) {
        const TExpr& desc = _t_pre_agg_exprs[i];
        const TFunction& fn = desc.nodes[0].fn;

        auto num_args = desc.nodes[0].num_children;
        _agg_input_columns[i].resize(num_args);
        _agg_input_raw_columns[i].resize(num_args);
        int node_idx = 0;
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            Expr* expr = nullptr;
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_tree_from_thrift_with_jit(state->obj_pool(), desc.nodes, nullptr, &node_idx,
                                                                   &expr, &ctx, state));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }

        TypeDescriptor return_type = TypeDescriptor::from_thrift(fn.ret_type);
        TypeDescriptor serde_type = TypeDescriptor::from_thrift(fn.aggregate_fn.intermediate_type);
        TypeDescriptor arg_type = fn.arg_types.empty() ? TypeDescriptor::from_logical_type(TYPE_UNKNOWN)
                                                       : TypeDescriptor::from_thrift(fn.arg_types[0]);

        // Collect arg_typedescs for aggregate function.
        std::vector<FunctionContext::TypeDesc> arg_typedescs;
        for (auto& type : fn.arg_types) {
            arg_typedescs.push_back(TypeDescriptor::from_thrift(type));
        }

        _agg_fn_ctxs[i] = FunctionContext::create_context(state, _mem_pool, return_type, arg_typedescs);
        state->obj_pool()->add(_agg_fn_ctxs[i]);

        bool is_input_nullable = false;
        const AggregateFunction* func = nullptr;
        if (fn.name.function_name == "count") {
            return_type.type = TYPE_BIGINT;
            arg_type.type = TYPE_BIGINT;
            serde_type.type = TYPE_BIGINT;
            is_input_nullable = !fn.arg_types.empty() && (desc.nodes[0].has_nullable_child);
            _agg_fn_types[i] = {serde_type, false, false};
        } else {
            // For nullable aggregate function(sum, max, min, avg),
            // we should always use nullable aggregate function.
            is_input_nullable = true;
            _agg_fn_types[i] = {serde_type, is_input_nullable, desc.nodes[0].is_nullable};
        }
        if (_is_window_function) {
            func = get_window_function(fn.name.function_name, arg_type.type, return_type.type, is_input_nullable,
                                       fn.binary_type, state->func_version());
        } else {
            func = get_aggregate_function(fn.name.function_name, arg_type.type, return_type.type, is_input_nullable,
                                          fn.binary_type, state->func_version());
        }
        if (func == nullptr) {
            return Status::InternalError(strings::Substitute("Invalid window function plan: ($0, $1, $2, $3, $4, $5)",
                                                             fn.name.function_name, arg_type.type, return_type.type,
                                                             is_input_nullable, fn.binary_type, state->func_version()));
        }
        _agg_functions[i] = func;
    }

    // Compute agg state total size and offsets.
    for (int i = 0; i < agg_size; ++i) {
        _agg_states_offsets[i] = _agg_states_total_size;
        _agg_states_total_size += _agg_functions[i]->size();
        _max_agg_state_align_size = std::max(_max_agg_state_align_size, _agg_functions[i]->alignof_size());

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _agg_fn_ctxs.size()) {
            size_t next_state_align_size = _agg_functions[i + 1]->alignof_size();
            // Extend total_size to next alignment requirement
            // Add padding by rounding up '_agg_states_total_size' to be a multiplier of next_state_align_size.
            _agg_states_total_size = (_agg_states_total_size + next_state_align_size - 1) / next_state_align_size *
                                     next_state_align_size;
        }
    }

    for (const auto& ctx : _agg_expr_ctxs) {
        RETURN_IF_ERROR(Expr::prepare(ctx, state));
        RETURN_IF_ERROR(Expr::open(ctx, state));
    }

    return Status::OK();
}

Status LocalPartitionPreAggregate::evaluate_agg_input_columns(Chunk* chunk) {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); j++) {
            // For simplicity and don't change the overall processing flow,
            // We handle const column as normal data column
            // TODO(kks): improve const column aggregate later
            ASSIGN_OR_RETURN(auto&& col, _agg_expr_ctxs[i][j]->evaluate(chunk));
            // if first column is const, we have to unpack it. Most agg function only has one arg, and treat it as non-const column
            if (j == 0) {
                _agg_input_columns[i][j] =
                        ColumnHelper::unpack_and_duplicate_const_column(chunk->num_rows(), std::move(col));
            } else {
                // if function has at least two argument, unpack const column selectively
                // for function like corr, FE forbid second args to be const, we will always unpack const column for it
                // for function like percentile_disc, the second args is const, do not unpack it
                if (_agg_expr_ctxs[i][j]->root()->is_constant()) {
                    _agg_input_columns[i][j] = std::move(col);
                } else {
                    _agg_input_columns[i][j] =
                            ColumnHelper::unpack_and_duplicate_const_column(chunk->num_rows(), std::move(col));
                }
            }
            _agg_input_raw_columns[i][j] = _agg_input_columns[i][j].get();
        }
    }
    return Status::OK();
}

Status LocalPartitionPreAggregate::compute_agg_state(Chunk* chunk, size_t partition_idx) {
    RETURN_IF_ERROR(evaluate_agg_input_columns(chunk));
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->update_batch_single_state(
                _agg_fn_ctxs[i], chunk->num_rows(), _agg_input_raw_columns[i].data(),
                _managed_fn_states[partition_idx]->mutable_data() + _agg_states_offsets[i]);
    }

    return Status::OK();
}

Status LocalPartitionPreAggregate::output_agg_streaming(Chunk* chunk) {
    RETURN_IF_ERROR(evaluate_agg_input_columns(chunk));
    MutableColumns agg_result_column = create_agg_result_columns(chunk->num_rows());
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        auto slot_id = _t_pre_agg_output_slot_id[i];
        _agg_functions[i]->convert_to_serialize_format(_agg_fn_ctxs[i], _agg_input_columns[i], chunk->num_rows(),
                                                       agg_result_column[i]);
        chunk->append_column(std::move(agg_result_column[i]), slot_id);
    }
    return Status::OK();
}

MutableColumns LocalPartitionPreAggregate::create_agg_result_columns(size_t num_rows) {
    MutableColumns agg_result_columns(_agg_fn_types.size());
    for (size_t i = 0; i < _agg_fn_types.size(); ++i) {
        // For count, count distinct, bitmap_union_int such as never return null function,
        // we need to create a not-nullable column.
        agg_result_columns[i] = ColumnHelper::create_column(_agg_fn_types[i].result_type, _agg_fn_types[i].is_nullable);
        agg_result_columns[i]->reserve(num_rows);
    }
    return agg_result_columns;
}

void LocalPartitionPreAggregate::output_agg_result(Chunk* chunk, const std::vector<size_t>& partition_idxes, bool eos) {
    // when eos, chunk is nullptr, just do nothing
    if (eos || chunk == nullptr || chunk->num_rows() < 1) return;

    MutableColumns agg_result_columns = create_agg_result_columns(chunk->num_rows());
    for (size_t partition_idx : partition_idxes) {
        auto agg_state = _managed_fn_states[partition_idx]->mutable_data();
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            _agg_functions[i]->serialize_to_column(_agg_fn_ctxs[i], agg_state + _agg_states_offsets[i],
                                                   agg_result_columns[i].get());
        }
    }
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        chunk->append_column(std::move(agg_result_columns[i]), _t_pre_agg_output_slot_id[i]);
    }
}

void LocalPartitionPreAggregate::output_agg_result(Chunk* chunk, size_t partition_idx, bool eos, bool is_first_chunk) {
    // when eos, chunk is nullptr, just do nothing
    if (eos || chunk == nullptr || chunk->num_rows() < 1) return;

    auto agg_state = _managed_fn_states[partition_idx]->mutable_data();

    MutableColumns agg_result_columns = create_agg_result_columns(chunk->num_rows());
    if (is_first_chunk) {
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            // add agg result into first row
            _agg_functions[i]->serialize_to_column(_agg_fn_ctxs[i], agg_state + _agg_states_offsets[i],
                                                   agg_result_columns[i].get());
            // reset agg result, after first row, we only append 'reset' result
            _agg_functions[i]->reset(_agg_fn_ctxs[i], _agg_input_columns[i], agg_state + _agg_states_offsets[i]);
        }
    }

    size_t num_default_rows = is_first_chunk ? chunk->num_rows() - 1 : chunk->num_rows();
    for (size_t i = 0; i < _agg_fn_types.size(); ++i) {
        // add 'reset' rows
        for (size_t j = 0; j < num_default_rows; j++) {
            _agg_functions[i]->serialize_to_column(_agg_fn_ctxs[i], agg_state + _agg_states_offsets[i],
                                                   agg_result_columns[i].get());
        }
    }
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        chunk->append_column(std::move(agg_result_columns[i]), _t_pre_agg_output_slot_id[i]);
    }
}

} // namespace starrocks::pipeline