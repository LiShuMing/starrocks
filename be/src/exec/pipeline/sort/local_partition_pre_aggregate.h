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

#pragma once

#include <queue>

#include "exec/analytor.h"
#include "exec/chunks_sorter.h"
#include "exec/partition/chunks_partitioner.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class RuntimeFilterBuildDescriptor;

struct FunctionTypes;
} // namespace starrocks

namespace starrocks::pipeline {
using AggDataPtr = uint8_t*;

struct LocalPartitionPreAggregate;

using ManagedPreAggregateStates = ManagedFunctionStates<LocalPartitionPreAggregate>;
using ManagedPreAggregateStatesPtr = std::unique_ptr<ManagedPreAggregateStates>;

struct LocalPartitionPreAggregate {
    LocalPartitionPreAggregate(const std::vector<TExpr>& t_pre_agg_exprs,
                               const std::vector<TSlotId>& t_pre_agg_output_slot_id, MemPool* mem_pool,
                               bool enable_pre_agg, bool is_window_function)
            : _t_pre_agg_exprs(t_pre_agg_exprs),
              _t_pre_agg_output_slot_id(t_pre_agg_output_slot_id),
              _mem_pool(mem_pool),
              _is_window_function(is_window_function) {}

public:
    // The followings are aggregate function information:
    std::vector<FunctionContext*> _agg_fn_ctxs;
    std::vector<const AggregateFunction*> _agg_functions;
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;

    std::vector<FunctionTypes> _agg_fn_types;
    const std::vector<TExpr>& _t_pre_agg_exprs;
    const std::vector<TSlotId>& _t_pre_agg_output_slot_id;

    // every partition has one Agg State
    std::vector<ManagedPreAggregateStatesPtr> _managed_fn_states;
    // The offset of the n-th aggregate function in a row of aggregate functions.
    std::vector<size_t> _agg_states_offsets;
    std::vector<Columns> _agg_input_columns;
    //raw pointers in order to get multi-column values
    std::vector<std::vector<const Column*>> _agg_input_raw_columns;
    // The memory pool for the aggregate function state
    MemPool* _mem_pool = nullptr;
    // The total size of the row for the aggregate function state.
    size_t _agg_states_total_size = 0;
    // The max align size for all aggregate state
    size_t _max_agg_state_align_size = 1;

    bool _is_first_chunk_of_current_sorter = true;
    // Whether the pre-aggregate is a window function
    bool _is_window_function = true;

public:
    // Prepare the pre-aggregate state
    Status prepare(RuntimeState* state);

    // Evaluate the aggregate input columns
    Status evaluate_agg_input_columns(Chunk* chunk);

    // Compute the aggregate state
    Status compute_agg_state(Chunk* chunk, size_t partition_idx);

    // Output the aggregate result streaming
    Status output_agg_streaming(Chunk* chunk);

    // Create the aggregate result columns
    MutableColumns create_agg_result_columns(size_t num_rows);

    // Output the aggregate result
    void output_agg_result(Chunk* chunk, const std::vector<size_t>& partition_idxes, bool eos);
    void output_agg_result(Chunk* chunk, size_t partition_idx, bool eos, bool is_first_chunk);
};

}; // namespace starrocks::pipeline