// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma  once

#ifndef STARROCKS_STREAMING_AGGREGATOR_H
#define STARROCKS_STREAMING_AGGREGATOR_H

#include <algorithm>
#include <any>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <queue>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/vectorized/aggregate/agg_hash_variant.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "gen_cpp/QueryPlanExtra_constants.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "streaming_hash_map.h"

namespace starrocks::vectorized {

using AggDataPtr = uint8_t*;
// StreamingAggregator is used for streaming materialized view, it differs with Aggregator:
// - streaming aggregator's state is not kept into mem_pool, rather in the StateCache which can be changed adaptively;
// - streaming aggregator's processing is by row rather than by chunk which is better in streaming materialized view;
//
// streaming aggregator handles input's chunk as below:
// - step1: compute the agg state:
//  - iterate each row of input chunk,
//      - 1.1 if its state is in StateCache, get the state's address
//      - 1.2 query the aggregate StateTable:
//          - 1.2.1 if it is in StateTable, deserialized the result and store in the StateCache, then redo 1.1
//          - 1.2.2 insert a new state into StateCache, and get the state's address
// - step2: update the intermediate agg state:
//  - iterate each row of input chunk, call aggregate functions' update functions to accumulate the state.
// - step3: update StateTable
//  - iterate incremental input rows and write back into the StateTable
// - step4: output the incremental results
//  - iterate incremental input rows and output to the next operator
struct ColumnType {
    TypeDescriptor result_type;
    bool is_nullable;
};

class StreamingAggregator final : public pipeline::ContextWithDependency {
    StreamingAggregator();

    ~StreamingAggregator() {
        if (_state != nullptr) {
            close(_state);
        }
    }

    Status open(RuntimeState* state);
    Status prepare(RuntimeState* state, ObjectPool* pool,
                   RuntimeProfile* runtime_profile,
                   MemTracker* mem_tracker);

    // Process input's chunks util `Epoch` chunk is received.
    Status process_chunk(vectorized::Chunk* chunk);

    // Flush the updated chunk into StateTable.
    Status process_epoch();

    void close(RuntimeState* state) override;
private:
    // Evaluate input chunk and save it into inner variables.
    Status _evaluate_exprs(vectorized::Chunk* chunk);
    //
    Status _build_hash_map(size_t chunk_size, bool agg_group_by_with_limit = false);

    // Called when need to generate incremental outputs.
    void _build_changes(int32_t chunk_size, vectorized::ChunkPtr* chunk);

    // If input row is INSERT/UPDATE_AFTER, need accumulate the input.
    void _accumulate_row(int32_t row_idx);
    // If input row is DELETE/UPDATE_BEFORE, need accumulate the input.
    void _retract_row(int32_t row_idx);

    // Create new aggregate function result column by type
    vectorized::Columns _create_agg_result_columns(size_t num_rows);
    vectorized::Columns _create_group_by_columns(size_t num_rows);
private:
    // Store group by keys to agg state map.
    StreamingHashMap _agg_map;

    // Exprs used to evaluate conjunct
    std::vector<ExprContext*> _conjunct_ctxs;

    // Exprs used to evaluate group by column
    std::vector<ExprContext*> _group_by_expr_ctxs;
    vectorized::Columns _group_by_columns;
    std::vector<ColumnType> _group_by_types;

    // The followings are aggregate function information:
    std::vector<starrocks_udf::FunctionContext*> _agg_fn_ctxs;
    // The offset of the n-th aggregate function in a row of aggregate functions.
    std::vector<size_t> _agg_states_offsets;
    vectorized::Buffer<vectorized::AggDataPtr> _tmp_agg_states;
    std::vector<const vectorized::AggregateFunction*> _agg_functions;
    // The expr used to evaluate agg input columns
    // one agg function could have multi input exprs
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    std::vector<std::vector<vectorized::ColumnPtr>> _agg_input_columns;
    //raw pointers in order to get multi-column values
    std::vector<std::vector<const vectorized::Column*>> _agg_input_raw_columns;

    RuntimeState* _state = nullptr;
};

} // namespace starrocks::vectorized

#endif //STARROCKS_STREAMING_AGGREGATOR_H
