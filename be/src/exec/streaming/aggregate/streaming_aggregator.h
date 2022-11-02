// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma  once

#ifndef STARROCKS_STREAMING_AGGREGATOR_H
#define STARROCKS_STREAMING_AGGREGATOR_H

#include <algorithm>
#include <utility>

#include "exec/streaming/aggregate/streaming_hash_map.h"
#include "exec/streaming/state/state_table.h"
#include "exec/streaming/stream_chunk.h"
#include "exec/vectorized/aggregator.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::streaming {

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

class StreamingAggregator final : public Aggregator {
    StreamingAggregator(const TPlanNode& tnode);

    ~StreamingAggregator() {
        if (_state != nullptr) {
            close(_state);
        }
    }

    Status open(RuntimeState* state);
    Status prepare(RuntimeState* state, ObjectPool* pool,
                   RuntimeProfile* runtime_profile,
                   MemTracker* mem_tracker) override {
        return Aggregator::prepare(state, pool, runtime_profile, mem_tracker);
    }

    // Process input's chunks util `Epoch` chunk is received.
    Status process_chunk(vectorized::Chunk* chunk);

    // Flush the updated chunk into StateTable.
    Status process_barrier(const StreamBarrier& barrier);

    void close(RuntimeState* state) override;
private:
    Status _build_hash_map(size_t chunk_size, bool agg_group_by_with_limit = false);

    // Called when need to generate incremental outputs.
    Status _build_changes(int32_t chunk_size, vectorized::ChunkPtr* chunk);

    // If input row is INSERT/UPDATE_AFTER, need accumulate the input.
    void _accumulate_row(int32_t row_idx);
    // If input row is DELETE/UPDATE_BEFORE, need accumulate the input.
    void _retract_row(int32_t row_idx);

private:
    // Store group by keys to agg state map.
    std::unique_ptr<StreamingHashMap> _agg_map;
    std::unique_ptr<StateCache> _state_cache;
    std::unique_ptr<StateTable> _result_state_table;
    std::unique_ptr<StateTable> _agg_state_table;
    Epoch _prev_epoch;
};

} // namespace starrocks::vectorized

#endif //STARROCKS_STREAMING_AGGREGATOR_H
