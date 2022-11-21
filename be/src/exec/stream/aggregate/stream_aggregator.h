// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma  once

#ifndef STARROCKS_STREAM_AGGREGATOR_H
#define STARROCKS_STREAM_AGGREGATOR_H

#include <algorithm>
#include <utility>

#include "exec/stream/aggregate/agg_state_data.h"
#include "exec/stream/state/mem_state_table.h"
#include "exec/stream/state/state_table.h"
#include "exec/stream/stream_chunk.h"
#include "exec/vectorized/aggregator.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::stream {

/**
 * StreamAggregator is used for stream materialized view, it differs with Aggregator:
 * stream aggregator's state is not kept into mem_pool, rather in the StateCache which can be changed adaptively;
 * stream aggregator's processing is by row rather than by chunk which is better in stream materialized view;
 * stream aggregator handles input's chunk as below:
 *  - step1: compute the agg state:
 *      - iterate each row of input chunk,
 *      - 1.1 if its state is in StateCache, get the state's address
 *      - 1.2 query the aggregate StateTable:
 *          - 1.2.1 if it is in StateTable, deserialized the result and store in the StateCache, then redo 1.1
 *          - 1.2.2 insert a new state into StateCache, and get the state's address
 *  - step2: update the intermediate agg state:
 *      - iterate each row of input chunk, call aggregate functions' update functions to accumulate the state.
 *  - step3: update StateTable
 *      - iterate incremental input rows and write back into the StateTable
 *  - step4: output the incremental results
 *      - iterate incremental input rows and output to the next operator
 */
class StreamAggregator final : public Aggregator {
public:
    StreamAggregator(std::shared_ptr<AggregatorParams>&& params);

    static constexpr StreamRowOp INSERT_OP = StreamRowOp::INSERT;
    static constexpr StreamRowOp DELETE_OP = StreamRowOp::DELETE;
    static constexpr StreamRowOp UPDATE_BEFORE_OP = StreamRowOp::UPDATE_BEFORE;
    static constexpr StreamRowOp UPDATE_AFTER_OP = StreamRowOp::UPDATE_AFTER;

    ~StreamAggregator() {
        if (_state != nullptr) {
            close(_state);
        }
    }

    Status open(RuntimeState* state) {
        return Aggregator::open(state);
    }
    Status prepare(RuntimeState* state, ObjectPool* pool,
                   RuntimeProfile* runtime_profile,
                   MemTracker* mem_tracker) override {
        RETURN_IF_ERROR(Aggregator::prepare(state, pool, runtime_profile, mem_tracker));
        RETURN_IF_ERROR(_prepare_state_tables(state));
        return Status::OK();
    }

    // Process input's chunks util `Epoch` chunk is received.
    Status process_chunk(vectorized::Chunk* chunk);

    // Called when need to generate incremental outputs and Output agg_states for the next batch.
    Status output_changes(int32_t chunk_size, vectorized::ChunkPtr* result_chunk,
                          vectorized::ChunkPtr* intermediate_chunk,
                          std::vector<vectorized::ChunkPtr>& detail_chunks);

    // Reset hashmap(like Cache's evict) when the transaction is over.
    Status reset_state(RuntimeState* state);

    void close(RuntimeState* state) override;
private:
    Status _prepare_state_tables(RuntimeState* state);

    DatumRow _convert_to_datum_row(const vectorized::Columns& columns, size_t row_idx);

    template <typename HashMapWithKey>
    Status _output_changes(HashMapWithKey& hash_map_with_key,
                           int32_t chunk_size, vectorized::ChunkPtr* chunk,
                           vectorized::ChunkPtr* intermediate_chunk,
                           std::vector<vectorized::ChunkPtr>& detail_chunks);

    // Only create selected agg funcs by agg_func_ids
    vectorized::Columns _create_needed_agg_result_columns(size_t num_rows,
                                                          const std::vector<int32_t>& agg_func_ids,
                                                          bool use_intermediate);

    vectorized::ChunkPtr _build_output_chunk_with_ops(const vectorized::Columns& group_by_columns,
                                                      const vectorized::Columns& agg_result_columns,
                                                      const UInt8ColumnPtr& ops,
                                                      bool is_intermediate_result);

    Status _output_intermediate_changes(int32_t chunk_size,
                                        const vectorized::Columns& group_by_columns,
                                        vectorized::ChunkPtr* intermediate_chunk);

    Status _output_result_changes(int32_t chunk_size,
                                  const vectorized::Columns& group_by_columns,
                                  vectorized::ChunkPtr* intermediate_chunk);
    Status _output_result_post_retract_changes(size_t chunk_size,
                                               const vectorized::Columns& group_by_columns,
                                               const vectorized::Buffer<uint8_t>& prev_existence,
                                               const ChunkPtr& prev_result_chunk,
                                               ChunkPtr* result_chunk);
    Status _output_result_prev_retract_changes(size_t chunk_size,
                                               const vectorized::Columns& group_by_columns,
                                               ChunkPtr* prev_result_chunk,
                                               vectorized::Buffer<uint8_t>* prev_existence);
    Status _output_result_changes_without_retract(size_t chunk_size,
                                                  const vectorized::Columns& group_by_columns,
                                                  ChunkPtr* result_chunk);

    Status _output_retract_detail(int32_t chunk_size, const vectorized::Columns& group_by_columns,
                                  std::vector<vectorized::ChunkPtr>& detail_chunks);

private:
    // Store buffers which can be reused in the incremental compute.
    std::unique_ptr<MemPool> _mem_pool;

    // Store group by keys to agg state map.
    std::unique_ptr<StateTable> _result_state_table;
    std::unique_ptr<StateTable> _intermediate_state_table;
    // TODO: support merge into one detail table later.
    std::vector<std::unique_ptr<StateTable>> _detail_state_tables;
    std::vector<SlotDescriptor*> _output_slots_without_op;
    int32_t _count_agg_idx{0};

    // store all agg states
    std::vector<std::unique_ptr<AggStateData>> _agg_func_states;
    // output intermediate columns.
    std::vector<int32_t> _intermediate_agg_func_ids;

    std::unique_ptr<IntermediateAggGroupState> _result_agg_group;
    std::unique_ptr<IntermediateAggGroupState> _intermediate_agg_group;
    std::unique_ptr<DetailAggGroupState> _detail_agg_group;

    std::vector<DatumRow> _non_found_keys;
    // Changed group by keys to generate outputs
    SliceHashSet _changed_keys;
};


} // namespace starrocks::vectorized

#endif //STARROCKS_STREAM_AGGREGATOR_H
