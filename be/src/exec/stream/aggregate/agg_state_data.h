// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#ifndef STARROCKS_AGG_STATE_DATA_H
#define STARROCKS_AGG_STATE_DATA_H

#include <map>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exec/stream/state/state_table.h"
#include "exec/stream/stream_chunk.h"
#include "exec/stream/stream_fdw.h"
#include "exprs/agg/aggregate.h"

namespace starrocks::stream {
using AggGroupStatePtr = uint8_t*;
using UInt8ColumnPtr = std::shared_ptr<vectorized::UInt8Column>;
using StreamStateTableKind = vectorized::StreamStateTableKind;

struct AggStateDataParams {
    int32_t func_idx;
    int32_t intermediate_table_idx; // maybe result/intermediate table decided by StreamStateTableKind
    int32_t retract_table_idx; // detail table idx only used by Detail state kind.
};

class AggStateData {
public:
    AggStateData(const vectorized::AggregateFunction* func,
                 starrocks_udf::FunctionContext* agg_fn_ctx,
                 vectorized::StreamStateTableKind agg_state_kind,
                 size_t agg_state_offset,
                 AggStateDataParams params)
            : _agg_function(func), _agg_fn_ctx(agg_fn_ctx),
              _agg_state_kind(agg_state_kind),
              _agg_state_offset(agg_state_offset), _params(params) {
        std::cout <<"agg_state_kind="<<agg_state_kind
                  << " func_idx="<<_params.func_idx
                  << " intermediate_table_idx=" << _params.intermediate_table_idx
                  << " retract_table_idx="<<_params.retract_table_idx<<std::endl;
    }
    virtual ~AggStateData() = default;

    // Restore intermediate state if needed by using last run state table's results from
    // result/intermediate state table.
    Status allocate_intermediate_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                        const std::vector<ChunkPtrOr>& result_chunks,
                                        const Buffer<AggGroupStatePtr>& agg_group_state) const;

    // Restore retract(detail) state if needed by using last run state's result from
    // detail state table
    Status allocate_retract_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                  const std::vector<ChunkIteratorPtrOr>& result_chunks,
                                  const Buffer<AggGroupStatePtr>& agg_group_state) const;

    Status process_chunk(size_t chunk_size,
                         const std::vector<ChunkPtrOr>& result_chunks,
                         const Buffer<uint8_t>& keys_not_in_map,
                         const StreamRowOp* ops,
                         std::vector<std::vector<const vectorized::Column*>>& raw_columns,
                         const Buffer<vectorized::AggDataPtr>& agg_group_state) const;

    Status output_result(size_t chunk_size,
                         const Buffer<vectorized::AggDataPtr>& agg_group_state,
                         vectorized::Column* to) const;

    Status output_detail(size_t chunk_size,
                         const Buffer<vectorized::AggDataPtr>& agg_group_state,
                         const vectorized::Columns& to, vectorized::Column * count) const;

    const int32_t agg_func_id() const { return _params.func_idx; }
    const int32_t intermediate_table_idx() const { return _params.intermediate_table_idx; }
    const int32_t retract_table_idx() const { return _params.retract_table_idx; }
    const vectorized::StreamStateTableKind state_table_kind() const { return _agg_state_kind; }
protected:
    const vectorized::AggregateFunction* _agg_function;
    starrocks_udf::FunctionContext* _agg_fn_ctx;
    vectorized::StreamStateTableKind _agg_state_kind;
    // idx represents the offset in all aggregate functions, used for agg_group_state
    size_t _agg_state_offset;
    AggStateDataParams _params;
};

class AggGroupState {
};

class IntermediateAggGroupState {
public:
    IntermediateAggGroupState(std::vector<AggStateData*> agg_states, StateTable* state_table):
            _agg_states(std::move(agg_states)), _state_table(state_table){
        DCHECK(state_table);
        for (auto& agg_state: _agg_states) {
            _agg_func_ids.push_back(agg_state->agg_func_id());
        }
    }

    ~IntermediateAggGroupState() = default;

    const std::vector<AggStateData*>& agg_states() const {
        return _agg_states;
    }

    const std::vector<int32_t>& agg_func_ids() const { return _agg_func_ids; }

    Status process_chunk(size_t chunk_size,
                         const Buffer<DatumRow>& non_found_keys,
                         const Buffer<uint8_t>& keys_not_in_map,
                         const StreamRowOp* ops,
                         std::vector<std::vector<const vectorized::Column*>>& raw_columns,
                         const Buffer<vectorized::AggDataPtr>& agg_group_state) const;
private:
    std::vector<AggStateData*> _agg_states;
    std::vector<int32_t> _agg_func_ids;
    StateTable* _state_table;
};

class DetailAggGroupState {
public:
    DetailAggGroupState(std::vector<AggStateData*> agg_states,
                        StateTable* result_state_table,
                        StateTable* intermediate_state_table,
                        const std::vector<std::unique_ptr<StateTable>>& detail_state_tables):
            _agg_states(std::move(agg_states)),
            _result_state_table(result_state_table),
            _intermediate_state_table(intermediate_state_table),
            _detail_state_tables(detail_state_tables){
        DCHECK(_result_state_table);
        DCHECK(!_detail_state_tables.empty());
        DCHECK_EQ(_detail_state_tables.size(), _agg_states.size());
    }

    ~DetailAggGroupState() = default;

    const std::vector<AggStateData*>& agg_states() const {
        return _agg_states;
    }


    const std::vector<std::unique_ptr<StateTable>>& detail_state_tables() const {
        return _detail_state_tables;
    }

    Status process_chunk(size_t chunk_size,
                         const std::vector<DatumRow>& non_found_keys,
                         const std::vector<uint8_t>& keys_not_in_map,
                         const StreamRowOp* ops,
                         std::vector<std::vector<const vectorized::Column*>>& raw_columns,
                         const Buffer<vectorized::AggDataPtr>& agg_group_state) const;
//
//    Status flush(size_t chunk_size,
//                 const Buffer<vectorized::AggDataPtr>& agg_group_state,
//                 vectorized::Columns* to) const;
private:
    std::vector<AggStateData*> _agg_states;
    StateTable* _result_state_table;
    StateTable* _intermediate_state_table;
    const std::vector<std::unique_ptr<StateTable>>& _detail_state_tables;
};

} // namespace starrocks::stream
#endif //STARROCKS_AGG_STATE_DATA_H
