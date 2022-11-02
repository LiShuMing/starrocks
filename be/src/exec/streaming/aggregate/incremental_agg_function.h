// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_AGG_STATE_DATA_H
#define STARROCKS_AGG_STATE_DATA_H

#include "exprs/agg/aggregate.h"
#include "exec/streaming/row_serde.h"
#include "exec/streaming/state/state_cache.h"
#include "exec/streaming/streaming_fdw.h"
#include "exec/streaming/stream_chunk.h"

namespace starrocks::streaming {

/**
 * For each aggregate function, it may use different agg state kind for Incremental MV:
 *  - Result       :  Use result table data as AggStateData, eg sum/count
 *  - Intermediate : Use intermediate table data as AggStateData which is a common state for Accumulate mode just as agg_data in OLAP mode.
 *  - Detail       : Use detail table data as AggStateData to accumulate/retract in retract mode.
 *
 *  Optimizer will decide which kind of agg state data is used for each agg function in different retract mode.
 */
enum AggStateDataKind {
    Result,
    Intermediate,
    Detail
};

using AggGroupStatePtr = uint8_t*;

struct CacheValue {
    DatumRowPtr result_output;
    AggGroupStatePtr agg_group;
};

class IncrementalAggFunction {
public:
    IncrementalAggFunction(const vectorized::AggregateFunction* func,
                           starrocks_udf::FunctionContext* agg_fn_ctx,
                           StateCacheRawPtr state_cache,
                           size_t agg_state_offset,
                           int32_t idx)
            : _agg_function(func), _agg_fn_ctx(agg_fn_ctx), _state_cache(state_cache),
              _agg_state_offset(agg_state_offset), _idx(idx){}
    ~IncrementalAggFunction() {}

    virtual Status process_chunk(size_t chunk_size, const std::vector<RowOp>& ops,
                                 std::vector<const vectorized::Column*> raw_columns,
                                 const Buffer<AggGroupStatePtr>& agg_group_state) {
        bool need_refresh = false;
        for (int i = 0; i < chunk_size; i++) {
            auto& op = ops[i];
            switch (op) {
            case RowOp::INSERT:
            case RowOp::UPDATE_BEFORE:
                _agg_function->update(_agg_fn_ctx, raw_columns.data(),
                                      agg_group_state[_idx] + _agg_state_offset, i);
                break;
            case RowOp::DELETE:
            case RowOp::UPDATE_AFTER:
                _agg_function->retract(_agg_fn_ctx, raw_columns.data(),
                                       agg_group_state[_idx] + _agg_state_offset, i, need_refresh);
                break;
            }
        }
    }

    virtual Status build_changes(size_t chunk_size, ChunkPtr* chunk);
protected:
    const vectorized::AggregateFunction* _agg_function;
    starrocks_udf::FunctionContext* _agg_fn_ctx;
    StateCacheRawPtr _state_cache;
    size_t _agg_state_offset;
    int32_t _idx;
};

class IncrementalAggFunctionWithResult : public IncrementalAggFunction {
public:
    IncrementalAggFunctionWithResult(const vectorized::AggregateFunction* func,
                                     starrocks_udf::FunctionContext* agg_fn_ctx,
                                     StateCacheRawPtr state_cache,
                                     size_t agg_state_offset,
                                     int32_t idx)
            :IncrementalAggFunction(func, agg_fn_ctx, state_cache, agg_state_offset, idx){}

    ~IncrementalAggFunctionWithResult() = default;

    Status build_changes(size_t chunk_size, ChunkPtr* chunk) override {
        return Status::OK();
    }
private:

};

class IncrementalAggFunctionWithIntermediate: public IncrementalAggFunction {
public:
    IncrementalAggFunctionWithIntermediate(const vectorized::AggregateFunction* func,
                                           starrocks_udf::FunctionContext* agg_fn_ctx,
                                           StateCacheRawPtr state_cache,
                                           size_t agg_state_offset,
                                           int32_t idx)
            :IncrementalAggFunction(func, agg_fn_ctx, state_cache, agg_state_offset, idx){}

    ~IncrementalAggFunctionWithIntermediate() = default;

    Status build_changes(size_t chunk_size, ChunkPtr* chunk) override {
        return Status::OK();
    }
private:

};

class IncrementalAggFunctionWithDetail: public IncrementalAggFunction {
public:
    IncrementalAggFunctionWithDetail(const vectorized::AggregateFunction* func,
                                     starrocks_udf::FunctionContext* agg_fn_ctx,
                                     StateCacheRawPtr state_cache,
                                     size_t agg_state_offset,
                                     int32_t idx)
            :IncrementalAggFunction(func, agg_fn_ctx, state_cache, agg_state_offset, idx){}

    ~IncrementalAggFunctionWithDetail() = default;

    Status process_chunk(size_t chunk_size, const std::vector<RowOp>& ops,
                         std::vector<const vectorized::Column*> raw_columns,
                         const Buffer<AggGroupStatePtr>& agg_group_state) override {
        bool need_refresh = false;
        for (int i = 0; i < chunk_size; i++) {
            auto& op = ops[i];
            switch (op) {
            case RowOp::INSERT:
            case RowOp::UPDATE_BEFORE:
                _agg_function->update(_agg_fn_ctx, raw_columns.data(),
                                      agg_group_state[_idx] + _agg_state_offset, i);
                break;
            case RowOp::DELETE:
            case RowOp::UPDATE_AFTER:
                _agg_function->retract(_agg_fn_ctx, raw_columns.data(),
                                       agg_group_state[_idx] + _agg_state_offset, i, need_refresh);
                break;
            }
        }
    }
    Status build_changes(size_t chunk_size, ChunkPtr* chunk) override {
        return Status::OK();
    }
private:

};


} // namespace starrocks::streaming
#endif //STARROCKS_AGG_STATE_DATA_H
