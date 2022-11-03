// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_AGG_STATE_DATA_H
#define STARROCKS_AGG_STATE_DATA_H

#include "exprs/agg/aggregate.h"
#include "exec/streaming/row_serde.h"
#include "exec/streaming/aggregate/agg_state_cache.h"
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

class AggStateData {
public:
    AggStateData(const vectorized::AggregateFunction* func,
                 starrocks_udf::FunctionContext* agg_fn_ctx,
                 StateCacheRawPtr state_cache,
                 size_t agg_state_offset,
                 int32_t idx)
            : _agg_function(func), _agg_fn_ctx(agg_fn_ctx), _state_cache(state_cache),
              _agg_state_offset(agg_state_offset), _idx(idx){}
    ~AggStateData() {}

    Status allocate_state(size_t chunk_size, std::vector<int8_t>* not_found,
                          const std::vector<DatumRowOpt>& result_rows,
                          const Buffer<AggGroupStatePtr>& agg_group_state) {
        int32_t j = 0;
        for (auto i = 0; i < chunk_size; i++) {
            if ((*not_found)[i]) {
                DCHECK_LT(j, chunk_size);
                auto& result_row = result_rows[j++];
                if (result_row.has_value()) {
                    // deserialize result row and allocate it to agg_state
                    auto row_data = result_row.value();
                    DCHECK_LT(_result_idx, row_data.size());
                    Datum& cell_data = row_data[_result_idx];
                    // ATTENTION!!!
                    Datum::deserialize(agg_group_state[_idx] + _agg_state_offset, &cell_data);
                } else {
                    // replacement new
                    _agg_function->create(_agg_fn_ctx, agg_group_state[_idx] + _agg_state_offset);
                }
            }
        }
        return Status::OK();
    }

    virtual Status process_chunk(size_t chunk_size, const std::vector<RowOp>& ops,
                                 std::vector<const vectorized::Column*> raw_columns,
                                 const Buffer<AggGroupStatePtr>& agg_group_state);

    virtual Status build_changes(size_t chunk_size, ChunkPtr* chunk);
protected:
    const vectorized::AggregateFunction* _agg_function;
    starrocks_udf::FunctionContext* _agg_fn_ctx;
    StateCacheRawPtr _state_cache;
    size_t _agg_state_offset;
    // idx represents the offset in all aggregate functions, used for agg_group_state
    int32_t _idx;
    // idx represents the offset in the result_output row.
    int32_t _result_idx;
};

class AggStateDataWithIntermediate: public AggStateData {
public:
    AggStateDataWithIntermediate(const vectorized::AggregateFunction* func,
                                 starrocks_udf::FunctionContext* agg_fn_ctx,
                                 StateCacheRawPtr state_cache,
                                 size_t agg_state_offset,
                                 int32_t idx,
                                 int32_t intermediate_idx)
            :AggStateData(func, agg_fn_ctx, state_cache, agg_state_offset, idx),
             _intermediate_idx(intermediate_idx){}

    ~AggStateDataWithIntermediate() = default;

    Status process_chunk(size_t chunk_size, const std::vector<RowOp>& ops,
                         std::vector<const vectorized::Column*> raw_columns,
                         const Buffer<AggGroupStatePtr>& agg_group_state) override {
        bool need_refresh = false;
        for (int i = 0; i < chunk_size; i++) {
            auto& op = ops[i];
            switch (op) {
            case RowOp::INSERT:
            case RowOp::UPDATE_BEFORE:
                _agg_function->update(_agg_fn_ctx, raw_columns.data(), agg_group_state[_idx] + _agg_state_offset, i);
                break;
            case RowOp::DELETE:
            case RowOp::UPDATE_AFTER:
                _agg_function->retract(_agg_fn_ctx, raw_columns.data(), agg_group_state[_idx] + _agg_state_offset, i,
                                       need_refresh);
                break;
            }
        }
        return Status::OK();
    }

    Status build_changes(size_t chunk_size, ChunkPtr* chunk) override {
        return Status::OK();
    }
private:
    int32_t _intermediate_idx;
};

class AggStateDataWithDetail: public AggStateData {
public:
    AggStateDataWithDetail(const vectorized::AggregateFunction* func,
                           starrocks_udf::FunctionContext* agg_fn_ctx,
                           StateCacheRawPtr state_cache,
                           size_t agg_state_offset,
                           int32_t idx)
            :AggStateData(func, agg_fn_ctx, state_cache, agg_state_offset, idx){}

    ~AggStateDataWithDetail() = default;

    Status process_chunk(size_t chunk_size, const std::vector<RowOp>& ops,
                         std::vector<const vectorized::Column*> raw_columns,
                         const Buffer<AggGroupStatePtr>& agg_group_state) override {
        // do nothing? maybe add a cache.
        // do accumulate in `output`
        return Status::OK();
    }

    Status build_changes(size_t chunk_size, ChunkPtr* chunk) override {
        return Status::OK();
    }
private:

};


} // namespace starrocks::streaming
#endif //STARROCKS_AGG_STATE_DATA_H
