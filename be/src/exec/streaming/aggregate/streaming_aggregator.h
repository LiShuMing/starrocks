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

class StreamingAggregator final : public pipeline::ContextWithDependency {
    StreamingAggregator();

    ~StreamingAggregator() {
        if (_state != nullptr) {
            close(_state);
        }
    }

    Status open(RuntimeState* state);
    Status prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile, MemTracker* mem_tracker);

    void close(RuntimeState* state) override;
private:

private:
    RuntimeState* _state = nullptr;
};

} // namespace starrocks::vectorized

#endif //STARROCKS_STREAMING_AGGREGATOR_H
