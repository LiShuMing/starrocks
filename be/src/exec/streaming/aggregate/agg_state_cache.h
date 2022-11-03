// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_AGG_STATE_CACHE_H
#define STARROCKS_AGG_STATE_CACHE_H

#include "exprs/agg/aggregate.h"
#include "exec/streaming/row_serde.h"
#include "exec/streaming/state/state_cache.h"
#include "exec/streaming/streaming_fdw.h"
#include "exec/streaming/stream_chunk.h"

namespace starrocks::streaming {

using AggGroupStatePtr = uint8_t*;

struct AggGroupCacheValue {
//    // Prev result which used to generate retract message and used as agg state.
//    DatumRowPtr prev_output;
    // A continuous address which represents all agg functions' states.
    AggGroupStatePtr agg_group_state;

    AggGroupCacheValue(int agg_group_state_size) {
        agg_group_state = new uint8_t[agg_group_state_size];
    }

    ~AggGroupCacheValue() {
        delete[] agg_group_state;
    }
};

static void delete_agg_group_cache_value(const CacheKey& key, void* value) {
    auto* state_value = (AggGroupCacheValue*)value;
    state_value->~AggGroupCacheValue();
}

}
#endif //STARROCKS_AGG_STATE_CACHE_H
