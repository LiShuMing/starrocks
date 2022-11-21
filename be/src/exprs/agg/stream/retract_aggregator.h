// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once
#ifndef STARROCKS_RETRACT_AGGREGATOR_H
#define STARROCKS_RETRACT_AGGREGATOR_H

#include <type_traits>
#include "column/column.h"

namespace starrocks::stream {
using AggDataPtr = uint8_t*;
using ConstAggDataPtr = const uint8_t*;

class RetractAggregateFunction {
public:
    //  STREAM MV METHODS

    // Update the aggregation state
    // columns points to columns containing arguments of aggregation function.
    // row_num is number of row which should be updated.
    virtual void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                         size_t row_num) const {
        throw std::runtime_error("retract function in aggregate is not supported for now.");
    }

    virtual void restore_detail(FunctionContext* ctx, size_t chunk_size,
                                const Columns& column, AggDataPtr __restrict state) const {
        throw std::runtime_error("restore_detail function in aggregate is not supported for now.");
    }

    virtual void sync_detail(FunctionContext* ctx, AggDataPtr __restrict state) const {
        throw std::runtime_error("retract function in aggregate is not supported for now.");
    }

    // Change the aggregation state to final result if necessary
    virtual void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state,
                               const Columns& to, Column * count) const {
        throw std::runtime_error("retract function in aggregate is not supported for now.");
    }

    // Intermediate state kind: result / intermediate state table
    // Result       : sum/count/min/max ...
    // Intermediate : AVG
    // Retract state table kind:
    // same_with_intermediate / detail
    virtual StreamStateTableKind stream_state_table_kind(bool need_retract) const {
        return StreamStateTableKind::UnSupported;
    }
};

}
#endif //STARROCKS_RETRACT_AGGREGATOR_H
