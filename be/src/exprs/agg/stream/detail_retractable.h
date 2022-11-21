// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <type_traits>

#include "column/column.h"
#include "column/type_traits.h"

namespace starrocks_udf {
class FunctionContext;
}

using starrocks_udf::FunctionContext;

namespace starrocks::vectorized {
using AggDataPtr = uint8_t*;
using ConstAggDataPtr = const uint8_t*;

// TODO: Support detail agg state reusable between different agg stats.
template <PrimitiveType PT>
struct StreamAggregateFunctionState {
    using T = RunTimeCppType<PT>;
    StreamAggregateFunctionState() = default;
    ~StreamAggregateFunctionState() = default;

    void update_rows(const T& value, int64_t num_rows) {
        auto iter = stream_buffer.find(value);
        if (iter != stream_buffer.end()) {
            stream_buffer[value] = iter->second + num_rows;
        } else {
            stream_buffer[value] = num_rows;
        }
    }

    std::map<T, int64_t> stream_buffer;
    bool need_sync{false};
};

} // namespace starrocks::vectorized
