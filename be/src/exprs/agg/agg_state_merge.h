// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "column/binary_column.h"
#include "column/object_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks {
struct AggStateMergeState {};

class AggStateMerge final : public AggregateFunctionBatchHelper<AggStateMergeState, AggStateMerge> {
public:
    AggStateUnion(AggregateFunctionPtr nested_function) : _function(std::move(nested_function)) {}

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        _function->reset(ctx, args, state);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK_EQ(1, columns.size());
        _function->merge(ctx, columns[0], state, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        _function->merge(ctx, column, state, row_num);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        _function->finalize_to_column(ctx, state, to);
        // TODO: repeat data into start + 1 to end
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        _function->serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK_EQ(1, src.size());
        auto& src = src[0];
        for (size_t i = 0; i < chunk_size; ++i) {
            _dst->append(src, i);
        }
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        _function->finalize_to_column(ctx, state, to);
    }

    std::string get_name() const override { return "agg_state_merge"; }

private:
    AggregateFunctionPtr _function;
};

} // namespace starrocks
