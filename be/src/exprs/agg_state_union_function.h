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

#include <utility>

#include "column/column.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg_state_base_function.h"
#include "exprs/function_context.h"
#include "runtime/agg_state_desc.h"
#include "runtime/mem_pool.h"

namespace starrocks {

/**
 * @brief compute the immediate result of aggregate function
 * DESC: immediate_type {agg_func}_state_union(arg_types)
 *  input type  : aggregate function's immediate types
 *  return type : aggregate function's immediate type
 */

class AggStateUnionFunction final : public AggStateBaseFunction {
public:
    AggStateUnionFunction(AggStateDesc agg_state_desc, TypeDescriptor immediate_type, std::vector<bool> arg_nullables)
            : AggStateBaseFunction(std::move(agg_state_desc), std::move(immediate_type), std::move(arg_nullables)) {
        _mem_pool = std::make_unique<MemPool>();
        VLOG_ROW << "AggStateUnionFunction constructor:" << _agg_state_desc.debug_string();
    }

    ~AggStateUnionFunction() {
        _mem_pool->free_all();
        _mem_pool.reset();
    }

    StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) override {
        if (columns.size() != 2) {
            return Status::InternalError("AggStateUnionFunction execute columns is not 2");
        }
        if (columns.size() != _arg_nullables.size()) {
            return Status::InternalError("AggStateUnionFunction execute columns size " +
                                         std::to_string(columns.size()) + " not match with arg_nullables size " +
                                         std::to_string(_arg_nullables.size()));
        }

        SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&kDefaultAggStateMergeFunctionAllocator);
        Columns new_columns;
        new_columns.reserve(columns.size());
        for (auto i = 0; i < columns.size(); i++) {
            bool is_result_nullable = _agg_state_desc.is_result_nullable() || _arg_nullables[i];
            ASSIGN_OR_RETURN(ColumnPtr new_column, _convert_to_nullable_column(columns[i], is_result_nullable));
            new_columns.emplace_back(new_column);
        }

        auto chunk_size = columns[0]->size();
        auto align_size = _function->alignof_size();
        auto state_size = ALIGN_TO(_function->size(), align_size);
        ColumnPtr result = ColumnHelper::create_column(_immediate_type, _agg_state_desc.is_result_nullable());
        // allocate the agg_state
        VLOG(1) << "allocate " << chunk_size << " rows, align_size=" << align_size << ", state_size=" << state_size;
        AggDataPtr agg_state = _mem_pool->allocate(state_size);
        for (size_t i = 0; i < chunk_size; i++) {
            _function->create(context, agg_state);

            // merge input agg states into result
            for (size_t j = 0; j < new_columns.size(); j++) {
                VLOG(1) << "merge " << i << "th row " << j << "th column";
                _function->merge(context, new_columns[j].get(), agg_state, i);
            }
            // serialize the agg_state into result
            VLOG(1) << "serialize " << i << "th row";
            _function->serialize_to_column(context, agg_state, result.get());
        }
        // destroy the agg_state
        VLOG(1) << "destroy " << chunk_size << "th row";
        _function->destroy(context, agg_state);

        return result;
    }

private:
    std::unique_ptr<MemPool> _mem_pool;
};

} // namespace starrocks
