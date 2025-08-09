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

#include <string>
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
 * DESC: immediate_type {agg_func}_state_merge(intermediate_type)
 *  input type  : aggregate function's intermediate types
 *  return type : aggregate function's result type
 */
class AggStateMergeFunction final : public AggStateBaseFunction {
public:
    AggStateMergeFunction(AggStateDesc agg_state_desc, TypeDescriptor immediate_type, std::vector<bool> arg_nullables)
            : AggStateBaseFunction(std::move(agg_state_desc), std::move(immediate_type), std::move(arg_nullables)) {
        VLOG_ROW << "AggStateMergeFunction constructor:" << _agg_state_desc.debug_string();
    }

    StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) override {
        if (columns.size() != 1) {
            return Status::InternalError("AggStateMergeFunction execute columns is not 1");
        }
        if (columns.size() != _arg_nullables.size()) {
            return Status::InternalError("AggStateMergeFunction execute columns size " +
                                         std::to_string(columns.size()) + " not match with arg_nullables size " +
                                         std::to_string(_arg_nullables.size()));
        }

        SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&kDefaultAggStateMergeFunctionAllocator);

        bool is_result_nullable = _agg_state_desc.is_result_nullable() || _arg_nullables[0];
        ASSIGN_OR_RETURN(ColumnPtr new_column, _convert_to_nullable_column(columns[0], is_result_nullable));

        // TODO: use mutable ptr as result
        auto& ret_type = _agg_state_desc.get_return_type();
        ColumnPtr result = ColumnHelper::create_column(ret_type, _agg_state_desc.is_result_nullable());
        auto chunk_size = columns[0]->size();

        // finalize agg states into result
        auto align_size = _function->alignof_size();
        auto state_size = ALIGN_TO(_function->size(), align_size);
        AggDataPtr agg_state = reinterpret_cast<AggDataPtr>(std::aligned_alloc(align_size, state_size));
        if (UNLIKELY(agg_state == nullptr)) {
            return Status::InternalError("Failed to allocate memory for aggregate state");
        }
        for (size_t i = 0; i < chunk_size; i++) {
            if (new_column->is_null(i)) {
                result->append_nulls(1);
                continue;
            }
            _function->create(context, agg_state);
            _function->merge(context, new_column.get(), agg_state, i);
            _function->finalize_to_column(context, agg_state, result.get());
            _function->destroy(context, agg_state);
        }
        std::free(agg_state);
        return result;
    }
};

} // namespace starrocks
