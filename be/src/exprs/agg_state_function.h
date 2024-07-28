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
#include "exprs/function_context.h"
#include "runtime/agg_state_desc.h"

namespace starrocks {

class AggStateFunction {
public:
    AggStateFunction(AggStateDesc* agg_state_desc, TypeDescriptor immediate_type, bool has_nullable_child)
            : _agg_state_desc(std::move(agg_state_desc)),
              _immediate_type(std::move(immediate_type)),
              _has_nullable_child(has_nullable_child) {
        DCHECK(_agg_state_desc != nullptr);
        _function = AggStateDesc::get_agg_state_func(_agg_state_desc, _has_nullable_child);
        DCHECK(_function != nullptr);
    }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (_function == nullptr) {
            return Status::InternalError("AggStateFunction is nullptr  for " + _agg_state_desc->get_func_name());
        }
        return Status::OK();
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) { return Status::OK(); }

    StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) {
        if (columns.size() == 0) {
            return Status::InternalError("AggStateFunction execute columns is empty");
        }
        if (columns.size() != 1) {
            return Status::InternalError("Invalid AggStateFunction input columns size: " +
                                         std::to_string(columns.size()));
        }

        auto column = columns[0];
        if (!_has_nullable_child && column->is_nullable()) {
            return Status::InternalError("AggStateFunction input column is nullable but agg function is not nullable");
        }
        // intermdiated column
        auto result = ColumnHelper::create_column(_immediate_type, _has_nullable_child);
        auto chunk_size = column->size();

        if (_has_nullable_child && !column->is_nullable()) {
            column = ColumnHelper::cast_to_nullable_column(column);
            Columns new_columns = {column};
            _function->convert_to_serialize_format(context, new_columns, chunk_size, &result);
        } else {
            _function->convert_to_serialize_format(context, columns, chunk_size, &result);
        }
        return result;
    }

private:
    AggStateDesc* _agg_state_desc;
    TypeDescriptor _immediate_type;
    bool _has_nullable_child;
    const AggregateFunction* _function;
};
using AggStateFunctionPtr = std::shared_ptr<starrocks::AggStateFunction>;

} // namespace starrocks
