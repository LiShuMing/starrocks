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
#include "common/status.h"
#include "exprs/agg_state_desc.h"
#include "exprs/function_context.h"

namespace starrocks {

class AggStateFunction {
public:
    AggStateFunction(TypeDescriptor return_type, AggStateDescPtr agg_state_desc)
            : _agg_state_desc(std::move(agg_state_desc)) {}

    static Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        auto* agg_func = _agg_state_desc->get_agg_function();
        if (agg_func == nullptr) {
            return Status::InternalError("AggStateFunction is nullptr  for " + _agg_state_desc->get_func_name());
        }
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) { return Status::OK(); }

    static StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) {
        if (columns.size() == 0) {
            return Status::InternalError("AggStateFunction execute columns is empty");
        }
        auto result = _agg_state_desc->create_serialize_column();
        auto chunk_size = columns[0]->size();
        // ensure columns' nullable are expected
        _funtion->convert_to_serialize_format(context, columns, chunk_size, result);
        return result;
    }

private:
    AggStateDescPtr _agg_state_desc;
};
using AggStateFunctionPtr = std::shared_ptr<starrocks::AggStateFunction>;

} // namespace starrocks
