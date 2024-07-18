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
#include "runtime/agg_state_type_desc.h"

namespace starrocks {

class AggStateFunction {
public:
    AggStateFunction(TypeDescriptor return_type, AggStateTypeDescPtr agg_state_type) {
        _agg_state_desc = std::make_shared<AggStateDesc>(agg_state_type, return_type);
        DCHECK(_agg_state_desc != nullptr);
        _function = _agg_state_desc->get_agg_function();
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
        auto result = _agg_state_desc->create_serialize_column();
        auto chunk_size = columns[0]->size();
        // ensure columns' nullable are expected
        _function->convert_to_serialize_format(context, columns, chunk_size, &result);
        return result;
    }

private:
    AggStateDescPtr _agg_state_desc;
    AggregateFunction* _function;
};
using AggStateFunctionPtr = std::shared_ptr<starrocks::AggStateFunction>;

} // namespace starrocks
