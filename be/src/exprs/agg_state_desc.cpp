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

#include "exprs/agg_state_desc.h"

#include <string>
#include <vector>

#include "exec/aggregate_function_builder.h"
#include "exprs/agg/aggregate.h"
#include "runtime/agg_state_type_desc.h"

namespace starrocks {

AggStateDesc::AggStateDesc(AggStateTypeDescPtr agg_state_type, TypeDescriptor return_type)
        : _agg_state_type(agg_state_type), _return_type(return_type) {
    DCHECK(_agg_state_type != nullptr);
    _agg_function = AggregateFunctionBuilder::build(
            _agg_state_type->get_func_name(), _return_type, _agg_state_type->get_arg_types(),
            _agg_state_type->is_result_nullable(), TFunctionBinaryType::BUILDIN, _agg_state_type->get_func_version());
    if (_agg_function == nullptr) {
        LOG(WARNING) << "Failed to get aggregate function for " << _func_name;
    }
}

} // namespace starrocks
