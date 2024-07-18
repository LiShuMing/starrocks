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
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "common/logging.h"
#include "exprs/agg/aggregate.h"
#include "runtime/agg_state_type_desc.h"
#include "runtime/types.h"
#include "types/logical_type.h"

namespace starrocks {

class AggStateDesc;
using AggStateDescPtr = std::shared_ptr<AggStateDesc>;

/**
 * @brief AggStateDesc is a struct that contains the information of an aggregate function.
 */
class AggStateDesc {
public:
    AggStateDesc(AggStateTypeDescPtr agg_state_type, TypeDescriptor return_type);

    ColumnPtr create_serialize_column() const { return ColumnHelper::create_column(_return_type, false); }
    std::string get_func_name() const { return _agg_state_type->get_func_name(); }
    AggregateFunction* get_agg_function() const { return _agg_function; }

private:
    // nested aggregate function name
    AggStateTypeDescPtr _agg_state_type;

    // nested aggregate function return type
    TypeDescriptor _return_type;

    AggregateFunction* _agg_function;
};

} // namespace starrocks
