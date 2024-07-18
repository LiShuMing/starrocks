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

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"

namespace starrocks {

class AggregateFunctionBuilder {
public:
    static AggregateFunction* build(std::string agg_func_name, TypeDescriptor return_type,
                                    std::vector<TypeDescriptor> arg_types, bool is_input_nullable,
                                    TFunctionBinaryType::type binary_type, int func_version) {
        // get function
        if (agg_func_name == "count") {
            return get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, is_input_nullable);
        } else {
            DCHECK_GE(arg_types.size(), 1);
            TypeDescriptor arg_type = arg_types[0];
            // Because intersect_count have two input types.
            // And intersect_count's first argument's type is alwasy Bitmap,
            // so we use its second arguments type as input.
            if (agg_func_name == "intersect_count") {
                arg_type = arg_types[1];
            }

            // Because max_by and min_by function have two input types,
            // so we use its second arguments type as input.
            if (agg_func_name == "max_by" || agg_func_name == "min_by") {
                arg_type = arg_types[1];
            }

            // Because windowfunnel have more two input types.
            // functions registry use 2th args(datetime/date).
            if (agg_func_name == "window_funnel") {
                arg_type = arg_types[1];
            }

            // hack for accepting various arguments
            if (agg_func_name == "exchange_bytes" || agg_func_name == "exchange_speed") {
                arg_type = TypeDescriptor(TYPE_BIGINT);
            }

            if (agg_func_name == "array_union_agg" || agg_func_name == "array_unique_agg") {
                // for array_union_agg use inner type as signature
                // arg_type = arg_type.children[0];
            }
            return get_aggregate_function(agg_func_name, arg_type.type, return_type.type, is_input_nullable,
                                          binary_type, func_version);
        }
    }
};

} // namespace starrocks
