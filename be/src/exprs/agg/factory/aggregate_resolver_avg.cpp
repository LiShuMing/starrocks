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

#include <memory>
#include <string>
#include <vector>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "exprs/agg/aggregate.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "types/logical_type.h"
#include "column/column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/agg/array_agg.h"
#include "exprs/agg/distinct.h"
#include "runtime/decimalv2_value.h"
#include "types/logical_type_infra.h"
#include "util/phmap/phmap.h"
#include "column/column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/agg/array_agg.h"
#include "exprs/agg/distinct.h"
#include "runtime/decimalv2_value.h"
#include "types/logical_type_infra.h"
#include "util/phmap/phmap.h"

namespace starrocks {

struct AvgDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt> && !lt_is_string<lt>) {
            auto func = AggregateFactory::MakeAvgAggregateFunction<lt>();
            using AvgState = AvgAggregateState<RunTimeCppType<ImmediateAvgResultLT<lt>>>;
            resolver->add_aggregate_mapping<lt, AvgResultLT<lt>, AvgState>("avg", true, func);
        }
    }
};

struct ArrayAggDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt> || lt_is_json<lt>) {
            auto func = std::make_shared<ArrayAggAggregateFunction<lt>>();
            using AggState = ArrayAggAggregateState<lt>;
            resolver->add_aggregate_mapping<lt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>("array_agg", false,
                                                                                                   func);
        }
    }
};

void AggregateFuncResolver::register_avg() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, AvgDispatcher(), this);
        type_dispatch_all(type, ArrayAggDispatcher(), this);
    }
    type_dispatch_all(TYPE_JSON, ArrayAggDispatcher(), this);
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("decimal_avg");
}

} // namespace starrocks
