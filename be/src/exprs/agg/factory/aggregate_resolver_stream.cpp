// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/stream/retract_maxmin.h"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"

namespace starrocks::vectorized {

struct RetractMinMaxDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt> || pt_is_string<pt>) {
            resolver->add_aggregate_mapping<pt, pt, MinAggregateData<pt>>(
                    "retract_min", true, AggregateFactory::MakeRetractMinAggregateFunction<pt>());
            resolver->add_aggregate_mapping<pt, pt, MaxAggregateData<pt>>(
                    "retract_max", true, AggregateFactory::MakeRetractMaxAggregateFunction<pt>());
        }
    }
};

void AggregateFuncResolver::register_retract_functions() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, RetractMinMaxDispatcher(), this);
    }
}

} // namespace starrocks::vectorized
