// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/scan/stream_source_operator.h"

namespace starrocks::stream {

StreamSourceOperator::StreamSourceOperator(OperatorFactory* factory, int32_t id, const std::string& name,
                                           int32_t plan_node_id, int32_t driver_sequence)
        : pipeline::SourceOperator(factory, id, name, plan_node_id, driver_sequence) {}
} // namespace starrocks::stream