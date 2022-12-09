// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once
#include <atomic>

#include "exec/pipeline/pipeline_driver.h"

namespace starrocks::pipeline {

class StreamPipelineDriver : public PipelineDriver {
public:
    StreamPipelineDriver(const Operators& operators, QueryContext* query_ctx, FragmentContext* fragment_ctx,
                         int32_t driver_id)
            : PipelineDriver(operators, query_ctx, fragment_ctx, driver_id) {}
    ~StreamPipelineDriver() override = default;

    StatusOr<DriverState> process(RuntimeState* runtime_state, int worker_id) override;

private:
    Status _mark_operator_epoch_finishing(OperatorPtr& op, RuntimeState* runtime_state);
    Status _mark_operator_epoch_finished(OperatorPtr& op, RuntimeState* runtime_state);

private:
    size_t _first_epoch_unfinished{0};
};

} // namespace starrocks::pipeline
