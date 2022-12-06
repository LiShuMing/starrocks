// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <gtest/gtest.h>

#include <chrono>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_sink_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/stream/scan/stream_source_operator.h"
#include "exec/stream/scan/trigger.h"
#include "gen_cpp/InternalService_types.h"
#include "gtest/gtest.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/storage_engine.h"

namespace starrocks::stream {

using InitiliazeFunc = std::function<Status()>;
using PrepareFunc = std::function<Status()>;

class StreamPipelineTest {
public:
    Status StartMV(InitiliazeFunc&& init_func) {
        RETURN_IF_ERROR(init_func());
        RETURN_IF_ERROR(PreparePipeline());
        RETURN_IF_ERROR(ExecutePipeline());
        return Status::OK();
    }
    Status PreparePipeline();
    Status ExecutePipeline();
    void StopMV();
    void CancelMV();

    Status StartEpoch(EpochInfo epoch_info);
    Status WaitUntilEpochEnd(EpochInfo epoch_info);

    size_t next_operator_id() { return ++_next_operator_id; }
    size_t next_plan_node_id() { return ++_next_plan_node_id; }
    uint32_t next_pipeline_id() { return ++_next_pipeline_id; }
    TUniqueId next_unique_id() {
        TUniqueId next;
        next.__set_hi(_next_unique_id++);
        next.__set_lo(_next_unique_id++);
        _unique_ids.emplace_back(std::move(next));
        return _unique_ids.back();
    }

protected:
    OpFactories maybe_interpolate_local_passthrough_exchange(OpFactories& pred_operators);

    ExecEnv* _exec_env = nullptr;
    pipeline::QueryContext* _query_ctx = nullptr;
    pipeline::FragmentContext* _fragment_ctx = nullptr;
    pipeline::FragmentFuture _fragment_future;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool* _obj_pool = nullptr;
    TExecPlanFragmentParams _request;
    // lambda used to init _pipelines
    std::function<void(RuntimeState*)> _pipeline_builder;
    pipeline::Pipelines _pipelines;
    size_t _next_operator_id = 0;
    size_t _next_plan_node_id = 0;
    uint32_t _next_pipeline_id = 0;
    int64_t _next_unique_id = 0;
    std::vector<TUniqueId> _unique_ids;
};

} // namespace starrocks::stream