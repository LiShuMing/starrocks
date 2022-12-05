// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/stream_pipeline.h"

#include <gtest/gtest.h>

#include <vector>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "runtime/exec_env.h"
#include "testutil/desc_tbl_helper.h"

namespace starrocks::stream {

Status StreamPipelineTest::_prepare() {
    _exec_env = ExecEnv::GetInstance();

    const auto& params = _request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_id = params.fragment_instance_id;

    _query_ctx = _exec_env->query_context_mgr()->get_or_register(query_id);
    _query_ctx->set_total_fragments(1);
    _query_ctx->set_delivery_expire_seconds(600);
    _query_ctx->set_query_expire_seconds(600);
    _query_ctx->extend_delivery_lifetime();
    _query_ctx->extend_query_lifetime();
    _query_ctx->init_mem_tracker(_exec_env->query_pool_mem_tracker()->limit(), _exec_env->query_pool_mem_tracker());

    _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_id);
    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(_request.params.query_id, _request.params.fragment_instance_id,
                                           _request.query_options, _request.query_globals, _exec_env));

    _fragment_future = _fragment_ctx->finish_future();
    _runtime_state = _fragment_ctx->runtime_state();

    _runtime_state->set_chunk_size(config::vector_chunk_size);
    _runtime_state->init_mem_trackers(_query_ctx->mem_tracker());
    _runtime_state->set_be_number(_request.backend_num);

    _obj_pool = _runtime_state->obj_pool();

    if (_pipeline_builder == nullptr) {
        return Status::Corruption("Non PipelineBuilder.");
    }

    _pipelines.clear();
    _pipeline_builder(_fragment_ctx->runtime_state());
    _fragment_ctx->set_pipelines(std::move(_pipelines));
    RETURN_IF_ERROR(_fragment_ctx->prepare_all_pipelines());

    pipeline::Drivers drivers;
    const auto& pipelines = _fragment_ctx->pipelines();
    const size_t num_pipelines = pipelines.size();
    for (auto n = 0; n < num_pipelines; ++n) {
        const auto& pipeline = pipelines[n];
        const auto degree_of_parallelism = pipeline->source_operator_factory()->degree_of_parallelism();

        LOG(INFO) << "Pipeline " << pipeline->to_readable_string() << " parallel=" << degree_of_parallelism
                  << " fragment_instance_id=" << print_id(params.fragment_instance_id);

        DCHECK(!pipeline->source_operator_factory()->with_morsels());

        for (size_t i = 0; i < degree_of_parallelism; ++i) {
            auto&& operators = pipeline->create_operators(degree_of_parallelism, i);
            pipeline::DriverPtr driver =
                    std::make_shared<pipeline::PipelineDriver>(std::move(operators), _query_ctx, _fragment_ctx, i);
            drivers.emplace_back(driver);
        }
    }

    _fragment_ctx->set_drivers(std::move(drivers));
    return Status::OK();
}

Status StreamPipelineTest::_execute() {
    for (const auto& driver : _fragment_ctx->drivers()) {
        RETURN_IF_ERROR(driver->prepare(_fragment_ctx->runtime_state()));
    }
    for (const auto& driver : _fragment_ctx->drivers()) {
        _exec_env->driver_executor()->submit(driver.get());
    }
    return Status::OK();
}

OpFactories StreamPipelineTest::maybe_interpolate_local_passthrough_exchange(OpFactories& pred_operators) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());
    auto* source_operator = down_cast<SourceOperatorFactory*>(pred_operators[0].get());
    if (source_operator->degree_of_parallelism() > 1) {
        auto pseudo_plan_node_id = -200;
        auto mem_mgr = std::make_shared<pipeline::LocalExchangeMemoryManager>(config::vector_chunk_size);
        auto local_exchange_source = std::make_shared<pipeline::LocalExchangeSourceOperatorFactory>(
                next_operator_id(), pseudo_plan_node_id, mem_mgr);
        auto local_exchange = std::make_shared<pipeline::PassthroughExchanger>(mem_mgr, local_exchange_source.get());
        auto local_exchange_sink = std::make_shared<pipeline::LocalExchangeSinkOperatorFactory>(
                next_operator_id(), pseudo_plan_node_id, local_exchange);
        // Add LocalExchangeSinkOperator to predecessor pipeline.
        pred_operators.emplace_back(std::move(local_exchange_sink));
        // predecessor pipeline comes to end.
        _pipelines.emplace_back(std::make_unique<pipeline::Pipeline>(next_pipeline_id(), pred_operators));

        OpFactories operators_source_with_local_exchange;
        // Multiple LocalChangeSinkOperators pipe into one LocalChangeSourceOperator.
        local_exchange_source->set_degree_of_parallelism(1);
        // A new pipeline is created, LocalExchangeSourceOperator is added as the head of the pipeline.
        operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));
        return operators_source_with_local_exchange;
    } else {
        return pred_operators;
    }
}

void StreamPipelineTest::StopMV() {
    _fragment_ctx->cancel(Status::OK());
}

Status StreamPipelineTest::StartEpoch(EpochInfo epoch_info) {
    auto drivers = _fragment_ctx->drivers();
    for (auto& driver : drivers) {
        auto* scan_op = driver->first_operator();
        if (auto* stream_source_op = dynamic_cast<StreamSourceOperator*>(scan_op); stream_source_op != nullptr) {
            stream_source_op->start_epoch(epoch_info);
        }
    }
    return Status::OK();
}

Status StreamPipelineTest::WaitUntilEpochEnd(EpochInfo epoch_info) {
    auto drivers = _fragment_ctx->drivers();
    for (auto& driver : drivers) {
        auto* sink_op = driver->last_operator();
        if (auto* stream_sink_op = dynamic_cast<StreamSinkOperator*>(sink_op); stream_sink_op != nullptr) {
            bool is_stream_barrier = stream_sink_op->is_stream_barrier();
            while (!is_stream_barrier) {
                sleep(0.01);
                is_stream_barrier = stream_sink_op->is_stream_barrier();
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks::stream
