// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/stream_operators.h"

#include <random>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/stream/aggregate/stream_aggregate_operator.h"
#include "exec/stream/stream_fdw.h"
#include "exec/stream/stream_pipeline.h"
#include "exec/stream/stream_test.h"

namespace starrocks::stream {

class StreamOperatorsTest : public StreamPipelineTest, public StreamTestBase {
protected:
    DescriptorTbl* _tbl;
    std::vector<std::vector<SlotTypeInfo>> _slot_infos;
    std::vector<GroupByKeyInfo> _group_by_infos;
    std::vector<AggInfo> _agg_infos;
    std::shared_ptr<StreamAggregator> _stream_aggregator;
};

TEST_F(StreamOperatorsTest, Dop_1) {
    DCHECK_IF_ERROR(StartMV([&]() {
        _pipeline_builder = [&](RuntimeState* state) {
            OpFactories op_factories{
                    std::make_shared<TestStreamSourceOperatorFactory>(
                            next_operator_id(), next_plan_node_id(),
                            TestStreamSourceParam{.num_column = 2, .start = 0, .step = 1, .chunk_size = 4}),
                    std::make_shared<TestStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()),
            };
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    EpochInfo epoch_info{.epoch_id = 0, .trigger_mode = TriggerMode::kManualTrigger};
    DCHECK_IF_ERROR(StartEpoch(epoch_info));
    DCHECK_IF_ERROR(WaitUntilEpochEnd(epoch_info));

    StopMV();
}

TEST_F(StreamOperatorsTest, MultiDop) {
    DCHECK_IF_ERROR(StartMV([&]() {
        _pipeline_builder = [&](RuntimeState* state) {
            OpFactories op_factories;
            auto source_factory = std::make_shared<TestStreamSourceOperatorFactory>(
                    next_operator_id(), next_plan_node_id(),
                    TestStreamSourceParam{.num_column = 2, .start = 0, .step = 1, .chunk_size = 4, .ndv_count = 4});
            source_factory->set_degree_of_parallelism(4);
            op_factories.emplace_back(std::move(source_factory));
            // add exchange node to gather multi source operator to one sink operator
            op_factories = maybe_interpolate_local_passthrough_exchange(op_factories);
            op_factories.emplace_back(
                    std::make_shared<TestStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()));
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    EpochInfo epoch_info{.epoch_id = 0, .trigger_mode = TriggerMode::kManualTrigger};
    DCHECK_IF_ERROR(StartEpoch(epoch_info));
    DCHECK_IF_ERROR(WaitUntilEpochEnd(epoch_info));

    StopMV();
}

TEST_F(StreamOperatorsTest, Test_StreamAggregator_Dop1) {
    DCHECK_IF_ERROR(StartMV([&]() {
        _pipeline_builder = [&](RuntimeState* state) {
            _slot_infos = std::vector<std::vector<SlotTypeInfo>>{
                    // input slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"col2", TYPE_BIGINT, false},
                    },
                    // intermediate slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"count_agg", TYPE_BIGINT, false},
                    },
                    // result slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"count_agg", TYPE_BIGINT, false},
                    },
            };
            _group_by_infos = {0};
            _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                              {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

            _tbl = GenerateDescTbl(_runtime_state, (*_obj_pool), _slot_infos);
            _runtime_state->set_desc_tbl(_tbl);
            _stream_aggregator = _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, true, 0);
            OpFactories op_factories{
                    std::make_shared<TestStreamSourceOperatorFactory>(
                            next_operator_id(), next_plan_node_id(),
                            TestStreamSourceParam{
                                    .num_column = 2, .start = 0, .step = 1, .chunk_size = 4, .ndv_count = 4}),
                    std::make_shared<StreamAggregateOperatorFactory>(next_operator_id(), next_plan_node_id(),
                                                                     _stream_aggregator),
                    std::make_shared<TestStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()),
            };
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    for (auto i = 0; i < 10; i++) {
        EpochInfo epoch_info{.epoch_id = i, .trigger_mode = TriggerMode::kManualTrigger};
        DCHECK_IF_ERROR(StartEpoch(epoch_info));
        DCHECK_IF_ERROR(WaitUntilEpochEnd(epoch_info));
    }
    StopMV();
}

TEST_F(StreamOperatorsTest, Test_StreamAggregator_MultiDop) {
    DCHECK_IF_ERROR(StartMV([&]() {
        _pipeline_builder = [&](RuntimeState* state) {
            _slot_infos = std::vector<std::vector<SlotTypeInfo>>{
                    // input slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"col2", TYPE_BIGINT, false},
                    },
                    // intermediate slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"count_agg", TYPE_BIGINT, false},
                    },
                    // result slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"count_agg", TYPE_BIGINT, false},
                    },
            };
            _group_by_infos = {0};
            _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                              {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

            _tbl = GenerateDescTbl(_runtime_state, (*_obj_pool), _slot_infos);
            _runtime_state->set_desc_tbl(_tbl);
            _stream_aggregator = _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, false, 0);
            OpFactories op_factories;
            auto source_factory = std::make_shared<TestStreamSourceOperatorFactory>(
                    next_operator_id(), next_plan_node_id(),
                    TestStreamSourceParam{.num_column = 2, .start = 0, .step = 1, .chunk_size = 4, .ndv_count = 8});
            source_factory->set_degree_of_parallelism(4);
            op_factories.emplace_back(std::move(source_factory));
            // add exchange node to gather multi source operator to one sink operator
            op_factories = maybe_interpolate_local_passthrough_exchange(op_factories);
            op_factories.emplace_back(std::make_shared<StreamAggregateOperatorFactory>(
                    next_operator_id(), next_plan_node_id(), _stream_aggregator));
            op_factories.emplace_back(
                    std::make_shared<TestStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()));
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    for (auto i = 0; i < 10; i++) {
        EpochInfo epoch_info{.epoch_id = i, .trigger_mode = TriggerMode::kManualTrigger};
        DCHECK_IF_ERROR(StartEpoch(epoch_info));
        DCHECK_IF_ERROR(WaitUntilEpochEnd(epoch_info));
    }
    StopMV();
}

} // namespace starrocks::stream