// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/stream_operator_test.h"

#include <random>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/stream/stream_fdw.h"
#include "exec/stream/stream_pipeline_test.h"

namespace starrocks::stream {

bool StreamGenerateSeriesSourceOperator::has_output() const {
    VLOG_ROW << "[StreamGenerateSeriesSourceOperator] has_output=" << _start_epoch;
    // TODO: exists new binlog
    return _start_epoch.load();
}

void StreamGenerateSeriesSourceOperator::start_epoch(const EpochInfo& epoch) {
    auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    // start must be after epoch is stopped
    DCHECK_LT(_epoch_deadline, now);
    _trigger_mode = epoch.trigger_mode;
    // refresh new deadline
    _curren_epoch = epoch;
    _epoch_deadline = now + milliseconds(epoch.max_binlog_ms).count();
}

void StreamGenerateSeriesSourceOperator::stop_epoch(const EpochInfo& epoch) {}

CommitOffset StreamGenerateSeriesSourceOperator::get_latest_offset() {
    return CommitOffset{_epoch_id, _start};
}

bool StreamGenerateSeriesSourceOperator::is_epoch_finished() {
    switch (_trigger_mode) {
    case TriggerMode::kManualTrigger:
        return (_processed_chunks + 1) % 2 == 0;
    case TriggerMode::kProcessTimeTrigger: {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _epoch_deadline;
    }
    default:
        throw std::runtime_error("Unsupported trigger_mode: " + std::to_string((int)_trigger_mode));
    }
}

StatusOr<vectorized::ChunkPtr> StreamGenerateSeriesSourceOperator::pull_chunk(starrocks::RuntimeState* state) {
    VLOG_ROW << "[StreamGenerateSeriesSourceOperator] pull_chunk";
    if (is_epoch_finished()) {
        // generate barrier
        _start_epoch.store(false);
        _processed_chunks += 1;
        return std::make_shared<vectorized::BarrierChunk>(1);
    } else {
        // generate chunk
        auto column = vectorized::Int64Column::create();
        auto ops = vectorized::UInt8Column::create();

        for (int64_t i = 0; i < CHUNK_SIZE; i++) {
            _start += _step;

            VLOG_ROW << "Append row:" << _start;
            column->append(_start);
            ops->append(0);
        }

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(column, SlotId(1));
        _processed_chunks += 1;
        return std::make_shared<StreamChunk>(std::move(chunk), std::move(ops));
    }
}

class StreamSourceOperatorTest : public StreamPipelineTest {};

TEST_F(StreamSourceOperatorTest, Dop_1) {
    DCHECK_IF_ERROR(StartMV([&]() {
        _pipeline_builder = [&](RuntimeState* state) {
            OpFactories op_factories{
                    std::make_shared<StreamGenerateSeriesSourceOperatorFactory>(next_operator_id(), next_plan_node_id(),
                                                                                0, 1),
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

TEST_F(StreamSourceOperatorTest, MultiDop) {
    DCHECK_IF_ERROR(StartMV([&]() {
        _pipeline_builder = [&](RuntimeState* state) {
            OpFactories op_factories;
            auto source_factory = std::make_shared<StreamGenerateSeriesSourceOperatorFactory>(
                    next_operator_id(), next_plan_node_id(), 0, 1);
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

} // namespace starrocks::stream