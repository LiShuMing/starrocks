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

    // refresh new deadline
    _curren_epoch = epoch;
    _epoch_deadline = now + milliseconds(epoch.max_binlog_ms).count();
}

void StreamGenerateSeriesSourceOperator::stop_epoch(const EpochInfo& epoch) {}

CommitOffset StreamGenerateSeriesSourceOperator::get_latest_offset() {
    return CommitOffset{_epoch_id, _start};
}

bool StreamGenerateSeriesSourceOperator::is_epoch_finished() {
    //    // TODO: maybe we can use processed time?
    //    auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    //    _start_epoch = now > _epoch_deadline;
    if (_start > 100) {
        return true;
    }
    return false;
}

StatusOr<vectorized::ChunkPtr> StreamGenerateSeriesSourceOperator::pull_chunk(starrocks::RuntimeState* state) {
    VLOG_ROW << "[StreamGenerateSeriesSourceOperator] pull_chunk";
    if (is_epoch_finished()) {
        // generate barrier
        _start_epoch.store(false);
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
        return std::make_shared<StreamChunk>(std::move(chunk), std::move(ops));
    }
}

class StreamSourceOperatorTest : public StreamPipelineTest {};

TEST_F(StreamSourceOperatorTest, TEST1) {
    DCHECK_IF_ERROR(StartMV([&]() {
        _pipeline_builder = [&](RuntimeState* state) {
            OpFactories op_factories{
                    std::make_shared<StreamGenerateSeriesSourceOperatorFactory>(next_operator_id(), next_plan_node_id(),
                                                                                0, 1),
                    std::make_shared<StreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()),
            };
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    sleep(3);

    StopMV();
}

} // namespace starrocks::stream