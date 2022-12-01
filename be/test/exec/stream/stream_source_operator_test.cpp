// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/stream_source_operator_test.h"

#include <random>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/stream/stream_fdw.h"
#include "exec/stream/stream_pipeline_test.h"

namespace starrocks::stream {

bool StreamGenerateSeriesSourceOperator::has_output() const {
    // TODO: exists new binlog
    return _start_epoch;
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
    // TODO: maybe we can use processed time?
    auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    _start_epoch = now > _epoch_deadline;
    return _start_epoch;
}

StatusOr<vectorized::ChunkPtr> StreamGenerateSeriesSourceOperator::pull_chunk(starrocks::RuntimeState* state) {
    if (is_epoch_finished()) {
        // generate barrier
        return std::make_shared<vectorized::BarrierChunk>(1);
    } else {
        // generate chunk
        auto column = vectorized::Int64Column::create();
        auto ops = vectorized::UInt8Column::create();

        for (int64_t i = 0; i < CHUNK_SIZE; i++) {
            _start += _step;
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
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    _pipeline_builder = [&](RuntimeState* state) {
        OpFactories op_factories;
        op_factories.push_back(std::make_shared<StreamGenerateSeriesSourceOperatorFactory>(
                next_operator_id(), "stream_generate_series_source_op", next_plan_node_id(), 0, 1));
        _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
    };

    TestPipeline();
}

} // namespace starrocks::stream