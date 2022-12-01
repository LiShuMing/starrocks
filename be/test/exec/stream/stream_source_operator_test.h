// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "column/barrier_chunk.h"
#include "exec/stream/scan/stream_source_operator.h"

namespace starrocks::stream {

class StreamGenerateSeriesSourceOperator final : public StreamSourceOperator {
public:
    StreamGenerateSeriesSourceOperator(pipeline::OperatorFactory* factory, int32_t id, const std::string& name,
                                       int32_t plan_node_id, int32_t driver_sequence, int64_t start, int64_t step)
            : StreamSourceOperator(factory, id, name, plan_node_id, driver_sequence), _start(start), _step(step) {}

    ~StreamGenerateSeriesSourceOperator() override = default;

    bool has_output() const override;

    void start_epoch(const EpochInfo& epoch) override;
    bool is_epoch_finished() override;
    void stop_epoch(const EpochInfo& epoch) override;
    CommitOffset get_latest_offset() override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(starrocks::RuntimeState* state) override;
    Status set_finishing(starrocks::RuntimeState* state) override { return Status::OK(); }
    Status set_finished(starrocks::RuntimeState* state) override { return Status::OK(); };

private:
    static constexpr auto CHUNK_SIZE = 1;

    int64_t _start;
    int64_t _step;
    int64_t _epoch_id{0};
};

class StreamGenerateSeriesSourceOperatorFactory final : public pipeline::SourceOperatorFactory {
public:
    StreamGenerateSeriesSourceOperatorFactory(int32_t id, const std::string& name, int32_t plan_node_id, int64_t start,
                                              int64_t step)
            : SourceOperatorFactory(id, name, plan_node_id), _start(start), _step(step) {}
    ~StreamGenerateSeriesSourceOperatorFactory() override = default;
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<StreamGenerateSeriesSourceOperator>(this, _id, _name, _plan_node_id, driver_sequence,
                                                                    _start, _step);
    }

private:
    int64_t _start;
    int64_t _step;
};

} // namespace starrocks::stream
