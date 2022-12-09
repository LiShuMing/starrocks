// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "column/barrier_chunk.h"
#include "exec/stream/scan/stream_source_operator.h"

namespace starrocks::stream {
struct TestStreamSourceParam {
    int64_t num_column;
    int64_t start;
    int64_t step;
    int64_t chunk_size;
    int64_t ndv_count{100};
};

class TestStreamSourceOperator final : public StreamSourceOperator {
public:
    TestStreamSourceOperator(pipeline::OperatorFactory* factory, int32_t id, const std::string& name,
                             int32_t plan_node_id, int32_t driver_sequence, TestStreamSourceParam param)
            : StreamSourceOperator(factory, id, name, plan_node_id, driver_sequence), _param(param) {}

    ~TestStreamSourceOperator() override = default;

    bool has_output() const override;

    void start_epoch(const EpochInfo& epoch) override;
    bool is_epoch_finished() const override;
    CommitOffset get_latest_offset() override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(starrocks::RuntimeState* state) override;

private:
    TestStreamSourceParam _param;
    int64_t _epoch_id{0};
    TriggerMode _trigger_mode{TriggerMode::kManualTrigger};
    int64_t _processed_chunks{0};
    ;
};

class TestStreamSourceOperatorFactory final : public pipeline::SourceOperatorFactory {
public:
    TestStreamSourceOperatorFactory(int32_t id, int32_t plan_node_id, TestStreamSourceParam param)
            : SourceOperatorFactory(id, "stream_source", plan_node_id), _param(param) {}
    ~TestStreamSourceOperatorFactory() override = default;
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestStreamSourceOperator>(this, _id, _name, _plan_node_id, driver_sequence, _param);
    }

private:
    TestStreamSourceParam _param;
};

class TestStreamSinkOperator final : public StreamSinkOperator {
public:
    TestStreamSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : StreamSinkOperator(factory, id, "stream_sink", plan_node_id, driver_sequence) {}

    ~TestStreamSinkOperator() override = default;

    bool has_output() const override { return !_is_finished && !_is_epoch_finished; }

    bool need_input() const override { return true; }

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::NotSupported("pull_chunk in StreamSinkOperator is not supported.");
    }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override {
        if (chunk) {
            if (typeid(*chunk) == typeid(vectorized::BarrierChunk)) {
                _is_epoch_finished.store(true);
            } else {
                _is_epoch_finished.store(false);
            }

            VLOG_ROW << "[StreamSinkOperator] result:" << chunk->debug_string()
                     << ", is_epoch_finished:" << _is_epoch_finished;

            std::cout << "Sink Result: " << chunk->debug_string() << std::endl;
        }
        this->_output_chunks.push_back(chunk);
        return Status::OK();
    }

private:
    bool _is_finished = false;
    // Result to be tested.
    std::vector<ChunkPtr> _output_chunks;
};

class TestStreamSinkOperatorFactory final : public OperatorFactory {
public:
    TestStreamSinkOperatorFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "stream_sink", plan_node_id) {}

    ~TestStreamSinkOperatorFactory() override = default;

    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestStreamSinkOperator>(this, _id, _plan_node_id, driver_sequence);
    }

private:
};

} // namespace starrocks::stream
