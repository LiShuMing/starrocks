// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/stream/aggregate/stream_aggregator.h"
#include "exec/stream/stream_fdw.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::stream {
using StreamAggregatorPtr = std::shared_ptr<StreamAggregator>;
using StreamAggregatorFactory = AggregatorFactoryBase<StreamAggregator>;
using StreamAggregatorFactoryPtr = std::shared_ptr<StreamAggregatorFactory>;

class StreamAggregateOperator : public Operator {
public:
    StreamAggregateOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                            StreamAggregatorPtr aggregator)
            : Operator(factory, id, "stream_aggregate", plan_node_id, driver_sequence),
              _aggregator(std::move(aggregator)) {
        _aggregator->ref();
    }

    ~StreamAggregateOperator() override = default;

    bool has_output() const override;
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    Status prepare(RuntimeState* state);
    void close(RuntimeState* state) override;

private:
    StreamAggregatorPtr _aggregator = nullptr;
    ChunkPtr _barrier_chunk;
    // Whether prev operator has no output
    bool _is_finished = false;
    bool _has_output = false;
};

class StreamAggregateOperatorFactory final : public OperatorFactory {
public:
    StreamAggregateOperatorFactory(int32_t id, int32_t plan_node_id, StreamAggregatorFactoryPtr aggregator_factory)
            : OperatorFactory(id, "stream_aggregate", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    // used for testing
    StreamAggregateOperatorFactory(int32_t id, int32_t plan_node_id, StreamAggregatorPtr aggregator)
            : OperatorFactory(id, "stream_aggregate", plan_node_id), _aggregator(std::move(aggregator)) {}

    ~StreamAggregateOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        if (_aggregator) {
            return std::make_shared<StreamAggregateOperator>(this, _id, _plan_node_id, driver_sequence, _aggregator);
        } else {
            return std::make_shared<StreamAggregateOperator>(this, _id, _plan_node_id, driver_sequence,
                                                             _aggregator_factory->get_or_create(driver_sequence));
        }
    }

private:
    StreamAggregatorFactoryPtr _aggregator_factory = nullptr;
    StreamAggregatorPtr _aggregator = nullptr;
};
} // namespace starrocks::stream
