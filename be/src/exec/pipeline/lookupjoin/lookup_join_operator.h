// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {

// LookupJoinOperator
class LookupJoinOperator final : public Operator {
public:
    LookupJoinOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int32_t driver_sequence)
            : Operator(factory, id, "nestloop_join_build", plan_node_id, driver_sequence) {
    }

    ~LookupJoinOperator() override = default;

    void close(RuntimeState* state) override;

    bool has_output() const override { return true; }

    bool need_input() const override { return !is_finished(); }

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
};

class LookupJoinOperatorFactory final : public OperatorFactory {
public:
    LookupJoinOperatorFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "lookupjoin_build", plan_node_id) {}

    ~LookupJoinOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LookupJoinOperator>(this, _id, _plan_node_id, driver_sequence);
    }

private:
};

} // namespace starrocks::pipeline
