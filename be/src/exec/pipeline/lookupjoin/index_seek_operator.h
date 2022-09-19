// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/operator.h"
#include "storage/rowset/segment_options.h"

namespace starrocks::pipeline {

// IndexSeekOperator
class IndexSeekOperator final : public Operator {
public:
    IndexSeekOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int32_t driver_sequence);

    ~IndexSeekOperator() override = default;

    void close(RuntimeState* state) override;

    bool has_output() const override { return true; }

    bool need_input() const override { return !is_finished(); }

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    SegmentReadOptions _read_options;
    vectorized::ChunkPtr _cur_chunk = nullptr;
    // TODO(lism): Use another iterator when Storage supports a better API.
    ChunkIteratorPtr _segmnt_iterator;
};

class IndexSeekOperatorFactory final : public OperatorFactory {
public:
    IndexSeekOperatorFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "lookupjoin_build", plan_node_id) {}

    ~IndexSeekOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<IndexSeekOperator>(this, _id, _plan_node_id, driver_sequence);
    }

private:
};

} // namespace starrocks::pipeline
