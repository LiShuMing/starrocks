// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <chrono>

#include "column/chunk.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/source_operator.h"
#include "exec/stream/stream_fdw.h"

namespace starrocks::stream {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;

struct CommitOffset {
    // epoch mark id
    int64_t epoch_id;

    // source operator commit offset
    int64_t latest_offset;

    // TODO: source operator id??
};

class StreamSourceOperator : public pipeline::SourceOperator {
public:
    StreamSourceOperator(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id,
                         int32_t driver_sequence);

    ~StreamSourceOperator() override = default;

    // never finished
    bool is_finished() const override { return _is_finished; }
    Status set_finished(starrocks::RuntimeState* state) override {
        _is_finished.store(true);
        return Status::OK();
    };

    virtual void start_epoch(const EpochInfo& epoch) = 0;
    virtual CommitOffset get_latest_offset() = 0;

protected:
    std::atomic_bool _is_epoch_finished{true};
    std::atomic_bool _is_finished{false};
    bool _has_output{true};
    EpochInfo _epoch_info;
};

class StreamSinkOperator : public pipeline::Operator {
public:
    StreamSinkOperator(OperatorFactory* factory, int32_t id, std::string name, int32_t plan_node_id,
                       int32_t driver_sequence)
            : Operator(factory, id, name, plan_node_id, driver_sequence) {}

    bool is_epoch_finished() const override { return _is_epoch_finished.load(); }
    bool is_epoch_finished(const EpochInfo& epoch_info) const { return _epoch_info.epoch_id >= epoch_info.epoch_id; }

protected:
    EpochInfo _epoch_info;
    std::atomic_bool _is_epoch_finished = false;
    std::atomic_bool _is_finished = false;
};

} // namespace starrocks::stream