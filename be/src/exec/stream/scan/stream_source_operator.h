// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <chrono>

#include "column/chunk.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/source_operator.h"
#include "exec/stream/scan/trigger.h"

namespace starrocks::stream {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;

struct EpochInfo {
    // epoch marker id
    int64_t epoch_id;
    // last lsn offset
    int64_t last_lsn_offset;
    // max binlog duration which this epoch will run
    int64_t max_binlog_ms;
    // max binlog offset which this epoch will run
    int64_t max_offsets;
    TriggerMode trigger_mode;

    std::string debug_string() const {
        std::stringstream ss;
        ss << "epoch_id=" << epoch_id << ", last_lsn_offset=" << last_lsn_offset << ", max_binlog_ms=" << max_binlog_ms
           << ", max_offsets=" << max_offsets << ", trigger_mode=" << (int)(trigger_mode);
        return ss.str();
    }
};

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

    // Start/End epoch implement
    Status set_epoch_finished(starrocks::RuntimeState* state) override { return Status::OK(); }
    Status set_epoch_finishing(starrocks::RuntimeState* state) override { return Status::OK(); }

    virtual void start_epoch(const EpochInfo& epoch) = 0;
    virtual CommitOffset get_latest_offset() = 0;

protected:
    std::atomic_bool _is_epoch_finished{true};
    std::atomic_bool _is_finished{false};
    EpochInfo _curren_epoch;
};

class StreamSinkOperator : public pipeline::Operator {
public:
    StreamSinkOperator(OperatorFactory* factory, int32_t id, std::string name, int32_t plan_node_id,
                       int32_t driver_sequence)
            : Operator(factory, id, name, plan_node_id, driver_sequence) {}

    bool is_epoch_finished() const override { return _is_epoch_finished.load(); }
    void reset_epoch_finished() { _is_epoch_finished.store(false); }

protected:
    std::atomic_bool _is_epoch_finished = false;
};

} // namespace starrocks::stream