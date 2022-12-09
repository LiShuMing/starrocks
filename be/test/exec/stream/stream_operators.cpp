// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/stream_operators.h"

#include <random>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/stream/stream_fdw.h"
#include "exec/stream/stream_pipeline.h"

namespace starrocks::stream {

bool TestStreamSourceOperator::has_output() const {
    return !_is_epoch_finished.load();
}

void TestStreamSourceOperator::start_epoch(const EpochInfo& epoch) {
    VLOG_ROW << ">>>>>>>>>>>> start epoch: " << epoch.debug_string() << ", is_epoch_finished:" << _is_epoch_finished;
    DCHECK(_is_epoch_finished);
    _is_epoch_finished.store(false);
    _trigger_mode = epoch.trigger_mode;
    _curren_epoch = epoch;
}

CommitOffset TestStreamSourceOperator::get_latest_offset() {
    return CommitOffset{_epoch_id, _param.start};
}

bool TestStreamSourceOperator::is_epoch_finished() const {
    return _is_epoch_finished;
}

void TestStreamSourceOperator::update_epoch_state() {
    switch (_trigger_mode) {
    case TriggerMode::kManualTrigger: {
        _is_epoch_finished.store((_processed_chunks + 1) % 2 == 0);
        break;
    }
    case TriggerMode::kProcessTimeTrigger: {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        _is_epoch_finished.store(now > 0);
        break;
    }
    default:
        VLOG_ROW << "Unsupported trigger_mode: " + std::to_string((int)_trigger_mode);
    }
    VLOG_ROW << "_is_epoch_finished:" << _is_epoch_finished;
}

StatusOr<vectorized::ChunkPtr> TestStreamSourceOperator::pull_chunk(starrocks::RuntimeState* state) {
    VLOG_ROW << "[TestStreamSourceOperator] pull_chunk";
    auto chunk = std::make_shared<Chunk>();
    for (auto idx = 0; idx < _param.num_column; idx++) {
        auto column = vectorized::Int64Column::create();
        for (int64_t i = 0; i < _param.chunk_size; i++) {
            _param.start += _param.step;
            VLOG_ROW << "Append col:" << idx << ", row:" << _param.start;
            column->append(_param.start % _param.ndv_count);
        }
        chunk->append_column(column, SlotId(idx));
    }

    // ops
    auto ops = vectorized::UInt8Column::create();
    for (int64_t i = 0; i < _param.chunk_size; i++) {
        ops->append(0);
    }
    _processed_chunks += 1;

    // update epoch state
    update_epoch_state();

    return StreamChunkConverter::make_stream_chunk(std::move(chunk), std::move(ops));
}

} // namespace starrocks::stream