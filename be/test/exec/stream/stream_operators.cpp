// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/stream_operators.h"

#include <random>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/stream/stream_fdw.h"
#include "exec/stream/stream_pipeline.h"

namespace starrocks::stream {

bool TestStreamSourceOperator::has_output() const {
    return _has_output;
}

void TestStreamSourceOperator::start_epoch(const EpochInfo& epoch_info) {
    std::lock_guard<std::mutex> lock(_start_epoch_lock);
    VLOG_ROW << ">>>>>>>>>>>> start epoch: " << epoch_info.debug_string()
             << ", is_epoch_finished:" << _is_epoch_finished;
    DCHECK(_is_epoch_finished);
    _has_output = true;
    _is_epoch_finished.store(false);
    _epoch_info = epoch_info;
}

CommitOffset TestStreamSourceOperator::get_latest_offset() {
    return CommitOffset{_epoch_info.epoch_id, _param.start};
}

void TestStreamSourceOperator::update_epoch_state() {
    DCHECK(!_is_epoch_finished);

    std::lock_guard<std::mutex> lock(_start_epoch_lock);
    switch (_epoch_info.trigger_mode) {
    case TriggerMode::kManualTrigger: {
        _is_epoch_finished.store(true);
        break;
    }
    case TriggerMode::kProcessTimeTrigger: {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        _is_epoch_finished.store(now > 0);
        break;
    }
    default:
        VLOG_ROW << "Unsupported trigger_mode: " + std::to_string((int)(_epoch_info.trigger_mode));
    }
    VLOG_ROW << "_is_epoch_finished:" << _is_epoch_finished;
}

StatusOr<vectorized::ChunkPtr> TestStreamSourceOperator::pull_chunk(starrocks::RuntimeState* state) {
    VLOG_ROW << "[TestStreamSourceOperator] pull_chunk, is_epoch_finished:" << _is_epoch_finished;
    if (_is_epoch_finished) {
        _has_output = false;
        return BarrierChunkConverter::make_barrier_chunk(_epoch_info);
    } else {
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
}

} // namespace starrocks::stream