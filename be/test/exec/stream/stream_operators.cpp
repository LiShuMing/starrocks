// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/stream_operators.h"

#include <random>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/stream/stream_fdw.h"
#include "exec/stream/stream_pipeline.h"

namespace starrocks::stream {

bool TestStreamSourceOperator::has_output() const {
    VLOG_ROW << "[TestStreamSourceOperator] has_output=" << !_is_epoch_finished;
    // TODO: exists new binlog
    return !_is_epoch_finished.load();
}

void TestStreamSourceOperator::start_epoch(const EpochInfo& epoch) {
    VLOG_ROW << "start epoch: " << epoch.debug_string();
    DCHECK(_is_epoch_finished);
    _is_epoch_finished.store(false);
    auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    // start must be after epoch is stopped
    //    DCHECK_LT(_epoch_deadline, now);
    _trigger_mode = epoch.trigger_mode;
    // refresh new deadline
    _curren_epoch = epoch;
    //    _epoch_deadline = now + milliseconds(epoch.max_binlog_ms).count();
}

CommitOffset TestStreamSourceOperator::get_latest_offset() {
    return CommitOffset{_epoch_id, _param.start};
}

bool TestStreamSourceOperator::is_epoch_finished() {
    VLOG_ROW << "trigger_mode: " + std::to_string((int)_trigger_mode);
    switch (_trigger_mode) {
    case TriggerMode::kManualTrigger:
        return (_processed_chunks + 1) % 2 == 0;
    case TriggerMode::kProcessTimeTrigger: {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > 0;
        //        return now > _epoch_deadline;
    }
    default:
        VLOG_ROW << "Unsupported trigger_mode: " + std::to_string((int)_trigger_mode);
        return false;
    }
}

StatusOr<vectorized::ChunkPtr> TestStreamSourceOperator::pull_chunk(starrocks::RuntimeState* state) {
    bool is_epoch_finished = this->is_epoch_finished();
    VLOG_ROW << "[TestStreamSourceOperator] pull_chunk, is_epoch_finished:" << is_epoch_finished;
    if (is_epoch_finished) {
        // generate barrier
        _is_epoch_finished.store(true);
        _processed_chunks += 1;
        return std::make_shared<vectorized::BarrierChunk>(1);
    } else {
        _is_epoch_finished.store(false);
        // generate chunk
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
        return std::make_shared<StreamChunk>(std::move(chunk), std::move(ops));
    }
}

} // namespace starrocks::stream