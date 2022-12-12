// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/aggregate/stream_aggregate_operator.h"

#include <variant>

#include "exec/exec_node.h"

namespace starrocks::stream {

bool StreamAggregateOperator::has_output() const {
    return _has_output;
}

bool StreamAggregateOperator::is_finished() const {
    return _is_finished || _aggregator->is_finished();
}

Status StreamAggregateOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    _aggregator->sink_complete();
    return Status::OK();
}

Status StreamAggregateOperator::set_finished(RuntimeState* state) {
    return _aggregator->set_finished();
}

Status StreamAggregateOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get(), _mem_tracker.get()));
    return _aggregator->open(state);
}

void StreamAggregateOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    Operator::close(state);
}

Status StreamAggregateOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (BarrierChunkConverter::is_barrier_chunk(chunk)) {
        VLOG_ROW << "process input barrier chunk.";
        _has_output = true;
        _barrier_chunk = std::move(chunk);
    } else {
        _has_output = false;
        VLOG_ROW << "process input chunk:" << chunk->debug_string();
        RETURN_IF_ERROR(_aggregator->process_chunk(dynamic_cast<StreamChunk*>(chunk.get())));
    }
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> StreamAggregateOperator::pull_chunk(RuntimeState* state) {
    DCHECK(!_aggregator->is_none_group_by_exprs());
    RETURN_IF_CANCELLED(state);
    _has_output = !_aggregator->is_ht_eos();
    VLOG_ROW << "process pull chunk, has_output:" << _has_output;
    if (!_has_output) {
        // need reset state
        RETURN_IF_ERROR(_aggregator->reset_state(state));
        return std::move(_barrier_chunk);
    } else {
        const auto chunk_size = state->chunk_size();
        StreamChunkPtr chunk = std::make_shared<vectorized::StreamChunk>();
        RETURN_IF_ERROR(_aggregator->output_changes(chunk_size, &chunk));

        // For having
        RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_aggregator->conjunct_ctxs(), chunk.get()));
        DCHECK_CHUNK(chunk);
        return std::move(chunk);
    }
}

} // namespace starrocks::stream
