// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/aggregate/stream_aggregate_operator.h"

#include <variant>

#include "exec/exec_node.h"

namespace starrocks::stream {

bool StreamAggregateOperator::has_output() const {
    return is_epoch_finished();
}

bool StreamAggregateOperator::is_finished() const {
    return _aggregator->is_sink_complete() && _aggregator->is_ht_eos();
}

Status StreamAggregateOperator::set_finished(RuntimeState* state) {
    return _aggregator->set_finished();
}

void StreamAggregateOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    Operator::close(state);
}

Status StreamAggregateOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (!chunk) {
        return Status::OK();
    }
    if (typeid(*chunk) == typeid(BarrierChunk)) {
        _is_epoch_finished = true;
    } else {
        DCHECK(typeid(*chunk) == typeid(StreamChunk));
        RETURN_IF_ERROR(_aggregator->process_chunk(dynamic_cast<StreamChunk*>(chunk.get())));
        _is_epoch_finished = false;
    }
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> StreamAggregateOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);

    DCHECK(!_aggregator->is_none_group_by_exprs());
    const auto chunk_size = state->chunk_size();
    StreamChunkPtr chunk = std::make_shared<vectorized::StreamChunk>();
    RETURN_IF_ERROR(_aggregator->output_changes(chunk_size, &chunk));

    // For having
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_aggregator->conjunct_ctxs(), chunk.get()));
    DCHECK_CHUNK(chunk);
    return std::move(chunk);
}

} // namespace starrocks::stream
