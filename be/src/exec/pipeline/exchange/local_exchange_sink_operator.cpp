// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/pipeline/exchange/local_exchange_sink_operator.h"

#include "column/barrier_chunk.h"
#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status LocalExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _exchanger->increment_sink_number();
    _unique_metrics->add_info_string("ShuffleNum", std::to_string(_exchanger->source_dop()));
    return Status::OK();
}

bool LocalExchangeSinkOperator::need_input() const {
    return !_is_finished && _exchanger->need_input();
}

StatusOr<vectorized::ChunkPtr> LocalExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't call pull_chunk from local exchange sink.");
}

Status LocalExchangeSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    _exchanger->finish(state);
    return Status::OK();
}

Status LocalExchangeSinkOperator::set_epoch_finishing(RuntimeState* state) {
    _is_epoch_finished = true;
    _exchanger->epoch_finish(state, nullptr);
    return Status::OK();
}

Status LocalExchangeSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (vectorized::BarrierChunkConverter::is_barrier_chunk(chunk)) {
        VLOG_ROW << "LocalExchangeSinkOperator is_barrier_chunk:"
                 << vectorized::BarrierChunkConverter::get_barrier_info(chunk).debug_string();

        _exchanger->epoch_finish(state, chunk);
        return Status::OK();
    }
    VLOG_ROW << "LocalExchangeSinkOperator chunk:" << chunk->debug_string();
    _is_epoch_finished = false;
    return _exchanger->accept(chunk, _driver_sequence);
}

} // namespace starrocks::pipeline
