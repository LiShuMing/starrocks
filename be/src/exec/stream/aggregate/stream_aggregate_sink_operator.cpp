// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/stream/aggregate/stream_aggregate_sink_operator.h"

#include "exec/exec_node.h"

namespace starrocks::stream {

Status StreamAggregateSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get(), _mem_tracker.get()));
    return _aggregator->open(state);
}

bool StreamAggregateSinkOperator::is_finished() const {
    return _is_input_finished;
}

Status StreamAggregateSinkOperator::set_finishing(RuntimeState* state) {
    _is_input_finished = true;
    _aggregator->sink_complete();
    return Status::OK();
}

Status StreamAggregateSinkOperator::set_finished(RuntimeState* state) {
    return _aggregator->set_finished();
}

bool StreamAggregateSinkOperator::is_epoch_finished() const {
    return _is_epoch_finished;
}

Status StreamAggregateSinkOperator::set_epoch_finishing(RuntimeState* state) {
    _is_epoch_finished = true;
    return Status::OK();
}

Status StreamAggregateSinkOperator::reset_epoch(RuntimeState* state) {
    _is_epoch_finished = false;
    return Status::OK();
}

StatusOr<ChunkPtr> StreamAggregateSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status StreamAggregateSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    VLOG_ROW << "push_chunk:" << chunk->num_rows();
    return _aggregator->process_chunk(dynamic_cast<StreamChunk*>(chunk.get()));
}

void StreamAggregateSinkOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    Operator::close(state);
}

} // namespace starrocks::stream
