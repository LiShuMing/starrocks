// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/lookupjoin/index_seek_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/operator.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

IndexSeekOperator::IndexSeekOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int32_t driver_sequence)
        : Operator(factory, id, "nestloop_join_build", plan_node_id, driver_sequence) {
    _segment_iterator = vectorized::new_segment_iterator(shared_from_this(), schema, _read_options);
}
void IndexSeekOperator::close(RuntimeState* state) {
    auto build_rows = ADD_COUNTER(_unique_metrics, "BuildRows", TUnit::UNIT);
    auto build_chunks = ADD_COUNTER(_unique_metrics, "BuildChunks", TUnit::UNIT);
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> IndexSeekOperator::pull_chunk(RuntimeState* state) {
    return std::move(_cur_chunk);
}
_check_and_resolve_conflict
Status IndexSeekOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

Status IndexSeekOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return Status::OK();
}

} // namespace starrocks::pipeline
