// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/lookupjoin/lookup_join_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/operator.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

void LookupJoinOperator::close(RuntimeState* state) {
    auto build_rows = ADD_COUNTER(_unique_metrics, "BuildRows", TUnit::UNIT);
    auto build_chunks = ADD_COUNTER(_unique_metrics, "BuildChunks", TUnit::UNIT);
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> LookupJoinOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from cross join right sink operator");
}

Status LookupJoinOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

Status LookupJoinOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return Status::OK();
}

} // namespace starrocks::pipeline
