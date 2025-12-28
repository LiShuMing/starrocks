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

#include "exec/partition/partition_hash_topn.h"

#include <algorithm>

#include "exec/pipeline/sort/sort_context.h"
#include "exec/sorting/sort_helper.h"

namespace starrocks {

PartitionHashTopn::PartitionHashTopn(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>& is_asc_order,
                                     const std::vector<bool>& is_null_first, size_t offset, size_t limit)
        : _sort_exprs(sort_exprs),
          _sort_descs(is_asc_order, is_null_first),
          _limit(limit),
          _partition_heap(PartitionComparator(&_sort_descs, _partition_infos)) {
    _partition_sort_column_slot_ids.reserve(_sort_exprs->size());
    for (ExprContext* expr_ctx : *_sort_exprs) {
        auto* expr = expr_ctx->root();
        if (expr->is_slotref()) {
            _partition_sort_column_slot_ids.emplace_back(down_cast<ColumnRef*>(expr)->slot_id());
        } else {
            DCHECK(false) << "sort expr is not a slot ref";
        }
    }
}

bool PartitionHashTopn::is_valid(size_t partition_idx, const Columns& partition_columns, size_t row_idx) {
    if (partition_idx < _partition_infos.size()) {
        DCHECK_LT(partition_idx, _partition_infos.size());
        auto& partition_info = _partition_infos[partition_idx];
        if (partition_info.is_invalid()) {
            return false;
        } else {
            return true;
        }
    } else {
        // compare to the top row of the partition
        bool is_better_than_top = _compare_to_top_row(partition_columns, row_idx);
        // update the partition info
        _partition_infos.emplace_back();

        // copy the sort columns with row_idx row
        auto& new_partition_info = _partition_infos.back();
        if (is_better_than_top) {
            for (int i = 0; i < partition_columns.size(); i++) {
                auto column = partition_columns[i]->clone_empty();
                column->append(*partition_columns[i], row_idx, 1);
                new_partition_info.sort_columns.emplace_back(std::move(column));
            }
            new_partition_info.to_valid();

            // only push the new partition to the heap if it's better than the top partition
            _partition_heap.push(_partition_infos.size() - 1);
            return true;
        } else {
            new_partition_info.invalidate();
            return false;
        }
    }
}

Status PartitionHashTopn::new_partition(size_t partition_idx) {
    DCHECK_EQ(_partition_infos.size(), partition_idx);
    _partition_infos.emplace_back();
    return Status::OK();
}

Status PartitionHashTopn::offer(size_t partition_idx, const ChunkPtr& chunk) {
    VLOG_ROW << "PartitionHashTopn offer, chunk size: " << chunk->num_rows() << ", partition_idx: " << partition_idx
             << ", heap size: " << _partition_heap.size();
    if (chunk->num_rows() == 0) {
        return Status::OK();
    }
    for (auto i = 0; i < chunk->num_rows(); i++) {
        VLOG_ROW << "PartitionHashTopn offer, row: " << i << ", partition_idx: " << partition_idx
                 << ", row: " << chunk->debug_row(i);
    }

    // // Get or create partition info
    // DCHECK_LT(partition_idx, _partition_infos.size());
    // auto& partition_info = _partition_infos[partition_idx];
    // if (partition_info.is_invalid() || partition_info.is_valid()) {
    //     // the partition is not valid, no need to compare with the new row
    //     return Status::OK();
    // }
    // // NOTE: order by exprs are the same for partitions, so we can use the 1st row to compare with the new row
    // // for each partition, only use the 1st row to compare with the new row
    // // partition_info.chunks.emplace_back(chunk);
    // // RETURN_IF_ERROR(_evaluate_sort_columns(chunk, &partition_info.sort_columns));
    // partition_info.sort_columns = partition_columns;
    // VLOG_ROW << "PartitionHashTopn offer, partition_idx: " << partition_idx
    //          << ", sort_columns size: " << partition_info.sort_columns.size();
    // partition_info.to_valid();
    // _partition_heap.push(partition_idx);
    // Keep only top N partitions (by their best row)
    while (!_partition_heap.empty() && _partition_heap.size() > _limit) {
        size_t top_partition_idx = _partition_heap.top();
        _partition_heap.pop();
        VLOG_ROW << "PartitionHashTopn offer, pop partition_idx: " << top_partition_idx
                 << ", heap size: " << _partition_heap.size();
        // To invalidate data early, trigger to invalidate here.
        _partition_infos[top_partition_idx].invalidate();
    }
    return Status::OK();
}

Status PartitionHashTopn::done() {
    // Keep only top N partitions (by their best row)
    VLOG_ROW << "PartitionHashTopn done, heap size: " << _partition_heap.size() << ", limit: " << _limit;
    while (!_partition_heap.empty() && _partition_heap.size() > _limit) {
        size_t top_partition_idx = _partition_heap.top();
        auto& top_partition = _partition_infos[top_partition_idx];
        VLOG_ROW << "PartitionHashTopn done, pop partition_idx: " << top_partition_idx
                 << ", heap size: " << _partition_heap.size();
        _partition_heap.pop();
        // To invalidate data early, trigger to invalidate here.
        top_partition.invalidate();
    }
    _is_done = true;
    VLOG_ROW << "PartitionHashTopn done";
    return Status::OK();
}

StatusOr<ChunkPtr> PartitionHashTopn::get_group_by_sort_columns(size_t expected_rows,
                                                                std::vector<size_t>& partition_idxes) {
    DCHECK(_is_done) << "PartitionHashTopn is not done";
    if (_partition_heap.empty()) {
        return ChunkPtr(nullptr);
    }
    int result_chunk_idx = 0;
    partition_idxes.clear();

    MutableColumns mutable_columns;
    mutable_columns.reserve(_sort_exprs->size());
    for (auto& column : _partition_infos[_partition_heap.top()].sort_columns) {
        mutable_columns.emplace_back(column->clone_empty());
    }
    while (!_partition_heap.empty() && result_chunk_idx < expected_rows) {
        size_t partition_idx = _partition_heap.top();
        partition_idxes.emplace_back(partition_idx);
        auto& partition_info = _partition_infos[partition_idx];
        for (int i = 0; i < partition_info.sort_columns.size(); i++) {
            mutable_columns[i]->append(*partition_info.sort_columns[i], 0, 1);
        }
        result_chunk_idx++;
        _partition_heap.pop();
    }
    auto chunk = std::make_shared<Chunk>();
    DCHECK_EQ(mutable_columns.size(), _partition_sort_column_slot_ids.size());
    for (int i = 0; i < mutable_columns.size(); i++) {
        chunk->append_column(std::move(mutable_columns[i]), _partition_sort_column_slot_ids[i]);
    }
    return chunk;
}

Status PartitionHashTopn::_evaluate_sort_columns(const ChunkPtr& chunk, Columns* columns) {
    columns->reserve(_sort_exprs->size());
    for (auto* expr : *_sort_exprs) {
        // TODO: only needs the 1st row of the sort columns for each partition, so we can optimize the evaluation here
        ASSIGN_OR_RETURN(ColumnPtr col, expr->evaluate(chunk.get()));
        columns->emplace_back(std::move(col));
    }
    for (auto i = 0; i < columns->size(); i++) {
        VLOG_ROW << "PartitionHashTopn evaluate_sort_columns, column: " << i
                 << ", column: " << columns->at(i)->debug_string();
    }

    return Status::OK();
}

} // namespace starrocks
