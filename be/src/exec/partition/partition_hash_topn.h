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

#pragma once

#include <limits>
#include <queue>
#include <unordered_map>
#include <vector>

#include "column/chunk.h"
#include "common/statusor.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sorting.h"
#include "exprs/expr_context.h"

namespace starrocks {

// A lightweight hash-aware topN helper.
// It keeps the best (offset + limit) rows according to the sort keys.
// The data is kept by referencing input chunks, so the caller must keep
// the chunks alive until done() is called.
class PartitionHashTopn {
public:
    PartitionHashTopn(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>& is_asc_order,
                      const std::vector<bool>& is_null_first, size_t offset, size_t limit);

    bool is_valid(size_t partition_idx, const Columns& partition_columns, size_t row_idx);

    // Feed one chunk. The chunk must outlive the topN instance until done() is called.
    Status offer(size_t partition_idx, const ChunkPtr& chunk);

    // New a partition.
    Status new_partition(size_t partition_idx);

    // Finalize and make the internal result iterable.
    Status done();

    // Return next chunk of results. nullptr chunk means EOS.
    StatusOr<ChunkPtr> get_group_by_sort_columns(size_t expected_rows, std::vector<size_t>& partition_idxes);

    bool exhausted() const { return _is_done && _partition_heap.empty(); }

    bool has_output() const { return _is_done && !_partition_heap.empty(); }

private:
    // PartitionInfo is used to keep all chunks for a partition
    // which is the same for each row compared by the sort/partition keys.
    // Optimized for cache locality: frequently accessed fields first.
    struct PartitionInfo {
        enum State : uint8_t {
            INITIAL = 0,
            VALID = 1,
            INVALID = 2,
        };
        // Put sort_columns first as it's accessed most frequently (in comparator hot path)
        // This improves cache locality when comparing partitions in the heap.
        // TODO: only keep the 1st row of the sort columns for each partition.
        Columns sort_columns;

        // chunks in this partition
        // NOTE: chunks should not empty since each partition should have at least one chunk.
        // but we use a hack tech here, when chunks is empty after pop, the partition is considered as invalid.
        std::vector<ChunkPtr> chunks;

        // 0: initial state
        // 1: valid partition
        // 2: invalid partition
        State state = State::INITIAL;

        // whether the partition is valid, if the partition is not valid, no need to compare with the new row
        bool is_invalid() const { return state == State::INVALID; }

        bool is_valid() const { return state == State::VALID; }

        bool is_new() const { return state == State::INITIAL; }

        void to_valid() { this->state = State::VALID; }

        void invalidate() {
            state = State::INVALID;
            for (auto& chunk : chunks) {
                chunk->reset();
            }
            sort_columns.clear();
            for (auto& column : sort_columns) {
                const_cast<Column*>(column.get())->reset_column();
            }
            chunks.clear();
        }
    };

    struct PartitionComparator {
        explicit PartitionComparator(const SortDescs* sort_descs, const std::vector<PartitionInfo>& partition_infos)
                : sort_descs(sort_descs), partition_infos(partition_infos) {}

        bool operator()(size_t lhs_idx, size_t rhs_idx) const {
            const auto& lhs = partition_infos[lhs_idx];
            const auto& rhs = partition_infos[rhs_idx];
            const auto& l_sort_columns = lhs.sort_columns;
            const auto& r_sort_columns = rhs.sort_columns;
            DCHECK_GT(l_sort_columns.size(), 0);
            DCHECK_EQ(l_sort_columns.size(), r_sort_columns.size());
            DCHECK_EQ(l_sort_columns.size(), sort_descs->num_columns());
            int cmp = compare_chunk_row(*sort_descs, l_sort_columns, r_sort_columns, 0, 0);
            return cmp < 0;
        }

        const SortDescs* sort_descs;
        const std::vector<PartitionInfo>& partition_infos;
    };

    bool _compare_to_top_row(const Columns& partition_columns, size_t row_idx) {
        auto& top_partition_info = _get_top_partition_info();
        auto& top_sort_columns = top_partition_info.sort_columns;
        for (int i = 0; i < partition_columns.size(); i++) {
            int cmp = partition_columns[i]->compare_at(row_idx, 0, *top_sort_columns[i],
                                                       _sort_descs.get_column_desc(i).nan_direction());
            if (cmp != 0) {
                return cmp < 0;
            }
        }
        return false;
    }

    Status _evaluate_sort_columns(const ChunkPtr& chunk, Columns* columns);

    PartitionInfo& _get_top_partition_info() {
        DCHECK_GT(_partition_heap.size(), 0);
        return _partition_infos.at(_partition_heap.top());
    }

    const std::vector<ExprContext*>* _sort_exprs;
    SortDescs _sort_descs;
    const size_t _limit;

    // Store all rows per partition
    std::vector<PartitionInfo> _partition_infos;
    // Heap to maintain top N partitions (by their best row)
    // Use indices instead of pointers to avoid invalidation when vector reallocates
    std::priority_queue<size_t, std::vector<size_t>, PartitionComparator> _partition_heap;
    std::vector<SlotId> _partition_sort_column_slot_ids;

    // The position of the next row to be returned for the current partition being processed
    // _current_partition_idx tracks which partition we're currently processing
    // If it doesn't match the top partition, we reset position tracking
    // Initialized to max value to indicate "no current partition"
    size_t _current_partition_idx =
            std::numeric_limits<size_t>::max(); // Track which partition we're currently processing
    size_t _result_chunk_idx = 0;               // Chunk index within current partition
    size_t _result_chunk_pos = 0;               // Row position within current chunk

    bool _is_done = false;
};

} // namespace starrocks
