// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <gtest/gtest.h>

#include "column/column_test_builder.h"
#include "exec/stream/stream_fdw.h"
#include "exprs/exprs_test_helper.h"
#include "testutil/desc_tbl_helper.h"

namespace starrocks::stream {

using SlotInfo = vectorized::SlotInfo;
using ExprsTestHelper = vectorized::ExprsTestHelper;
using StreamRowOp = vectorized::StreamRowOp;
using GroupByKeyInfo = SlotId;
using AggInfo = std::tuple<SlotId, std::string, LogicalType, LogicalType>;

class StreamTestBase : public testing::Test {
public:
    StreamTestBase() {
        _state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
        _runtime_profile = _state->runtime_profile();
        _mem_tracker = std::make_unique<MemTracker>();
    }

protected:
    template <typename T>
    ChunkPtr MakeStreamChunk(const std::vector<std::vector<T>>& cols, const std::vector<uint8_t>& ops) {
        auto chunk_ptr = std::make_shared<Chunk>();
        for (size_t i = 0; i < cols.size(); i++) {
            auto col = vectorized::ColumnTestBuilder::build_column<T>(cols[i]);
            chunk_ptr->append_column(std::move(col), i);
        }
        auto op_col = vectorized::ColumnTestBuilder::build_uint8_column(ops);
        chunk_ptr->append_column(std::move(op_col), cols.size());
        return chunk_ptr;
    }

    // TODO: now we assume, int32_t/ ... / int32_t / ... / uint8_t
    template <class T>
    void CheckChunk(ChunkPtr chunk, std::vector<LogicalType> types, std::vector<std::vector<T>> ans,
                    std::vector<uint8_t> ops) {
        auto chunk_size = chunk->num_rows();
        auto num_col = chunk->num_columns();
        DCHECK_EQ(types.size(), num_col);
        // Check data except ops.
        DCHECK_EQ(chunk_size, ans[0].size());

        for (size_t col_idx = 0; col_idx < ans.size(); ++col_idx) {
            auto& col = chunk->get_column_by_index(col_idx);
            auto exp_col = ans[col_idx];
            CheckColumn<T>(col, exp_col);
        }
        // check ops.
        if (ops.size() > 0) {
            DCHECK_EQ(chunk_size, ops.size());
            auto col_idx = num_col - 1;
            auto& col = chunk->get_column_by_index(col_idx);
            CheckColumn<uint8_t>(col, ops);
        }
    }

    template <typename T>
    void CheckColumn(const ColumnPtr& real_col, std::vector<T> exp_col) {
        VLOG_ROW << "Start to check column";
        DCHECK_EQ(real_col->size(), exp_col.size());
        auto num_row = real_col->size();
        for (size_t i = 0; i < num_row; i++) {
            CheckDatum<T>(real_col->get(i), exp_col[i]);
        }
    }

    template <typename T>
    void CheckDatum(Datum datum, T data) {
        DCHECK_EQ(datum.get<T>(), data);
    }

protected:
    RuntimeState* _state;
    ObjectPool _obj_pool;
    DescriptorTbl* _tbl;
    RuntimeProfile* _runtime_profile;
    std::unique_ptr<MemTracker> _mem_tracker;
};
} // namespace starrocks::stream
