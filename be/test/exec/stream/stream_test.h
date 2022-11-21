// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_STREAM_TEST_H
#define STARROCKS_STREAM_TEST_H

#include "column/column_test_builder.h"
#include "exec/stream/stream_fdw.h"
#include "exprs/exprs_test_helper.h"
#include <gtest/gtest.h>
#include "testutil/desc_tbl_helper.h"

namespace starrocks::stream {

using SlotInfo = vectorized::SlotInfo;
using ExprsTestHelper = vectorized::ExprsTestHelper;
using StreamRowOp = vectorized::StreamRowOp;
using GroupByKeyInfo = SlotId;
using AggInfo = std::tuple<SlotId, std::string, PrimitiveType, PrimitiveType>;

class StreamTestBase : public testing::Test{
public :
    StreamTestBase() {
        _state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
        _runtime_profile = _state->runtime_profile();
        _mem_tracker = std::make_unique<MemTracker>();
    }
protected:
    ChunkPtr MakeChunk(const std::vector<std::vector<int64_t>>& cols, const std::vector<uint8_t>& ops) {
        auto chunk_ptr = std::make_shared<Chunk>();
        for (size_t i = 0; i < cols.size(); i++) {
            auto col = vectorized::ColumnTestBuilder::build_int64_column(cols[i]);
            chunk_ptr->append_column(std::move(col), i);
        }
        auto op_col = vectorized::ColumnTestBuilder::build_uint8_column(ops);
        chunk_ptr->append_column(std::move(op_col), cols.size());
        return chunk_ptr;
    }

    void CheckDatum(ChunkPtr chunk, const std::vector<int32_t>& ans, uint8_t op) {
        DCHECK_EQ(chunk->num_rows(), 1);
        auto num_cols = ans.size();
        DCHECK_EQ(chunk->num_columns(), num_cols + 1);
        for (size_t i = 0; i < num_cols; i++) {
            auto col = chunk->get_column_by_index(i);
            DCHECK_EQ((col->get(0)).get_int32(), ans[i]);
        }

        auto op_col = chunk->get_column_by_index(num_cols);
        DCHECK_EQ(op_col->get(0).get_uint8(), op);

    }

    // TODO: now we assume, int32_t/ ... / int32_t / ... / uint8_t
    void CheckChunk(ChunkPtr chunk, std::vector<PrimitiveType> types,
                    std::vector<std::vector<int64_t>> ans,
                    std::vector<uint8_t> ops) {
        auto chunk_size = chunk->num_rows();
        auto num_col = chunk->num_columns();
        DCHECK_EQ(types.size(), num_col);
        {
            // Check data except ops.
            DCHECK_EQ(chunk_size, ans[0].size());

            for (size_t col_idx = 0; col_idx < ans.size(); ++col_idx) {
                auto& col = chunk->get_column_by_index(col_idx);
                auto exp_col = ans[col_idx];
                for (size_t i = 0; i < chunk_size; i++) {
                    CheckDatumWithType(types[col_idx], col->get(i), exp_col[i]);
                }
            }
        }
        // check ops.
        if (ops.size() > 0) {
            DCHECK_EQ(chunk_size, ops.size());
            auto col_idx = num_col - 1;
            auto& col = chunk->get_column_by_index(col_idx);
            for (size_t i = 0; i < chunk_size; i++) {
                CheckDatumWithType(types[col_idx], col->get(i), ops[i]);
            }
        }
    }

    template<typename T>
    void CheckDatumWithType(PrimitiveType type, Datum datum, T data) {
        switch (type) {
        case TYPE_INT:
            DCHECK_EQ(datum.get_int32(), data);
            break;
        case TYPE_BIGINT:
            DCHECK_EQ(datum.get_int64(), data);
            break;
        case TYPE_BOOLEAN:
            DCHECK_EQ(datum.get_uint8(), data);
            break;
        default:
            std::cout << "NOT SUPPORTED!!!" << std::endl;
            break;
        }
    }

protected:
    RuntimeState* _state;
    ObjectPool _obj_pool;
    DescriptorTbl* _tbl;
    RuntimeProfile* _runtime_profile;
    std::unique_ptr<MemTracker> _mem_tracker;
};
} // namespace: starrocks::stream

#endif //STARROCKS_STREAM_TEST_H
