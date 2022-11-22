// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/state/mem_state_table.h"

#include <gtest/gtest.h>

#include <vector>

#include "exec/stream/stream_test.h"
#include "testutil/desc_tbl_helper.h"

namespace starrocks::stream {

class MemStateTableTest : public StreamTestBase {
public:
    MemStateTableTest() = default;

    void SetUp() override {
        std::vector<SlotInfo> src_slots = std::vector<SlotInfo>{{"col1", TYPE_INT, false},
                                                                {"col2", TYPE_INT, false},
                                                                {"col3", TYPE_INT, false},
                                                                {"agg1", TYPE_INT, false},
                                                                {"op", TYPE_BOOLEAN, false}};
        _tbl = vectorized::DescTblHelper::generate_desc_tbl(_state, _obj_pool, {src_slots});
        _state->set_desc_tbl(_tbl);
    }

    void TearDown() override {}

protected:
    void CheckReadByPoint(StateTable* state_table, const std::vector<int32_t>& keys, const std::vector<int32_t>& ans,
                          uint8_t op) {
        return CheckReadByKey(state_table, keys, {ans}, {op});
    }

    void CheckReadByKey(StateTable* state_table, const std::vector<int32_t>& keys,
                        const std::vector<std::vector<int32_t>>& expect_rows, const std::vector<uint8_t>& expect_ops) {
        // only one column key
        DatumRow row;
        for (auto& key : keys) {
            Datum datum;
            datum.set_int32(key);
            row.emplace_back(datum);
        }

        auto iter_or = state_table->get_chunk_iter(row);
        DCHECK(iter_or.ok());
        auto chunk_or = state_table->get_chunk(row);
        DCHECK(chunk_or.ok());
        auto chunk = chunk_or.value();
        DCHECK_EQ(chunk->num_rows(), expect_rows.size());
        for (auto i = 0; i < chunk->num_rows(); i++) {
            CheckRowOfChunk(chunk, expect_rows[i], expect_ops[i], i);
        }
    }

    void CheckRowOfChunk(ChunkPtr chunk, const std::vector<int32_t>& ans, uint8_t op, int32_t row_idx) {
        DCHECK_EQ(chunk->num_rows(), 1);
        auto num_cols = ans.size();
        DCHECK_EQ(chunk->num_columns(), num_cols + 1);
        for (size_t i = 0; i < num_cols; i++) {
            auto col = chunk->get_column_by_index(i);
            DCHECK_EQ((col->get(row_idx)).get_int32(), ans[i]);
        }

        auto op_col = chunk->get_column_by_index(num_cols);
        DCHECK_EQ(op_col->get(row_idx).get_uint8(), op);
    }
};

TEST_F(MemStateTableTest, TestPointSeek) {
    auto tuple_desc = _tbl->get_tuple_descriptor(0);
    auto state_table = std::make_unique<MemStateTable>(tuple_desc->slots(), 1, false);
    auto chunk_ptr = MakeChunk({{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {11, 12, 13}}, {1, 2, 3});
    // write table
    state_table->flush(_state, chunk_ptr.get());
    // read table
    CheckReadByPoint(state_table.get(), {1}, {1, 1, 11}, 1);
    CheckReadByPoint(state_table.get(), {2}, {2, 2, 12}, 2);
    CheckReadByPoint(state_table.get(), {3}, {3, 3, 13}, 3);

    // UPDATE keys
    auto chunk_ptr2 = MakeChunk({{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {21, 22, 23}}, {1, 1, 1});
    // write table
    state_table->flush(_state, chunk_ptr2.get());
    // read table
    CheckReadByPoint(state_table.get(), {1}, {1, 1, 21}, 1);
    CheckReadByPoint(state_table.get(), {2}, {2, 2, 22}, 1);
    CheckReadByPoint(state_table.get(), {3}, {3, 3, 23}, 1);
}

TEST_F(MemStateTableTest, TestPrefixSeek) {
    auto tuple_desc = _tbl->get_tuple_descriptor(0);
    auto state_table = std::make_unique<MemStateTable>(tuple_desc->slots(), 3, false);
    auto chunk_ptr = MakeChunk({{1, 1, 1}, {1, 1, 1}, {1, 2, 3}, {11, 12, 13}}, {1, 2, 3});
    // write table
    state_table->flush(_state, chunk_ptr.get());
    // read table
    CheckReadByKey(state_table.get(), {1, 1},
                   {
                           {1, 11},
                           {2, 12},
                           {3, 13},
                   },
                   {1, 2, 3});

    // UPDATE keys
    auto chunk_ptr2 = MakeChunk({{1, 1, 1}, {1, 1, 1}, {1, 2, 3}, {21, 22, 23}}, {1, 1, 1});
    // write table
    state_table->flush(_state, chunk_ptr2.get());
    // read table
    CheckReadByKey(state_table.get(), {1, 1},
                   {
                           {1, 21},
                           {2, 22},
                           {3, 23},
                   },
                   {1, 1, 1});
}

} // namespace starrocks::stream
