// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/state/mem_state_table.h"

#include <vector>

#include "exec/stream/stream_test.h"
#include <gtest/gtest.h>
#include "testutil/desc_tbl_helper.h"

namespace starrocks::stream {

class MemStateTableTest : public StreamTestBase {
public :
    MemStateTableTest() {}

    void SetUp() override {
        std::vector<SlotInfo> src_slots = std::vector<SlotInfo> {
                {"col1", TYPE_INT, false},
                {"col2", TYPE_INT, false},
                {"col3", TYPE_INT, false},
                {"agg1", TYPE_INT, false},
                {"op", TYPE_BOOLEAN, false}
        };
        _tbl = vectorized::DescTblHelper::generate_desc_tbl(_state, _obj_pool, {src_slots});
        _state->set_desc_tbl(_tbl);
    }

    void TearDown() override {}
protected:
    void CheckReadByKey(StateTable* state_table, std::vector<int32_t> keys, std::vector<int32_t> ans, uint8_t op) {
        // only one column key
        DatumRow row;
        for (auto& key: keys) {
            Datum datum;
            datum.set_int32(key);
            row.emplace_back(datum);
        }

        auto iter_or = state_table->get_chunk_iter(row);
        DCHECK(iter_or.ok());
        auto chunk_or = state_table->get_chunk(row);
        DCHECK(chunk_or.ok());
        auto chunk = chunk_or.value();
        CheckDatum(chunk, ans, op);
    }
};

TEST_F(MemStateTableTest, TestPointSeek) {
    auto tuple_desc = _tbl->get_tuple_descriptor(0);
    auto state_table = std::make_unique<MemStateTable>(tuple_desc->slots(), 1, false);
    auto chunk_ptr = MakeChunk({{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {11, 12, 13}}, {1, 2, 3});
    // write table
    state_table->flush(_state, chunk_ptr.get());
    // read table
    CheckReadByKey(state_table.get(), {1}, {1, 1, 11}, 1);
    CheckReadByKey(state_table.get(), {2}, {2, 2, 12}, 2);
    CheckReadByKey(state_table.get(), {3}, {3, 3, 13}, 3);

    // UPDATE keys
    auto chunk_ptr2 = MakeChunk({{1, 2, 3}, {21, 22, 23}}, {1, 1, 1});
    // write table
    state_table->flush(_state, chunk_ptr2.get());
    // read table
    CheckReadByKey(state_table.get(), {1}, {1, 1, 21}, 1);
    CheckReadByKey(state_table.get(), {2}, {2, 2, 22}, 1);
    CheckReadByKey(state_table.get(), {3}, {3, 3, 23}, 1);
}

TEST_F(MemStateTableTest, TestPrefixSeek) {
    auto tuple_desc = _tbl->get_tuple_descriptor(0);
    auto state_table = std::make_unique<MemStateTable>(tuple_desc->slots(), 1, false);
    auto chunk_ptr = MakeChunk({{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {11, 12, 13}}, {1, 2, 3});
    // write table
    state_table->flush(_state, chunk_ptr.get());
    // read table
    CheckReadByKey(state_table.get(), {1}, {1, 1, 11}, 1);
    CheckReadByKey(state_table.get(), {2}, {2, 2, 12}, 2);
    CheckReadByKey(state_table.get(), {3}, {3, 3, 13}, 3);

    // UPDATE keys
    auto chunk_ptr2 = MakeChunk({{1, 2, 3}, {21, 22, 23}}, {1, 1, 1});
    // write table
    state_table->flush(_state, chunk_ptr2.get());
    // read table
    CheckReadByKey(state_table.get(), {1}, {1, 1, 21}, 1);
    CheckReadByKey(state_table.get(), {2}, {2, 2, 22}, 1);
    CheckReadByKey(state_table.get(), {3}, {3, 3, 23}, 1);
}

} // namespace starrocks::stream
