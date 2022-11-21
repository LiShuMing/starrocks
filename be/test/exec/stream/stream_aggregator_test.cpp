// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include "exec/stream/aggregate/stream_aggregator.h"

#include <gtest/gtest.h>

#include <vector>

#include "exec/stream/stream_test.h"
#include "testutil/desc_tbl_helper.h"

namespace starrocks::stream {

class StreamAggregateTestBase : public StreamTestBase {
public:
    // TODO: Now only support all data types are int64_t, maybe more flexible later.
    struct StreamRowData {
        std::vector<std::vector<int64_t>> rows;
        std::vector<uint8_t> ops;
    };

    StreamAggregateTestBase() {}

    ~StreamAggregateTestBase() = default;

    std::vector<PrimitiveType> get_slot_types(std::vector<SlotInfo> slot_infos) {
        std::vector<PrimitiveType> types;
        for (auto& info : slot_infos) {
            types.push_back(std::get<1>(info));
        }
        return types;
    }

    void RunBatchAndCheck(size_t run_id, const StreamRowData& input_rows, const StreamRowData& expect_result_data) {
        return RunBatchAndCheck(run_id, input_rows, expect_result_data, StreamRowData{{}, {}});
    }

    void RunBatchAndCheck(size_t run_id, const StreamRowData& input_rows, const StreamRowData& expect_result_data,
                          const StreamRowData& expect_intermediate_data) {
        std::cout << "[RunBatchAndCheck] >>>>>>>>>>>>>>> Run: " << run_id << std::endl;
        auto result_chunk_ptr = std::make_shared<Chunk>();
        auto intermediate_chunk_ptr = std::make_shared<Chunk>();
        std::vector<vectorized::ChunkPtr> detail_chunks;

        auto input_chunk_ptr = MakeChunk(input_rows.rows, input_rows.ops);
        auto chunk_size = input_chunk_ptr->num_rows();
        DCHECK_IF_ERROR(_stream_aggregator->process_chunk(input_chunk_ptr.get()));
        DCHECK_IF_ERROR(_stream_aggregator->output_changes(chunk_size, &result_chunk_ptr, &intermediate_chunk_ptr,
                                                           detail_chunks));
        for (auto& column : result_chunk_ptr->columns()) {
            std::cout << "[RunBatchAndCheck] result column:" << column->debug_string() << std::endl;
        }
        CheckChunk(result_chunk_ptr, get_slot_types(_slot_infos[2]), expect_result_data.rows, expect_result_data.ops);

        // intermediate data may not exist
        if (!expect_intermediate_data.rows.empty()) {
            for (auto& column : intermediate_chunk_ptr->columns()) {
                std::cout << "[RunBatchAndCheck] intermediate column:" << column->debug_string() << std::endl;
            }
            CheckChunk(intermediate_chunk_ptr, get_slot_types(_slot_infos[1]), expect_intermediate_data.rows,
                       expect_intermediate_data.ops);
        }
        if (!detail_chunks.empty()) {
            for (auto& detail_chunk : detail_chunks) {
                std::cout << "[RunBatchAndCheck] detail chunks..." << std::endl;
                for (auto& column : detail_chunk->columns()) {
                    std::cout << "[RunBatchAndCheck] detail column:" << column->debug_string() << std::endl;
                }
            }
        }
        DCHECK_IF_ERROR(_stream_aggregator->reset_state(_state));
    }

    void SetUp() override {}

    void TearDown() override {}

protected:
    std::unique_ptr<StreamAggregator> _create_stream_aggregator(const std::vector<std::vector<SlotInfo>>& slot_infos,
                                                                const std::vector<GroupByKeyInfo>& group_by_infos,
                                                                const std::vector<AggInfo>& agg_infos,
                                                                bool is_generate_retract, int32_t count_agg_idx) {
        auto params = std::make_shared<AggregatorParams>();
        params->needs_finalize = false;
        params->has_outer_join_child = false;
        params->streaming_preaggregation_mode = TStreamingPreaggregationMode::AUTO;
        params->intermediate_tuple_id = 1;
        params->output_tuple_id = 2;
        params->count_agg_idx = count_agg_idx;
        params->sql_grouping_keys = "";
        params->sql_aggregate_functions = "";
        params->conjuncts = {};
        params->is_testing = true;
        params->is_stream_mv = true;
        // TODO: test more cases.
        params->is_append_only = false;
        params->is_generate_retract = is_generate_retract;
        params->grouping_exprs = _create_group_by_exprs(slot_infos[0], group_by_infos);
        params->intermediate_aggr_exprs = {};
        params->aggregate_functions = _create_agg_exprs(slot_infos[0], agg_infos);
        return std::make_unique<StreamAggregator>(std::move(params));
    }

    std::vector<TExpr> _create_group_by_exprs(std::vector<SlotInfo> slot_infos, std::vector<GroupByKeyInfo> infos) {
        std::vector<TExpr> exprs;
        for (auto& slot_id : infos) {
            auto info = slot_infos[slot_id];
            auto type = ExprsTestHelper::create_scalar_type_desc(to_thrift(std::get<1>(info)));
            auto t_expr_node = ExprsTestHelper::create_slot_expr_node(0, slot_id, type, false);
            exprs.emplace_back(ExprsTestHelper::create_slot_expr(t_expr_node));
        }
        return exprs;
    }
    std::vector<TExpr> _create_agg_exprs(std::vector<SlotInfo> slot_infos, std::vector<AggInfo> infos) {
        std::vector<TExpr> exprs;
        for (auto& agg_info : infos) {
            auto slot_id = std::get<0>(agg_info);
            auto agg_name = std::get<1>(agg_info);
            auto slot_info = slot_infos[slot_id];
            // agg input type
            auto t_type = ExprsTestHelper::create_scalar_type_desc(to_thrift(std::get<1>(slot_info)));
            // agg intermediate type
            auto intermediate_type = ExprsTestHelper::create_scalar_type_desc(to_thrift(std::get<2>(agg_info)));
            // agg result type
            auto ret_type = ExprsTestHelper::create_scalar_type_desc(to_thrift(std::get<3>(agg_info)));

            auto child_node = ExprsTestHelper::create_slot_expr_node(0, slot_id, t_type, false);

            auto f_fn = ExprsTestHelper::create_builtin_function(agg_name, {t_type}, intermediate_type, ret_type);
            auto agg_expr = ExprsTestHelper::create_aggregate_expr(f_fn, {child_node});
            exprs.emplace_back(agg_expr);
        }
        return exprs;
    }

protected:
    std::unique_ptr<StreamAggregator> _stream_aggregator;
    std::unique_ptr<StreamAggregator> _stream_aggregator_generate_retract;
    std::vector<std::vector<SlotInfo>> _slot_infos;
    std::vector<GroupByKeyInfo> _group_by_infos;
    std::vector<AggInfo> _agg_infos;
};

class CountStreamAggregateTestBase : public StreamAggregateTestBase {
public:
    CountStreamAggregateTestBase(bool is_generate_retract) {
        _slot_infos = std::vector<std::vector<SlotInfo>>{
                // input slots
                {{"col1", TYPE_BIGINT, false}, {"col2", TYPE_BIGINT, false}, {"op", TYPE_BOOLEAN, false}},
                // intermediate slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
                // result slots
                {{"col1", TYPE_BIGINT, false},
                 {"count_agg", TYPE_BIGINT, false},
                 // TODO: result output should contain ops column
                 {"op", TYPE_BOOLEAN, false}},
        };
        _group_by_infos = {0};
        _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                          {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

        _tbl = vectorized::DescTblHelper::generate_desc_tbl(_state, _obj_pool, _slot_infos);
        _state->set_desc_tbl(_tbl);
        _stream_aggregator =
                _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, is_generate_retract, 0);
    }
};

class CountStreamAggregateTest : public CountStreamAggregateTestBase {
public:
    CountStreamAggregateTest() : CountStreamAggregateTestBase(false) {}
};

class CountStreamAggregateTestWithGenerateRetracts : public CountStreamAggregateTestBase {
public:
    CountStreamAggregateTestWithGenerateRetracts() : CountStreamAggregateTestBase(true) {}
};

TEST_F(CountStreamAggregateTest, TestNoRetracts_NoGenerateRetracts) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 2 INSERT
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 2 0
    // 2 1 0
    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}}, StreamRowData{{{1, 2}, {2, 1}}, {0, 0}});
    _stream_aggregator->close(_state);
}

TEST_F(CountStreamAggregateTest, TestWithRetracts_NoGenerateRetracts) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 1 DELETE
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 0 0 <---- It's Confused If No generated Retracts.
    // 2 1 0
    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 1}}, StreamRowData{{{1, 2}, {0, 1}}, {0, 0}});
    _stream_aggregator->close(_state);
}

TEST_F(CountStreamAggregateTest, TestNoRetracts_MultiRuns) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // RUN 1
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 2 INSERT
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 2 0
    // 2 1 0
    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}}, StreamRowData{{{1, 2}, {2, 1}}, {0, 0}});

    // RUN 2
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 2 INSERT
    //
    // last run(intermediate) data
    // c0, count(c1), op
    // 1 2 0
    // 2 1 0
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 4 0
    // 2 2 0
    RunBatchAndCheck(2, StreamRowData{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}}, StreamRowData{{{1, 2}, {4, 2}}, {0, 0}});

    // RUN 3
    // RUN 2
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  3 2 INSERT
    //
    // last run(intermediate) data
    // c0, count(c1), op
    // 1 4 0
    // 2 2 0
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 5 0
    // 2 3 0
    // 3 1 0
    RunBatchAndCheck(3, StreamRowData{{{1, 2, 3}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData{{{1, 2, 3}, {5, 3, 1}}, {0, 0, 0}});

    // Final Close
    _stream_aggregator->close(_state);
}

TEST_F(CountStreamAggregateTestWithGenerateRetracts, TestWithRetracts_GenerateRetracts) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 1 DELETE
    // group by c0 count(c1)
    // with retract
    // c0, count(c1), op
    // 2 1 0 <--- group by key 0 will not be output, because it's retracted.
    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 1}}, StreamRowData{{{2}, {1}}, {0}});
    _stream_aggregator->close(_state);
}

TEST_F(CountStreamAggregateTestWithGenerateRetracts, TestWithRetracts_MultiRuns) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // Run 1
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 1 DELETE
    // group by c0 count(c1)
    // with retract
    // c0, count(c1), op
    // 2 1 INSERT <--- group by key 0 will not be output, because it's retracted.
    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 1}}, StreamRowData{{{2}, {1}}, {0}});

    // Run 2
    // Data:
    // c0 c1 op
    //  2 2 DELETE
    //  1 1 INSERT
    //  3 1 INSERT
    // (last run) data
    // 2 1 INSERT
    // group by c0 count(c1)
    // with retract
    // c0, count(c1), op
    // 2 1 DELETE
    // 1 1 INSERT
    // 3 1 INSERT
    RunBatchAndCheck(2, StreamRowData{{{2, 1, 3}, {2, 1, 1}}, {1, 0, 0}},
                     StreamRowData{{{2, 1, 3}, {1, 1, 1}}, {1, 0, 0}}); // TODO: make sure state table is also cleaned.

    // Run 3
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  3 1 INSERT
    //  1 1 INSERT
    // (last run) data
    // 2 1 INSERT
    // 1 1 INSERT
    // 3 1 INSERT
    // group by c0 count(c1)
    // with retract
    // c0, count(c1), op
    // 1 1 UPDATE_BEFORE
    // 3 1 UPDATE_BEFORE
    // 2 1 INSERT
    // 1 3 UPDATE_AFTER
    // 3 1 UPDATE_AFTER
    RunBatchAndCheck(3, StreamRowData{{{2, 1, 3}, {2, 1, 1}}, {0, 0, 0}},
                     StreamRowData{{{1, 3, 2, 1, 3}, {1, 1, 1, 2, 2}},
                                   {2, 2, 0, 3, 3}}); // make sure state table is also cleaned.

    _stream_aggregator->close(_state);
}

class MinMaxCountStreamAggregateTest : public StreamAggregateTestBase {
public:
    MinMaxCountStreamAggregateTest(bool is_generate_retract) {
        _slot_infos = std::vector<std::vector<SlotInfo>>{
                // input slots
                {{"col1", TYPE_BIGINT, false}, {"col2", TYPE_BIGINT, false}, {"op", TYPE_BOOLEAN, false}},
                // intermediate slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"min_agg", TYPE_BIGINT, false},
                        {"max_agg", TYPE_BIGINT, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
                // result slots
                {{"col1", TYPE_BIGINT, false},
                 {"min_agg", TYPE_BIGINT, false},
                 {"max_agg", TYPE_BIGINT, false},
                 {"count_agg", TYPE_BIGINT, false},
                 // TODO: result output should contain ops column
                 {"op", TYPE_BOOLEAN, false}},
        };
        _group_by_infos = {0};
        _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                          {1, "retract_min", TYPE_BIGINT, TYPE_BIGINT},
                                          {1, "retract_max", TYPE_BIGINT, TYPE_BIGINT},

                                          {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

        _tbl = vectorized::DescTblHelper::generate_desc_tbl(_state, _obj_pool, _slot_infos);
        _state->set_desc_tbl(_tbl);
        _stream_aggregator =
                _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, is_generate_retract, 2);
    }
};

class MinMaxCountStreamAggregateTestWithoutRetract : public MinMaxCountStreamAggregateTest {
public:
    MinMaxCountStreamAggregateTestWithoutRetract() : MinMaxCountStreamAggregateTest(false) {}
};

TEST_F(MinMaxCountStreamAggregateTestWithoutRetract, TestNoRetracts_OneRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));

    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData{{{1, 2}, {1, 2}, {2, 2}}, {0, 0}});
    _stream_aggregator->close(_state);
}

TEST_F(MinMaxCountStreamAggregateTestWithoutRetract, TestNoRetracts_MultiRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));
    // Run 1
    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData{{{1, 2}, {1, 2}, {2, 2}, {2, 1}}, {0, 0}});
    // Run 2
    RunBatchAndCheck(2, StreamRowData{{{1, 2, 3}, {1, 3, 3}}, {0, 0, 0}},
                     StreamRowData{{{1, 2, 3}, {1, 2, 3}, {2, 3, 3}, {3, 2, 1}}, {0, 0, 0}});
    // Run 3
    RunBatchAndCheck(3, StreamRowData{{{2, 3}, {1, 4}}, {0, 0}},
                     StreamRowData{{{2, 3}, {1, 3}, {3, 4}, {3, 2}}, {0, 0}});
    _stream_aggregator->close(_state);
}

class MinMaxCountStreamAggregateTestWithRetract : public MinMaxCountStreamAggregateTest {
public:
    MinMaxCountStreamAggregateTestWithRetract() : MinMaxCountStreamAggregateTest(true) {}
};

TEST_F(MinMaxCountStreamAggregateTestWithRetract, TestWihRetracts_OneRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));

    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {1, 2, 1}}, {0, 0, 1}}, StreamRowData{{{2}, {2}, {2}, {1}}, {0}});
    _stream_aggregator->close(_state);
}

TEST_F(MinMaxCountStreamAggregateTestWithRetract, TestWihRetracts_MultiRun_NoRetractInput) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));
    // Run 1
    // Input:
    // key  value
    // 1    0
    // 2    2
    // 1    2
    // key  min max count op
    // 1    0   2   2   0
    // 2    2   2   1   0
    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {0, 2, 2}}, {0, 0, 0}},
                     StreamRowData{{{1, 2}, {0, 2}, {2, 2}, {2, 1}}, {0, 0}});
    // Run 2
    // Input:
    // key  value
    // 1    1
    // 2    3
    // 3    3
    // key  min max count op
    // 1    0   2   2    UPDATE_BEFORE
    // 2    2   2   1    UPDATE_BEFORE
    // 1    0   2   3    UPDATE_AFTER
    // 2    2   3   2    UPDATE_AFTER
    // 3    3   3   1    INSERT
    RunBatchAndCheck(
            2, StreamRowData{{{1, 2, 3}, {1, 3, 3}}, {0, 0, 0}},
            StreamRowData{{{1, 2, 1, 2, 3}, {0, 2, 0, 2, 3}, {2, 2, 2, 3, 3}, {2, 1, 3, 2, 1}}, {2, 2, 3, 3, 0}});
    // Run 3
    // Input:
    // key  value
    // 2    1
    // 3    4
    // key  min max count op
    // 2    2   3   2    UPDATE_BEFORE
    // 3    3   3   1    UPDATE_BEFORE
    // 2    1   3   3    UPDATE_AFTER
    // 3    3   4   2    INSERT
    RunBatchAndCheck(3, StreamRowData{{{2, 3}, {1, 4}}, {0, 0}},
                     StreamRowData{{{2, 3, 2, 3}, {2, 3, 1, 3}, {3, 3, 3, 4}, {2, 1, 3, 2}}, {2, 2, 3, 3}});
    _stream_aggregator->close(_state);
}

TEST_F(MinMaxCountStreamAggregateTestWithRetract, TestWihRetracts_MultiRun_RetractInputs) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_state, &_obj_pool, _runtime_profile, _mem_tracker.get()));
    DCHECK_IF_ERROR(_stream_aggregator->open(_state));

    // key <-> min <-> max <-> count
    // Run 1
    // Input:
    // key  value
    // 1    +1
    // 2    +2
    // 1    +2
    // key  min max count op
    // 1    1   2   2   0
    // 2    2   2   1   0
    RunBatchAndCheck(1, StreamRowData{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData{{{1, 2}, {1, 2}, {2, 2}, {2, 1}}, {0, 0}});
    // Run 2
    // Input:
    // key  value
    // 1    -1
    // 2    -2
    // 3    +3
    // key  min max count op
    // 1    1   2   2   2
    // 2    2   2   1   1
    // 1    1   2   1   3  ??
    // 3    3   3   1   0
    RunBatchAndCheck(2, StreamRowData{{{1, 2, 3}, {1, 2, 3}}, {1, 1, 0}},
                     StreamRowData{{{1, 2, 1, 3}, {1, 2, 1, 3}, {2, 2, 2, 3}, {2, 1, 1, 1}}, {2, 1, 3, 0}});
    // Run 3
    // Input:
    // key  value
    // 1    -2
    // 2    +2
    // 3    -3
    // key  min max count op
    // 1    1   2   1   1
    // 3    3   3   1   1
    // 2    2   2   1   0
    RunBatchAndCheck(3, StreamRowData{{{1, 2, 3}, {2, 2, 3}}, {1, 0, 1}},
                     StreamRowData{{{1, 3, 2}, {1, 3, 2}, {2, 3, 2}, {1, 1, 1}}, {1, 1, 0}});
    _stream_aggregator->close(_state);
}

} // namespace starrocks::stream
