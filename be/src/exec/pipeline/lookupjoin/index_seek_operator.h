// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/operator.h"
#include "exec/vectorized/olap_scan_prepare.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_reader.h"

namespace starrocks::pipeline {
using namespace vectorized;

// IndexSeekOperator
class IndexSeekOperator final : public Operator {
public:
    IndexSeekOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int32_t driver_sequence,
                      TInternalScanRange* scan_range, const std::vector<std::string>& key_column_names,
                      const TupleDescriptor* tuple_desc, const std::vector<ExprContext*>& conjunct_ctxs);

    ~IndexSeekOperator() override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return true; }

    bool need_input() const override { return !is_finished(); }

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    Status _get_tablet(const TInternalScanRange* scan_range);
    Status _init_conjuncts_manager(RuntimeState* state);
    Status _init_scanner_columns(std::vector<uint32_t>& scanner_columns);
    Status _init_reader_params(RuntimeState* state,
                               const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges);
    Status _init_tablet_reader(RuntimeState* state) ;

    bool _is_finished = false;
    vectorized::ChunkPtr _cur_chunk = nullptr;

    TabletSharedPtr _tablet;
    TInternalScanRange* _scan_range;
    const std::vector<std::string>& _key_column_names;
    const TupleDescriptor* _tuple_desc;
    const std::vector<ExprContext*>& _conjunct_ctxs;

    int64_t _version;
    ObjectPool _obj_pool;
    vectorized::TabletReaderParams _params{};
    vectorized::OlapScanConjunctsManager _conjuncts_manager;
    std::vector<std::unique_ptr<OlapScanRange>> _key_ranges;
    vectorized::ConjunctivePredicates _not_push_down_predicates;
    std::vector<ExprContext*>* _not_push_down_conjuncts;

    using PredicatePtr = std::unique_ptr<vectorized::ColumnPredicate>;
    std::vector<PredicatePtr> _predicate_free_pool;

    // NOTE: _reader may reference the _predicate_free_pool, it should be released before the _predicate_free_pool
    std::shared_ptr<vectorized::TabletReader> _reader;
    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<vectorized::ChunkIterator> _prj_iter;
};

class IndexSeekOperatorFactory final : public OperatorFactory {
public:
    IndexSeekOperatorFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "index_seek_operator", plan_node_id) {}

    ~IndexSeekOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
//        return std::make_shared<IndexSeekOperator>(this, _id, _plan_node_id, driver_sequence);
        return nullptr;
    }

private:
};

} // namespace starrocks::pipeline
