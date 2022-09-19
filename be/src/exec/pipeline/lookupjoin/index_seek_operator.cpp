// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/lookupjoin/index_seek_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/operator.h"
#include "exec/vectorized/olap_scan_node.h"
#include "util/runtime_profile.h"
#include "storage/chunk_helper.h"
#include "storage/predicate_parser.h"

namespace starrocks::pipeline {

IndexSeekOperator::IndexSeekOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int32_t driver_sequence,
                                     TInternalScanRange* scan_range, const std::vector<std::string>& key_column_names,
                                     const TupleDescriptor* tuple_desc, const std::vector<ExprContext*>& conjunct_ctxs)
        : Operator(factory, id, "nestloop_join_build", plan_node_id, driver_sequence),
          _scan_range(scan_range), _key_column_names(key_column_names), _tuple_desc(tuple_desc),
          _conjunct_ctxs(conjunct_ctxs) {
}

IndexSeekOperator::~IndexSeekOperator() {
    _reader.reset();
    _predicate_free_pool.clear();
}

Status IndexSeekOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(_init_tablet_reader(state));
    return Status::OK();
}

Status IndexSeekOperator::_get_tablet(const TInternalScanRange* scan_range) {
    _version = strtoul(scan_range->version.c_str(), nullptr, 10);
    ASSIGN_OR_RETURN(_tablet, vectorized::OlapScanNode::get_tablet(scan_range));
    return Status::OK();
}

Status IndexSeekOperator::_init_conjuncts_manager(RuntimeState* state) {
    vectorized::OlapScanConjunctsManager& cm = _conjuncts_manager;
    cm.conjunct_ctxs_ptr = &_conjunct_ctxs;
    cm.tuple_desc = _tuple_desc;
    cm.obj_pool = &_obj_pool;
    cm.key_column_names = &_key_column_names;
    cm.runtime_state = state;

    // Parse conjuncts via _conjuncts_manager.
    RETURN_IF_ERROR(cm.parse_conjuncts(true, config::max_scan_key_num, false));
    // Get key_ranges and not_push_down_conjuncts from _conjuncts_manager.
    RETURN_IF_ERROR(_conjuncts_manager.get_key_ranges(&_key_ranges));
    _conjuncts_manager.get_not_push_down_conjuncts(&_not_push_down_conjuncts);

    return Status::OK();
}

Status IndexSeekOperator::_init_reader_params(RuntimeState* state,
                                              const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) {
    _params.is_pipeline = true;
    _params.chunk_size = state->chunk_size();
    _params.reader_type = READER_QUERY;
    _params.skip_aggregation = true;
    _params.runtime_state = state;
    _params.use_page_cache = !config::disable_storage_page_cache;

    PredicateParser parser(_tablet->tablet_schema());
    std::vector<PredicatePtr> preds;
    RETURN_IF_ERROR(_conjuncts_manager.get_column_predicates(&parser, &preds));
    for (auto& p : preds) {
        if (parser.can_pushdown(p.get())) {
            _params.predicates.push_back(p.get());
        } else {
            _not_push_down_predicates.add(p.get());
        }
        _predicate_free_pool.emplace_back(std::move(p));
    }

    // Range
    for (const auto& key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _params.range = key_range->begin_include ? TabletReaderParams::RangeStartOperation::GE
                                                 : TabletReaderParams::RangeStartOperation::GT;
        _params.end_range = key_range->end_include ? TabletReaderParams::RangeEndOperation::LE
                                                   : TabletReaderParams::RangeEndOperation::LT;

        _params.start_key.push_back(key_range->begin_scan_range);
        _params.end_key.push_back(key_range->end_scan_range);
    }

    return Status::OK();
}

Status IndexSeekOperator::_init_scanner_columns(std::vector<uint32_t>& scanner_columns) {
    auto slots = _tuple_desc->slots();
    for (auto slot : slots) {
        DCHECK(slot->is_materialized());
        int32_t index = _tablet->field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid field name: " << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        scanner_columns.push_back(index);
    }
    if (scanner_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

Status IndexSeekOperator::_init_tablet_reader(RuntimeState* state) {
    // init tablet
    // TODO: how to define scan_range?
    RETURN_IF_ERROR(_get_tablet(_scan_range));

    // output columns of `this` OlapScanner, i.e, the final output columns of `get_chunk`.
    std::vector<uint32_t> scanner_columns;

    RETURN_IF_ERROR(_init_scanner_columns(scanner_columns));
    // _init_reader_params
    RETURN_IF_ERROR(_init_reader_params(state, _key_ranges));

    const TabletSchema& tablet_schema = _tablet->tablet_schema();
    starrocks::vectorized::Schema child_schema =
            ChunkHelper::convert_schema_to_format_v2(tablet_schema, scanner_columns);

    std::vector<std::unique_ptr<OlapScanRange>> key_ranges;
    RETURN_IF_ERROR(_conjuncts_manager.get_key_ranges(&key_ranges));

    _reader = std::make_shared<TabletReader>(_tablet, Version(0, _version), std::move(child_schema));
    _prj_iter = _reader;
    RETURN_IF_ERROR(_reader->prepare());
    RETURN_IF_ERROR(_reader->open(_params));

    return Status::OK();
}

void IndexSeekOperator::close(RuntimeState* state) {
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> IndexSeekOperator::pull_chunk(RuntimeState* state) {
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }
    _cur_chunk.reset(ChunkHelper::new_chunk_pooled(_prj_iter->output_schema(), state->chunk_size(), true));
    do {
        RETURN_IF_ERROR(_prj_iter->get_next(_cur_chunk.get()));
        // TODO: remove unused predicate.
    } while (_cur_chunk->num_rows() == 0);
    return std::move(_cur_chunk);
}

Status IndexSeekOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

Status IndexSeekOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return Status::OK();
}

} // namespace starrocks::pipeline
