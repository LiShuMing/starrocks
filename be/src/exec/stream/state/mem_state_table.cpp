// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "mem_state_table.h"

namespace starrocks::stream {

Status MemStateTable::init() {
    return Status::OK();
}

Status MemStateTable::prepare(RuntimeState* state) {
    return Status::OK();
}

Status MemStateTable::open(RuntimeState* state) {
    return Status::OK();
}

Status MemStateTable::close(RuntimeState* state) {
    return Status::OK();
}

bool MemStateTable::_equal_key(const DatumKeyRow& m_k, const DatumRow key) const {
    for (auto i = 0; i < key.size(); i++) {
        if (!key[i].equal_datum_key(m_k[i])) {
            return false;
        }
    }
    return true;
}

ChunkIteratorPtrOr MemStateTable::get_chunk_iter(const DatumRow& key) {
    VLOG_ROW << "[get_chunk_iter] lookup key size:" << key.size() << ", k_num:" << _k_num;
    if (key.size() < _k_num) {
        // prefix scan
        std::vector<DatumRow> rows;
        for (auto iter = _kv_mapping.begin(); iter != _kv_mapping.end(); iter++) {
            // if equal
            auto m_k = iter->first;
            if (_equal_key(m_k, key)) {
                DatumRow row;
                // add extra key cols + value cols
                for (int32_t s = key.size(); s < m_k.size(); s++) {
                    row.push_back(Datum(m_k[s]));
                }
                for (auto& datum : iter->second) {
                    row.push_back(datum);
                }
                rows.push_back(std::move(row));
            }
        }
        if (rows.empty()) {
            return Status::EndOfFile("");
        }
        auto result_slots = std::vector<SlotDescriptor*>{_slots.begin() + key.size(), _slots.end()};
        return std::make_shared<DatumRowIterator>(_make_schema_from_slots(result_slots), std::move(rows));
    } else {
        // point seek
        DCHECK_EQ(key.size(), _k_num);
        auto key_row = _convert_datum_row_to_key(key, 0, _k_num);

        auto result_slots = std::vector<SlotDescriptor*>{_slots.begin() + _k_num, _slots.end()};
        if (auto it = _kv_mapping.find(key_row); it != _kv_mapping.end()) {
            auto& value_row = it->second;
            auto rows = std::vector<DatumRow>{value_row};
            return std::make_shared<DatumRowIterator>(_make_schema_from_slots(result_slots), std::move(rows));
        } else {
            return Status::EndOfFile("NotFound");
        }
    }
}

vectorized::Schema MemStateTable::_make_schema_from_slots(const std::vector<SlotDescriptor*>& slots) {
    vectorized::Fields fields;
    for (auto& slot : slots) {
        auto type_desc = slot->type();
        VLOG_ROW << "[make_schema_from_slots] type:" << type_desc;
        auto field = std::make_shared<vectorized::Field>(slot->id(), slot->col_name(), type_desc.type, false);
        fields.emplace_back(std::move(field));
    }
    return vectorized::Schema(std::move(fields), KeysType::PRIMARY_KEYS, {});
}

std::vector<ChunkIteratorPtrOr> MemStateTable::get_chunk_iters(const std::vector<DatumRow>& keys) {
    std::vector<ChunkIteratorPtrOr> ans;
    ans.reserve(keys.size());
    for (auto& key : keys) {
        ans.emplace_back(get_chunk_iter(key));
    }
    return ans;
}

DatumRow MemStateTable::_make_datum_row(vectorized::Chunk* chunk, size_t start, size_t end, int row_idx) {
    DatumRow row;
    for (size_t i = start; i < end; i++) {
        DCHECK_LT(i, chunk->num_columns());
        auto& column = chunk->get_column_by_index(i);
        row.push_back(column->get(row_idx));
    }
    return row;
}

DatumKeyRow MemStateTable::_make_datum_key_row(vectorized::Chunk* chunk, size_t start, size_t end, int row_idx) {
    DatumKeyRow row;
    for (size_t i = start; i < end; i++) {
        DCHECK_LT(i, chunk->num_columns());
        auto& column = chunk->get_column_by_index(i);
        row.push_back((column->get(row_idx)).convert2DatumKey());
    }
    return row;
}

DatumKeyRow MemStateTable::_convert_datum_row_to_key(const DatumRow& row, size_t start, size_t end) {
    DatumKeyRow key_row;
    for (size_t i = start; i < end; i++) {
        auto datum = row[i];
        key_row.push_back(datum.convert2DatumKey());
    }
    return key_row;
}

Status MemStateTable::flush(RuntimeState* state, vectorized::Chunk* chunk) {
    if (_flush_op_col) {
        return _flush_with_ops(state, chunk);
    } else {
        return _flush_without_ops(state, chunk);
    }
}

Status MemStateTable::_flush_without_ops(RuntimeState* state, vectorized::Chunk* chunk) {
    auto chunk_size = chunk->num_rows();

    for (auto i = 0; i < chunk_size; i++) {
        auto k = _make_datum_key_row(chunk, 0, _k_num, i);
        auto v = _make_datum_row(chunk, _k_num, _cols_num, i);
        _kv_mapping[k] = std::move(v);
    }
    return Status::OK();
}

Status MemStateTable::_flush_with_ops(RuntimeState* state, vectorized::Chunk* chunk) {
    auto chunk_size = chunk->num_rows();
    auto* ops = chunk->ops();
    for (auto i = 0; i < chunk_size; i++) {
        if (ops[i] == StreamRowOp::UPDATE_BEFORE) {
            continue;
        }
        auto k = _make_datum_key_row(chunk, 0, _k_num, i);
        if (ops[i] == StreamRowOp::DELETE) {
            _kv_mapping.erase(k);
            continue;
        }
        auto v = _make_datum_row(chunk, _k_num, _cols_num, i);
        _kv_mapping[k] = std::move(v);
    }
    return Status::OK();
}

} // namespace starrocks::stream