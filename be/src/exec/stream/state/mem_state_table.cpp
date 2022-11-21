// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "mem_state_table.h"

namespace starrocks::stream {

/**
 * This is a hook to deduce Field from PrimitiveType. for now only used for tests.
 */
class PrimitiveTypeToScalarFieldTypeMapping {
public:
    PrimitiveTypeToScalarFieldTypeMapping() {
        for (auto& i : _data) {
            i = LOGICAL_TYPE_UNKNOWN;
        }
        _data[TYPE_BOOLEAN] = LOGICAL_TYPE_BOOL;
        _data[TYPE_TINYINT] = LOGICAL_TYPE_TINYINT;
        _data[TYPE_SMALLINT] = LOGICAL_TYPE_SMALLINT;
        _data[TYPE_INT] = LOGICAL_TYPE_INT;
        _data[TYPE_BIGINT] = LOGICAL_TYPE_BIGINT;
        _data[TYPE_LARGEINT] = LOGICAL_TYPE_LARGEINT;
        _data[TYPE_FLOAT] = LOGICAL_TYPE_FLOAT;
        _data[TYPE_DOUBLE] = LOGICAL_TYPE_DOUBLE;
        _data[TYPE_CHAR] = LOGICAL_TYPE_CHAR;
        _data[TYPE_VARCHAR] = LOGICAL_TYPE_VARCHAR;
        //        _data[TYPE_DATE] = LOGICAL_TYPE_DATE;
        _data[TYPE_DATE] = LOGICAL_TYPE_DATE_V2;
        _data[TYPE_DATETIME] = LOGICAL_TYPE_TIMESTAMP;
        //        _data[TYPE_DATETIME] = TYPE_DATETIME;
        _data[TYPE_DECIMAL] = LOGICAL_TYPE_DATETIME;
        _data[TYPE_DECIMALV2] = LOGICAL_TYPE_DECIMAL_V2;
        _data[TYPE_DECIMAL32] = LOGICAL_TYPE_DECIMAL32;
        _data[TYPE_DECIMAL64] = LOGICAL_TYPE_DECIMAL64;
        _data[TYPE_DECIMAL128] = LOGICAL_TYPE_DECIMAL128;
        _data[TYPE_JSON] = LOGICAL_TYPE_JSON;
    }
    LogicalType get_field_type(PrimitiveType p_type) { return _data[p_type]; }

private:
    // TODO: add TYPE_MAX_VALUE
    LogicalType _data[TYPE_FUNCTION];
};

static PrimitiveTypeToScalarFieldTypeMapping g_ptype_to_scalar_ftype;

LogicalType primitive_type_to_scalar_field_type(PrimitiveType p_type) {
    LogicalType ftype = g_ptype_to_scalar_ftype.get_field_type(p_type);
    DCHECK(ftype != LOGICAL_TYPE_UNKNOWN);
    return ftype;
}

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

bool MemStateTable::_equal_key(const DatumKeyRow& m_k, const DatumRow key) {
    for (auto i = 0; i < key.size(); i++) {
        auto type = _slots[i]->type().type;
        switch (type) {
        case TYPE_BIGINT:
            if (key[i].get_int64() != std::get<int64_t>(m_k[i])) {
                return false;
            }
            break;
        case TYPE_INT:
            if (key[i].get_int32() != std::get<int32_t>(m_k[i])) {
                return false;
            }
            break;
        case TYPE_VARCHAR:
        case TYPE_CHAR:
            if (key[i].get_slice() != std::get<Slice>(m_k[i])) {
                return false;
            }
            break;
        default:
            return false;
        }
    }
    return true;
}

Datum MemStateTable::convert_datum_key_to_datum(PrimitiveType type, DatumKey datum_key) {
    switch (type) {
    case TYPE_BIGINT:
        return Datum(std::get<int64_t>(datum_key));
    case TYPE_INT:
        return Datum(std::get<int32_t>(datum_key));
    case TYPE_VARCHAR:
    case TYPE_CHAR:
        return Datum(std::get<Slice>(datum_key));
    default:
        throw std::runtime_error("not supported yet!");
    }
}

ChunkIteratorPtrOr MemStateTable::get_chunk_iter(const DatumRow& key) {
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
                    row.push_back(convert_datum_key_to_datum(_slots[s]->type().type, m_k[s]));
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
        return std::make_shared<DatumRowIterator>(make_schema_from_slots(result_slots), rows);
    } else {
        // point seek
        DCHECK_EQ(key.size(), _k_num);
        auto key_row = convert_datum_row_to_key(key, 0, _k_num);

        auto result_slots = std::vector<SlotDescriptor*>{_slots.begin() + _k_num, _slots.end()};
        if (auto it = _kv_mapping.find(key_row); it != _kv_mapping.end()) {
            auto value_row = it->second;
            auto rows = std::vector<DatumRow>{value_row};
            return std::make_shared<DatumRowIterator>(make_schema_from_slots(result_slots), rows);
        } else {
            return Status::EndOfFile("");
        }
    }
}

vectorized::Schema MemStateTable::make_schema_from_slots(const std::vector<SlotDescriptor*>& slots) {
    vectorized::Fields fields;
    for (auto& slot : slots) {
        auto type_desc = slot->type();
        auto f_type = primitive_type_to_scalar_field_type(type_desc.type);
        auto field = std::make_shared<vectorized::Field>(slot->id(), slot->col_name(), f_type, false);
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

DatumRow MemStateTable::make_datum_row(vectorized::Chunk* chunk, size_t start, size_t end, int row_idx) {
    DatumRow row;
    for (size_t i = start; i < end; i++) {
        DCHECK_LT(i, chunk->num_columns());
        auto& column = chunk->get_column_by_index(i);
        row.push_back(column->get(row_idx));
    }
    return row;
}

DatumKeyRow MemStateTable::make_datum_key_row(vectorized::Chunk* chunk, size_t start, size_t end, int row_idx) {
    DatumKeyRow row;
    for (size_t i = start; i < end; i++) {
        DCHECK_LT(i, chunk->num_columns());
        auto& column = chunk->get_column_by_index(i);
        row.push_back((column->get(row_idx)).convert2DatumKey());
    }
    return row;
}

DatumKeyRow MemStateTable::convert_datum_row_to_key(const DatumRow& row, size_t start, size_t end) {
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
        auto k = make_datum_key_row(chunk, 0, _k_num, i);
        auto v = make_datum_row(chunk, _k_num, _cols_num, i);
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
        auto k = make_datum_key_row(chunk, 0, _k_num, i);
        if (ops[i] == StreamRowOp::DELETE) {
            _kv_mapping.erase(k);
            continue;
        }
        auto v = make_datum_row(chunk, _k_num, _cols_num, i);
        _kv_mapping[k] = std::move(v);
    }
    return Status::OK();
}

} // namespace starrocks::stream