// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#ifndef STARROCKS_MEM_STATE_TABLE_H
#define STARROCKS_MEM_STATE_TABLE_H

#include "column/datum.h"
#include "column/field.h"
#include "column/schema.h"
#include "exec/stream/state/state_table.h"
#include "storage/chunk_iterator.h"

namespace starrocks::stream {

using DatumKeyRow = std::vector<vectorized::DatumKey>;
using DatumKey = vectorized::DatumKey;

LogicalType primitive_type_to_scalar_field_type(PrimitiveType p_type);

// DatumIterator only have one datum row for the pk.
class DatumRowIterator final : public vectorized::ChunkIterator {
public:
    explicit DatumRowIterator(vectorized::Schema schema, std::vector<DatumRow> rows) :
            ChunkIterator(schema, rows.size()), _rows(rows) {}

    void close() override {}

protected:
    Status do_get_next(Chunk* chunk) override {
        if (!_is_eos) {
            _convert_datum_rows_to_chunk(_rows, chunk);
            _is_eos = true;
            return Status::OK();
        }
        return Status::EndOfFile("end of empty iterator");
    }

    Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override {
        return Status::EndOfFile("end of empty iterator");
    }
private:
    void _convert_datum_rows_to_chunk(const std::vector<DatumRow>& rows, Chunk* chunk) {
        // Chunk should be already allocated, no need create column any more.
        DCHECK(chunk);
        for (size_t row_num = 0; row_num < rows.size(); row_num++) {
            auto& row = rows[row_num];
            for (size_t i = 0; i < row.size(); i++) {
                DCHECK_LT(i, chunk->num_columns());
                auto& col = chunk->get_column_by_index(i);
                col->append_datum(row[i]);
            }
        }
    }
private:
    std::vector<DatumRow> _rows;
    bool _is_eos{false};
};

// Used for testing.
class MemStateTable: public StateTable {
public:
    // For MemStateTable, we assume flushed chunk's columns is assigned as:
    // _k_num | _v_num
    MemStateTable(std::vector<SlotDescriptor*> slots, size_t k_num, bool flush_op_col):
            _slots(slots), _k_num(k_num), _cols_num(slots.size()), _flush_op_col(flush_op_col) {}
    ~MemStateTable() override = default;

    Status init() override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    ChunkIteratorPtrOr get_chunk_iter(const DatumRow& key) override;
    std::vector<ChunkIteratorPtrOr> get_chunk_iters(const std::vector<DatumRow>& keys) override;
    Status flush(RuntimeState* state, vectorized::Chunk* chunk) override;
    vectorized::Schema make_schema_from_slots(const std::vector<SlotDescriptor*>& slots);

    static DatumKeyRow convert_datum_row_to_key(const DatumRow& row, size_t start, size_t end);
    static Datum convert_datum_key_to_datum(PrimitiveType type, DatumKey datum_key);
    static DatumKeyRow make_datum_key_row(vectorized::Chunk* chunk, size_t start, size_t end, int row_idx);
    static DatumRow make_datum_row(vectorized::Chunk* chunk, size_t start, size_t end, int row_idx);
private:
    bool _equal_key(const DatumKeyRow& m_k, const DatumRow key);
    Status _flush_with_ops(RuntimeState* state, vectorized::Chunk* chunk);
    Status _flush_without_ops(RuntimeState* state, vectorized::Chunk* chunk);
private:
    TupleDescriptor* _tuple_desc;
    std::vector<SlotDescriptor*> _slots;
    size_t _k_num;
    size_t _cols_num;
    bool _flush_op_col;
    std::map<DatumKeyRow, DatumRow> _kv_mapping;
};

} // namespace starrocks::stream
#endif //STARROCKS_MEM_STATE_TABLE_H
