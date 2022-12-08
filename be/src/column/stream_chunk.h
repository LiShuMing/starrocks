// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "column/chunk.h"
#include "column/chunk_extra_data.h"

namespace starrocks::vectorized {

using UInt8ColumnPtr = std::shared_ptr<vectorized::UInt8Column>;

enum StreamRowOp : std::uint8_t { INSERT = 0, DELETE = 1, UPDATE_BEFORE = 2, UPDATE_AFTER = 3 };
using StreamRowOps = std::vector<StreamRowOp>;

using StreamChunk = Chunk;
using StreamChunkPtr = std::shared_ptr<StreamChunk>;

class StreamChunkConverter {
public:
    static StreamChunkPtr make_stream_chunk(ChunkPtr chunk, UInt8ColumnPtr ops) {
        static std::vector<ChunkExtraDataMeta> stream_extra_data_meta = {
                ChunkExtraDataMeta{.type = TypeDescriptor(TYPE_UNSIGNED_TINYINT), .is_null = false, .is_const = false}};
        std::vector<ColumnPtr> stream_extra_data = {ops};
        auto extra_data = std::make_shared<ChunkExtraColumnsData>(std::move(stream_extra_data_meta),
                                                                  std::move(stream_extra_data));
        chunk->set_extra_data(std::move(extra_data));
        return chunk;
    }

    static bool has_ops_colum(const StreamChunk& chunk) {
        if (chunk.has_extra_data() && typeid(*chunk.get_extra_data()) == typeid(ChunkExtraColumnsData)) {
            return true;
        }
        return false;
    }

    static bool has_ops_colum(StreamChunkPtr chunk_ptr) {
        if (!chunk_ptr) {
            return false;
        }
        return has_ops_colum(*chunk_ptr);
    }

    static bool has_ops_colum(StreamChunk* chunk_ptr) {
        if (!chunk_ptr) {
            return false;
        }
        return has_ops_colum(*chunk_ptr);
    }

    static UInt8Column* ops_col(const StreamChunk& stream_chunk) {
        DCHECK(has_ops_colum(stream_chunk));
        auto extra_column_data = dynamic_cast<ChunkExtraColumnsData*>(stream_chunk.get_extra_data().get());
        DCHECK(extra_column_data);
        DCHECK_EQ(extra_column_data->columns().size(), 1);
        auto* op_col = ColumnHelper::as_raw_column<UInt8Column>(extra_column_data->columns()[0]);
        DCHECK(op_col);
        return op_col;
    }

    static UInt8Column* ops_col(StreamChunkPtr stream_chunk_ptr) {
        DCHECK(stream_chunk_ptr);
        return ops_col(*stream_chunk_ptr);
    }

    static UInt8Column* ops_col(StreamChunk* stream_chunk_ptr) {
        DCHECK(stream_chunk_ptr);
        return ops_col(*stream_chunk_ptr);
    }

    static const StreamRowOp* ops(StreamChunk stream_chunk) {
        auto* op_col = ops_col(stream_chunk);
        return (StreamRowOp*)(op_col->get_data().data());
    }

    static const StreamRowOp* ops(StreamChunk* stream_chunk) {
        auto* op_col = ops_col(stream_chunk);
        return (StreamRowOp*)(op_col->get_data().data());
    }

    static const StreamRowOp* ops(StreamChunkPtr stream_chunk) {
        auto* op_col = ops_col(stream_chunk);
        return (StreamRowOp*)(op_col->get_data().data());
    }
};

} // namespace starrocks::vectorized
