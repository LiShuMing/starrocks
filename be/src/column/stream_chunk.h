// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "column/chunk.h"

namespace starrocks::vectorized {

/**
enum StreamRowOp : std::uint8_t { INSERT = 0, DELETE = 1, UPDATE_BEFORE = 2, UPDATE_AFTER = 3 };

class StreamChunk : public Chunk {
public:
    StreamChunk(ChunkPtr chunk, std::vector<StreamRowOp> ops):
            _chunk(std::move(chunk)), _ops(std::move(ops))  {
        if (_chunk) {
            DCHECK_EQ(_chunk->num_rows(), _ops.size());
        }
    }

    StreamChunk(StreamChunk&& other) = default;
    StreamChunk& operator=(StreamChunk&& other) = default;

    ~StreamChunk() override = default;

    // Disallow copy and assignment.
    StreamChunk(const StreamChunk& other) = delete;
    StreamChunk& operator=(const StreamChunk& other) = delete;

    // For stream chunk, the last column must be Int8Column and not nullable, convert to StreamRowOp.
    const std::vector<StreamRowOp>& ops() const { return _ops; }
    const ChunkPtr chunk() const {return _chunk;}
private:
    ChunkPtr _chunk;
    std::vector<StreamRowOp> _ops;
};*/
}
