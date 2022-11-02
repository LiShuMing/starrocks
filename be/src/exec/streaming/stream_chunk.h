// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_STREAM_CHUNK_H
#define STARROCKS_STREAM_CHUNK_H

#include "column/chunk.h"
#include "exec/streaming/streaming_fdw.h"
#include "runtime/memory/chunk.h"

namespace starrocks::streaming {

class StreamChunk;
using StreamingChunkPtr = std::shared_ptr<StreamChunk>;

enum RowOp {
    INSERT,
    DELETE,
    UPDATE_BEFORE,
    UPDATE_AFTER
};

struct StreamBarrier {
    Epoch epoch;
    int64_t version;
};

class StreamChunk: public vectorized::Chunk {
public:
    const std::vector<RowOp>& ops() const { return _ops; }
private:
    std::vector<RowOp> _ops;
};

}
#endif //STARROCKS_STREAM_CHUNK_H
