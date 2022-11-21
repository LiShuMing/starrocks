// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#ifndef STARROCKS_STATETABLE_H
#define STARROCKS_STATETABLE_H

#include "common/status.h"
#include "exec/stream/stream_fdw.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/tablet.h"

namespace starrocks::stream {
using ChunkIteratorPtrOr = StatusOr<vectorized::ChunkIteratorPtr>;
using ChunkPtrOr = StatusOr<vectorized::ChunkPtr>;

class StateTable {
public:
    virtual ~StateTable() = default;

    virtual Status init() = 0;
    virtual Status prepare(RuntimeState* state) = 0;
    virtual Status open(RuntimeState* state) = 0;
    virtual Status close(RuntimeState* state) = 0;
    virtual Status flush(RuntimeState* state, vectorized::Chunk* chunk) = 0;

    virtual ChunkIteratorPtrOr get_chunk_iter(const DatumRow& key) = 0;
    // If input is one pk row, result must be in one chunk!
    virtual ChunkPtrOr get_chunk(const DatumRow& key);

    // Batch API
    virtual std::vector<ChunkIteratorPtrOr> get_chunk_iters(const std::vector<DatumRow>& keys) = 0;
    virtual std::vector<ChunkPtrOr> get_chunks(const std::vector<DatumRow>& keys);

};

}
#endif //STARROCKS_STATETABLE_H
