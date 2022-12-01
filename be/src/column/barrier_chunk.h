// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "column/chunk.h"

namespace starrocks::vectorized {

using UInt8ColumnPtr = std::shared_ptr<vectorized::UInt8Column>;

class BarrierChunk;
using BarrierChunkPtr = std::shared_ptr<BarrierChunk>;

/**
 * BarrierChunk is used for Stream MV which generates a Barrier message.
 */
class BarrierChunk : public Chunk {
public:
    BarrierChunk(int64_t epoch_id) : Chunk(), _epoch_id(epoch_id) {}

    BarrierChunk() = default;

    BarrierChunk(BarrierChunk&& other) = default;
    BarrierChunk& operator=(BarrierChunk&& other) = default;

    ~BarrierChunk() override = default;

    // Disallow copy and assignment.
    BarrierChunk(const BarrierChunk& other) = delete;
    BarrierChunk& operator=(const BarrierChunk& other) = delete;

    std::string debug_string() override { return "[BarrierChunk] epoch_id=" + std::to_string(_epoch_id); }

private:
    int64_t _epoch_id;
};

} // namespace starrocks::vectorized
