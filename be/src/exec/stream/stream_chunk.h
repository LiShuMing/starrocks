// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_STREAM_CHUNK_H
#define STARROCKS_STREAM_CHUNK_H

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "runtime/memory/chunk.h"
#include "stream_fdw.h"

namespace starrocks::stream {

struct StreamBarrier {
    Epoch epoch;
    int64_t version;
};

} // namespace starrocks::stream
#endif //STARROCKS_STREAM_CHUNK_H
