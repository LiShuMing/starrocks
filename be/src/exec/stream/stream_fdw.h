// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#ifndef STARROCKS_STREAM_FDW_H
#define STARROCKS_STREAM_FDW_H

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/stream_chunk.h"
#include "column/type_traits.h"

namespace starrocks::stream {

template <typename T>
using Buffer = vectorized::Buffer<T>;
using Columns = vectorized::Columns;
using SliceHashSet = vectorized::SliceHashSet;

using Epoch = int64_t;
using Datum = vectorized::Datum;
using DaumKey = vectorized::DatumKey;
using DatumRow = std::vector<vectorized::Datum>;
using DatumRowPtr = std::shared_ptr<DatumRow>;
using DatumRowOpt = std::optional<DatumRow>;
using Chunk = vectorized::Chunk;
using ChunkPtr = vectorized::ChunkPtr;
using StreamRowOp = vectorized::StreamRowOp;

// represents all aggs' data for one group by key.
using AggDataPtr = uint8_t*;

} // namespace starrocks::stream

#endif //STARROCKS_STREAM_FDW_H
