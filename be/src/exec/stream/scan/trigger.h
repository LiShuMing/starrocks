// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "column/chunk.h"

namespace starrocks::stream {

struct EpochTrigger {};

struct OffsetEpochTrigger : public EpochTrigger {
    int64_t max_offset;
};

struct ProcessTimeEpochTrigger : public EpochTrigger {
    int64_t max_time_ms;
};

struct ManualEpochTrigger : public EpochTrigger {};

} // namespace starrocks::stream