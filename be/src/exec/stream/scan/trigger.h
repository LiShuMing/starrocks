// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "column/chunk.h"

namespace starrocks::stream {

enum class TriggerMode { kOffsetTrigger = 0, kProcessTimeTrigger, kManualTrigger };

struct EpochTrigger {
    EpochTrigger() = default;
    virtual ~EpochTrigger() = default;
    virtual TriggerMode trigger_mode() = 0;
};

//struct OffsetEpochTrigger : public EpochTrigger {
//    ~OffsetEpochTrigger() override = default;
//    TriggerMode trigger_mode() override { return TriggerMode::kOffsetTrigger; }
//    int64_t max_offset;
//};
//
//struct ProcessTimeEpochTrigger : public EpochTrigger {
//    ~ProcessTimeEpochTrigger() override = default;
//    TriggerMode trigger_mode() override { return TriggerMode::kProcessTimeTrigger; }
//    int64_t max_time_ms;
//};

struct ManualEpochTrigger : public EpochTrigger {
    ~ManualEpochTrigger() override = default;
    TriggerMode trigger_mode() override { return TriggerMode::kManualTrigger; }
};

} // namespace starrocks::stream