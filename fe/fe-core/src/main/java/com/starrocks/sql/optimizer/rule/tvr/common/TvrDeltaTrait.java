// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rule.tvr.common;

import com.starrocks.common.tvr.TvrTableDelta;

public class TvrDeltaTrait {
    public static final TvrDeltaTrait DEFAULT = new TvrDeltaTrait(TvrTableDelta.emptyDelta(),
            TvrChangeType.MONOTONIC, TvrDeltaStats.EMPTY);

    private final TvrTableDelta tvrTableDelta;
    private final TvrChangeType tvrChangeType;
    private final TvrDeltaStats tvrDeltaStats;

    public TvrDeltaTrait(TvrTableDelta tvrTableDelta,
                         TvrChangeType tvrChangeType,
                         TvrDeltaStats tvrDeltaStats) {
        this.tvrTableDelta = tvrTableDelta;
        this.tvrChangeType = tvrChangeType;
        this.tvrDeltaStats = tvrDeltaStats;
    }

    public static TvrDeltaTrait ofMonotonic(TvrTableDelta tvrTableDelta,
                                            TvrDeltaStats tvrDeltaStats) {
        return new TvrDeltaTrait(tvrTableDelta, TvrChangeType.MONOTONIC, tvrDeltaStats);
    }

    public static TvrDeltaTrait ofRetractable(TvrTableDelta tvrTableDelta,
                                              TvrDeltaStats tvrDeltaStats) {
        return new TvrDeltaTrait(tvrTableDelta, TvrChangeType.RETRACTABLE, tvrDeltaStats);
    }

    public boolean isAppendOnly() {
        return tvrChangeType == TvrChangeType.MONOTONIC;
    }

    public TvrTableDelta getTvrDelta() {
        return tvrTableDelta;
    }

    public TvrChangeType getTvrChangeType() {
        return tvrChangeType;
    }

    public TvrDeltaStats getTvrDeltaStats() {
        return tvrDeltaStats;
    }
}
