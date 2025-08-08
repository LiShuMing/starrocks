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

package com.starrocks.sql.optimizer.rule.tvr;

import com.starrocks.common.tvr.TvrTableDelta;

public class TvrTrait {
    private final TvrTableDelta tvrTableDelta;
    private final boolean isAppendOnly;

    public TvrTrait(TvrTableDelta tvrTableDelta, boolean isAppendOnly) {
        this.tvrTableDelta = tvrTableDelta;
        this.isAppendOnly = isAppendOnly;
    }

    public static TvrTrait of(TvrTableDelta tvrTableDelta, boolean isAppendOnly) {
        return new TvrTrait(tvrTableDelta, isAppendOnly);
    }

    /**
     * Check if the TVR is append-only.
     * @return true if the TVR is append-only, false otherwise.
     */
    public boolean isAppendOnly() {
        return isAppendOnly;
    }

    public TvrTableDelta getTvrDelta() {
        return tvrTableDelta;
    }
}
