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

package com.starrocks.common.tvr;

import java.util.Objects;
import java.util.Optional;

/**
 * This is used for IVM (Incremental Version Management) to represent a delta.
 */
public class TvrTableDelta extends TvrVersionRange {

    public static TvrTableDelta of(TvrVersion from, TvrVersion to) {
        return new TvrTableDelta(from, to);
    }

    public static TvrTableDelta of(Optional<Long> from, Optional<Long> to) {
        return new TvrTableDelta(from, to);
    }

    public static TvrTableDelta emptyDelta() {
        return new TvrTableDelta(TvrVersion.MIN, TvrVersion.MIN);
    }

    public TvrTableDelta(TvrVersion from, TvrVersion to) {
        super(from, to);
    }

    public TvrTableDelta(Optional<Long> from, Optional<Long> to) {
        super(TvrVersion.of(from.orElse(TvrVersion.MIN_TIME)),
                TvrVersion.of(to.orElse(TvrVersion.MAX_TIME)));
    }

    public TvrTableSnapshot fromSnapshot() {
        return TvrTableSnapshot.of(from);
    }

    public TvrTableSnapshot toSnapshot() {
        return TvrTableSnapshot.of(to);
    }

    @Override
    public String toString() {
        return "Delta[" + from + "," + to + "]";
    }

    @Override
    public TvrTableDelta copy(TvrVersion from, TvrVersion to) {
        return new TvrTableDelta(from, to);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TvrTableDelta that = (TvrTableDelta) o;
        return Objects.equals(from, that.from) && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }
}
