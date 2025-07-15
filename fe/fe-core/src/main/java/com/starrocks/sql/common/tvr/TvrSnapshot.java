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

package com.starrocks.sql.common.tvr;

import java.util.Objects;
import java.util.Optional;

// This is a query options. The main function is to ensure that the same version is used during the query process.
// For tables that support time travel with query period, before calling the `ConnectorMetadata` interface,
// you need to obtain the table version by `getTableVersionRange` interface for passing,
// otherwise it will be used as an empty table.

public class TvrSnapshot extends TvrDelta  {

    public static TvrSnapshot empty() {
        return new TvrSnapshot(Optional.empty());
    }

    public static TvrSnapshot of(Optional<Long> end) {
        return new TvrSnapshot(end);
    }

    public static TvrSnapshot of(TvrVersion end) {
        return new TvrSnapshot(end);
    }

    public TvrSnapshot(Optional<Long> snapshot) {
        this(TvrVersion.of(snapshot.orElse(TvrVersion.MAX_TIME)));
    }

    public TvrSnapshot(TvrVersion to) {
        super(TvrVersion.MIN, to);
    }

    public Optional<Long> end() {
        if (end.isMax()) {
            return Optional.empty();
        } else {
            return Optional.of(end.getVersion());
        }
    }

    public boolean isEmpty() {
        return start.isMin() && end.isMax();
    }

    @Override
    public String toString() {
        return "Snapshot@(" + end + ")";
    }

    @Override
    public TvrSnapshot copy(TvrVersion from, TvrVersion to) {
        return new TvrSnapshot(to);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TvrSnapshot that = (TvrSnapshot) o;
        return Objects.equals(start, that.start) && Objects.equals(end, that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }
}
