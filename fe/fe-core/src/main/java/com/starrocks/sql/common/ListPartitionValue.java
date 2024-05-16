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

package com.starrocks.sql.common;

import java.util.List;
import java.util.Objects;

/**
 * `PartitionRange` contains a `PartitionKey` range and the partition's name to represent a table's partition range info.
 */
public class ListPartitionValue {
    // multi values
    private final List<String> partitionKey;
    private final String partitionName;

    public ListPartitionValue(String partitionName,
                              List<String> partitionKeys) {
        this.partitionName = partitionName;
        this.partitionKey = partitionKeys;
    }

    public List<String> getPartitionKey() {
        return partitionKey;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public int hashCode() {
        // only consider partition key
        return Objects.hash(partitionKey);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof ListPartitionValue)) {
            return false;
        }
        ListPartitionValue other = (ListPartitionValue) o;
        List<String> otherSelectedPartitionKeys = other.getPartitionKey();
        if (otherSelectedPartitionKeys.size() != partitionKey.size()) {
            return false;
        }
        int len = partitionKey.size();
        for (int i = 0; i < len; i++) {
            if (!partitionKey.get(i).equals(otherSelectedPartitionKeys.get(i))) {
                return false;
            }
        }
        return true;
    }
}
