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

package com.starrocks.sql.optimizer.operator;

public class OpRuleBit {
    // Like LogicalJoinOperator#transformMask, add a mask to avoid one operator's dead-loop in one transform rule.
    // eg: MV's UNION-ALL RULE:
    //                 UNION                         UNION
    //               /        \                    /       \
    //  OP -->   EXTRA-OP    MV-SCAN  -->     UNION    MV-SCAN     ---> ....
    //                                       /      \
    //                                  EXTRA-OP    MV-SCAN
    // Operator's rule mask: operator that has been union rewrite and no needs to rewrite again.
    public static final int OP_UNION_ALL_BIT = 0;
    // Operator's rule mask: operator that has been push down rewrite and no needs to rewrite again.
    public static final int OP_PUSH_DOWN_BIT = 1;
    public static final int OP_TRANSPARENT_MV_BIT = 2;
    public static final int OP_PARTITION_PRUNE_BIT = 3;
}
