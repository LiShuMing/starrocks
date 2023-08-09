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

package com.starrocks.sql.optimizer;

import com.starrocks.qe.ConnectContext;

/**
 * GECost contains GroupExpression and Cost to represent the cost of a group expression better.
 */
public class CostContext {
    private final Double cost;
    private final GroupExpression groupExpression;

    public CostContext(GroupExpression groupExpression, Double cost) {
        this.groupExpression = groupExpression;
        this.cost = cost;
    }

    public GroupExpression getGroupExpression() {
        return groupExpression;
    }

    public Double getCost() {
        return cost;
    }

    /**
     * Is current context better than the given equivalent context based on cost?
     */
    public boolean isBetterThan(CostContext other) {
        // If materialized view force rewrite is enabled, we choose group expression which contains the
        // materialized view first, if both expressions have/haven't materialized views, choose the lower cost one.
        if (ConnectContext.get().getSessionVariable().isEnableMaterializedViewForceRewrite()) {
            boolean isPlanRewrittenByMV = this.groupExpression.hasRewrittenByMV();
            boolean isOtherPlanRewrittenByMV = other.groupExpression.hasRewrittenByMV();

            if (isPlanRewrittenByMV == isOtherPlanRewrittenByMV) {
                // choose the lower cost one if both have/haven't rewritten by mv.
                return this.cost.compareTo(other.cost) < 0.0;
            } else if (isPlanRewrittenByMV) {
                return true;
            } else {
                return false;
            }
        }

        // by default choose the lower cost one.
        return this.cost.compareTo(other.cost) < 0.0;
    }
}