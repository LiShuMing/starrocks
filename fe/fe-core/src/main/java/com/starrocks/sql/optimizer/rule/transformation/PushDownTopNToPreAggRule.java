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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.thrift.TSortInfo;

import java.util.List;

/*
 * When a top-n operator follows after a 2 phase aggregation, and the top-n order by columns do not depend
 * on the aggregation results (i.e., they are the same as the group by columns), the topN could be
 * pushed down below the global aggregation to filter group by inputs during aggregation.
 * This avoids computing aggregates for groups that will be discarded by the topN.
 *
 * before:
 *           | cardinality: n
 *     TopN(Partial)
 *           |
 *      Agg(Global)
 *           |
 *        Exchange
 *           |
 *      Agg(Local)
 *
 * after:
 *           | cardinality: n
 *     TopN(Partial)
 *           |
 *      Agg(Global)
 *           |
 *        Exchange
 *           | cardinality: <= limit
 *      Agg(Local) [with topN information to filter group by data during aggregation]
 **/
public class PushDownTopNToPreAggRule extends TransformationRule {

    private PushDownTopNToPreAggRule() {
        super(RuleType.TF_PUSH_DOWN_TOPN_AGG, Pattern.create(OperatorType.LOGICAL_TOPN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF))));
    }

    private static final PushDownTopNToPreAggRule INSTANCE = new PushDownTopNToPreAggRule();

    public static PushDownTopNToPreAggRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        int topNPushDownAggMode = context.getSessionVariable().getTopNPushDownAggMode();
        if (topNPushDownAggMode < 0) {
            return false;
        }

        LogicalTopNOperator topn = (LogicalTopNOperator) input.getOp();
        if (topn.isTopNPushDownAgg()) {
            return false;
        }

        if (!topn.hasLimit() || topn.getLimit() > context.getSessionVariable().getCboPushDownTopNLimit()) {
            return false;
        }

        if (topn.getSortPhase() != SortPhase.PARTIAL || topn.hasOffset() || topn.getPredicate() != null) {
            return false;
        }

        if (topn.getPartitionByColumns() != null && !topn.getPartitionByColumns().isEmpty()) {
            return false;
        }

        OptExpression topnChild = input.inputAt(0);
        LogicalAggregationOperator aggGlobal = (LogicalAggregationOperator) topnChild.getOp();

        if (!aggGlobal.isSplit() || aggGlobal.getType() != AggType.GLOBAL || aggGlobal.getPredicate() != null) {
            return false;
        }

        OptExpression aggGlobalChild = topnChild.inputAt(0);
        LogicalAggregationOperator aggLocal = (LogicalAggregationOperator) aggGlobalChild.getOp();

        if (aggLocal.getType() != AggType.LOCAL || aggLocal.getPredicate() != null) {
            return false;
        }

        // Verify order by columns are exactly the same as group by columns for the optimization
        if (topNPushDownAggMode >= 1) {
            List<Ordering> orderByElements = topn.getOrderByElements();
            List<ColumnRefOperator> groupingKeys = aggGlobal.getGroupingKeys();
            if (!orderByElements.stream()
                    .allMatch(orderByElement -> groupingKeys.contains(orderByElement.getColumnRef()))) {
                return false;
            }

            if (orderByElements.size() != groupingKeys.size()) {
                return false;
            }

            for (int i = 0; i < orderByElements.size(); i++) {
                if (!orderByElements.get(i).getColumnRef().equals(groupingKeys.get(i))) {
                    return false;
                }
            }
        } else {
            List<Ordering> orderByElements = topn.getOrderByElements();
            List<ColumnRefOperator> groupingKeys = aggGlobal.getGroupingKeys();
            return orderByElements.stream().allMatch(
                    orderByElement -> groupingKeys.contains(orderByElement.getColumnRef()));
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topn = (LogicalTopNOperator) input.getOp();

        OptExpression agg = input.inputAt(0);
        LogicalAggregationOperator aggOp = (LogicalAggregationOperator) agg.getOp();

        OptExpression localAgg = agg.inputAt(0);
        LogicalAggregationOperator localAggOp = (LogicalAggregationOperator) localAgg.getOp();

        // Create a new TopN operator that will be placed above the local aggregate
        // This TopN operator will be used to filter group by data during local aggregation
        LogicalTopNOperator localTopNOp = new LogicalTopNOperator.Builder()
                .withOperator(topn)
                .setSortPhase(SortPhase.PARTIAL)
                .setIsSplit(false)
                .setPerPipeline(true) // No merge needed
                .build();

        // Create new local aggregation with TopN information for filtering during aggregation
        OptExpression newLocalAgg = OptExpression.create(new LogicalAggregationOperator.Builder()
                .withOperator(localAggOp)
                .setTopNLocalAgg(true)
                .setAggTopnSortInfo(topn.getSortInfo()) // Pass the sort info to the local agg for filtering
                .build(), localAgg.getInputs());

        // Create the local TopN that filters during aggregation
        OptExpression newLocalTopN = OptExpression.create(localTopNOp, newLocalAgg);

        // Update the global aggregation to take input from the local TopN
        OptExpression newAgg = OptExpression.create(aggOp, newLocalTopN);

        // Return the original topN with the new agg structure
        return Lists.newArrayList(OptExpression.create(topn, newAgg));
    }
}
