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
// limitations under the License

package com.starrocks.sql.optimizer.rule.tvr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrLazyOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOpUtils;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptContext;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptMeta;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TvrJoinRule extends TvrTransformationRule {

    public TvrJoinRule() {
        super(RuleType.TF_TVR_JOIN, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return isSupportedTvr(input);
    }

    @Override
    public OptExpression doTransform(OptExpression input,
                                     OptimizerContext context) {
        LogicalJoinOperator join = input.getOp().cast();
        OptExpression leftChildDelta = input.inputAt(0);
        TvrOptMeta leftOptMeta = leftChildDelta.getTvrMeta();
        OptExpression rightChildDelta = input.inputAt(1);
        TvrOptMeta rightOptMeta = rightChildDelta.getTvrMeta();

        // TODO: use left table's tvrOptMeta as the root
        // TODO: Use mv as the history state instead of recomputing.
        List<ColumnRefOperator> joinOutputColRefs = input.getRowOutputInfo().getOutputColRefs();

        // delta join
        if (!leftOptMeta.isAppendOnly() || !rightOptMeta.isAppendOnly()) {
            throw new IllegalStateException("Join operator should be append-only for TVR: " + join.getJoinType()
                    + " in " + input);
        }
        OptExpression deltaJoin = doTransformWithMonotonic(context, input, join, joinOutputColRefs,
                leftOptMeta, rightOptMeta);

        return deltaJoin;
    }

    private TvrOptMeta buildJoinTvrOptMeta(OptimizerContext context,
                                           LogicalJoinOperator join,
                                           TvrOptMeta leftOptMeta,
                                           TvrOptMeta rightOptMeta,
                                           List<ColumnRefOperator> joinOutputColRefs,
                                           TvrTableDeltaTrait joinDeltaTrait) {
        TvrOptExpression tvrLeftFrom = leftOptMeta.getFrom();
        TvrOptExpression tvrLeftTo = leftOptMeta.getTo();
        TvrOptExpression tvrRightFrom = rightOptMeta.getFrom();
        TvrOptExpression tvrRightTo = rightOptMeta.getTo();
        // from opt
        TvrLazyOptExpression fromJoin = TvrLazyOptExpression.of(() -> {
            LogicalJoinOperator newJoinOp = buildNewJoinOperator(join);
            OptExpressionWithOutput fromOpt = buildJoinOptExpression(context, joinOutputColRefs,
                    newJoinOp, tvrLeftFrom.optExpression(), tvrRightFrom.optExpression(), false);
            return new TvrOptExpression(tvrLeftFrom.tvrVersionRange(), fromOpt.optExpression());
        });
        // to opt
        TvrLazyOptExpression toJoin = TvrLazyOptExpression.of(() -> {
            LogicalJoinOperator newJoinOp = buildNewJoinOperator(join);
            OptExpressionWithOutput toOpt = buildJoinOptExpression(context, joinOutputColRefs,
                    newJoinOp, tvrLeftTo.optExpression(), tvrRightTo.optExpression(), false);
            return new TvrOptExpression(tvrLeftTo.tvrVersionRange(), toOpt.optExpression());
        });
        // root opt group
        return new TvrOptMeta(joinDeltaTrait, fromJoin, toJoin);
    }

    private List<OptExpressionWithOutput> buildCommonJoinDelta(OptimizerContext context,
                                                               LogicalJoinOperator join,
                                                               List<ColumnRefOperator> joinOutputColRefs,
                                                               TvrOptExpression tvrLeftFrom,
                                                               OptExpression rightDelta,
                                                               TvrOptExpression tvrRightTo,
                                                               OptExpression leftDelta) {
        OptExpressionWithOutput deltaOutput1 =
                buildJoinOptExpression(context, joinOutputColRefs, join, tvrLeftFrom.optExpression(),
                        rightDelta, true);
        OptExpressionWithOutput deltaOutput2 =
                buildJoinOptExpression(context, joinOutputColRefs, join, leftDelta,
                        tvrRightTo.optExpression(), true);
        return Lists.newArrayList(deltaOutput1, deltaOutput2);
    }

    private OptExpressionWithOutput buildPaddingJoinDelta(OptimizerContext context,
                                                          OptExpression input,
                                                          LogicalJoinOperator join,
                                                          List<ColumnRefOperator> joinOutputColRefs,
                                                          OptExpression leftDelta,
                                                          TvrOptExpression tvrRightTo) {
        // rewrite all right table column refs to constant NULL values
        OptExpression rightChild = tvrRightTo.optExpression();
        List<ColumnRefOperator> rightOutputColRefs = rightChild.getRowOutputInfo().getOutputColRefs();
        Map<ColumnRefOperator, ScalarOperator> rewrittenColumnRefMap = Maps.newHashMap();
        for (ColumnRefOperator rightColRef : rightOutputColRefs) {
            // replace the column ref with a constant NULL value
            ScalarOperator nullColRef = ConstantOperator.createNull(rightColRef.getType());
            rewrittenColumnRefMap.put(rightColRef, nullColRef);
        }
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewrittenColumnRefMap, true);
        Projection joinProjection = join.getProjection();

        Map<ColumnRefOperator, ScalarOperator> newProjectionColumnRefMap;
        if (joinProjection != null) {
            // rewrite the projection to use the constant NULL values for right table columns
            Map<ColumnRefOperator, ScalarOperator> projectionColumnRefMap = joinProjection.getColumnRefMap();
            newProjectionColumnRefMap = projectionColumnRefMap
                    .entrySet()
                    .stream()
                    .map(entry -> {
                        ColumnRefOperator colRef = entry.getKey();
                        ScalarOperator rewrittenOperator = rewriter.rewrite(entry.getValue());
                        return Maps.immutableEntry(colRef, rewrittenOperator);
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } else {
            newProjectionColumnRefMap = joinOutputColRefs.stream()
                    .map(colRef -> Maps.immutableEntry(colRef, rewriter.rewrite(colRef)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        LogicalJoinOperator newJoin = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.LEFT_ANTI_JOIN)
                .setProjection(new Projection(newProjectionColumnRefMap))
                .build();
        OptExpressionWithOutput deltaOutput =
                buildJoinOptExpression(context, joinOutputColRefs, newJoin, leftDelta,
                        tvrRightTo.optExpression(), true);
        return deltaOutput;
    }

    private OptExpression doTransformWithMonotonic(OptimizerContext context,
                                                   OptExpression input,
                                                   LogicalJoinOperator join,
                                                   List<ColumnRefOperator> joinOutputColRefs,
                                                   TvrOptMeta leftOptMeta,
                                                   TvrOptMeta rightOptMeta) {
        TvrOptExpression tvrLeftFrom = leftOptMeta.getFrom();
        TvrOptExpression tvrLeftTo = leftOptMeta.getTo();
        TvrOptExpression tvrRightFrom = rightOptMeta.getFrom();
        TvrOptExpression tvrRightTo = rightOptMeta.getTo();
        OptExpression leftDelta = input.inputAt(0);
        OptExpression rightDelta = input.inputAt(1);

        JoinOperator joinType = join.getJoinType();
        if (joinType.isInnerJoin() || joinType.isCrossJoin()) {
            // build the tvrOptMeta for the join operator, use the left table's tvrOptMeta as the root
            List<OptExpressionWithOutput> commonJoinDelta =
                    buildCommonJoinDelta(context, join, joinOutputColRefs, tvrLeftFrom, rightDelta, tvrRightTo, leftDelta);
            // build the join tvrOptMeta
            TvrOptMeta joinTvrOptMeta = buildJoinTvrOptMeta(context, join, leftOptMeta, rightOptMeta, joinOutputColRefs,
                    leftOptMeta.tvrDeltaTrait());
            // return a union operator to merge the common join delta
            return buildUnionOperator(joinTvrOptMeta, joinOutputColRefs, commonJoinDelta);
        } else if (joinType.isLeftSemiJoin()) {
            // part1: left delta left semi join right to
            OptExpressionWithOutput deltaOutput1 = buildJoinOptExpression(context, joinOutputColRefs, join, leftDelta,
                            tvrRightTo.optExpression(), true);

            // part2: (former non match) left semi join right delta
            LogicalJoinOperator newAntiJoin = new LogicalJoinOperator.Builder()
                    .withOperator(join)
                    .setJoinType(JoinOperator.LEFT_ANTI_JOIN)
                    .build();
            OptExpression prevAntiJoin = OptExpression.create(newAntiJoin, tvrLeftFrom.optExpression(),
                    tvrRightFrom.optExpression());
            OptExpressionWithOutput deltaOutput2 = buildJoinOptExpression(context, joinOutputColRefs,
                    join, prevAntiJoin, rightDelta, true);

            // build the tvrOptMeta for the join operator, use the left table's tvrOptMeta as the root
            TvrOptMeta joinTvrOptMeta = buildJoinTvrOptMeta(context, join, leftOptMeta, rightOptMeta,
                    joinOutputColRefs, leftOptMeta.tvrDeltaTrait());
            // merge all children into a single union operator
            return buildUnionOperator(joinTvrOptMeta, joinOutputColRefs, Lists.newArrayList(
                    deltaOutput1, deltaOutput2));
        } else if (joinType.isLeftOuterJoin()) {
            MaterializedView resultTable = getTvrTargetMV(context);

            // for left outer join, its result is retractable, so we need to build a retractable delta trait.
            TvrTableDeltaTrait joinDeltaTrait = TvrTableDeltaTrait.ofRetractable(leftOptMeta.tvrDeltaTrait().getTvrDelta(),
                    leftOptMeta.tvrDeltaTrait().getTvrDeltaStats());
            TvrOptMeta joinTvrOptMeta = buildJoinTvrOptMeta(context, join, leftOptMeta, rightOptMeta, joinOutputColRefs,
                    joinDeltaTrait);

            // part1: common inner join delta
            LogicalJoinOperator newInnerJoin = new LogicalJoinOperator.Builder()
                    .withOperator(join)
                    .setJoinType(JoinOperator.INNER_JOIN)
                    .build();
            List<OptExpressionWithOutput> innerJoinPart = buildCommonJoinDelta(
                    context, newInnerJoin, joinOutputColRefs, tvrLeftFrom, rightDelta, tvrRightTo, leftDelta);

            // part2: except the common join delta, we also need to add the padding result for non-match for left table.
            OptExpressionWithOutput paddingJoinDelta =
                    buildPaddingJoinDelta(context, input, join, joinOutputColRefs, leftDelta, tvrRightTo);

            // merge all children into a single union operator
            List<OptExpressionWithOutput> newChildren = Lists.newArrayList(innerJoinPart);
            newChildren.add(paddingJoinDelta);
            return buildUnionOperator(joinTvrOptMeta, joinOutputColRefs, newChildren);
        } else if (joinType.isFullOuterJoin()) {
            MaterializedView resultTable = getTvrTargetMV(context);
            // for full outer join, its result is retractable, so we need to build a retractable delta trait.
            TvrTableDeltaTrait joinDeltaTrait = TvrTableDeltaTrait.ofRetractable(leftOptMeta.tvrDeltaTrait().getTvrDelta(),
                    leftOptMeta.tvrDeltaTrait().getTvrDeltaStats());
            TvrOptMeta joinTvrOptMeta = buildJoinTvrOptMeta(context, join, leftOptMeta, rightOptMeta, joinOutputColRefs,
                    joinDeltaTrait);

            // part1: common inner join delta
            LogicalJoinOperator newInnerJoin = new LogicalJoinOperator.Builder()
                    .withOperator(join)
                    .setJoinType(JoinOperator.INNER_JOIN)
                    .build();
            List<OptExpressionWithOutput> innerJoinPart = buildCommonJoinDelta(
                    context, newInnerJoin, joinOutputColRefs, tvrLeftFrom, rightDelta, tvrRightTo, leftDelta);

            // part2: add the padding result for non-match for left table.
            OptExpressionWithOutput leftPaddingDelta =
                    buildPaddingJoinDelta(context, input, join, joinOutputColRefs, leftDelta, tvrRightTo);

            // part3: add the padding result for non-match for right table.
            OptExpressionWithOutput rightPaddingDelta =
                    buildPaddingJoinDelta(context, input, join, joinOutputColRefs, rightDelta, tvrLeftTo);

            // merge all children into a single union operator
            List<OptExpressionWithOutput> newChildren = Lists.newArrayList(innerJoinPart);
            newChildren.add(leftPaddingDelta);
            newChildren.add(rightPaddingDelta);
            return buildUnionOperator(joinTvrOptMeta, joinOutputColRefs, newChildren);
        } else {
            throw new IllegalStateException(
                    "Unsupported join type for TVR: " + join.getJoinType() + " in " + input);
        }
    }

    private MaterializedView getTvrTargetMV(OptimizerContext context) {
        TvrOptContext tvrOptContext = context.getTvrOptContext();
        MaterializedView resultTable = tvrOptContext.getTvrTargetMV();
        if (resultTable == null) {
            throw new IllegalStateException("TVR target MV is not set in session variable");
        }
        List<Column> mvColumns = resultTable.getFullSchema();
        if (mvColumns.stream().noneMatch(col -> col.getName().equals(TvrOpUtils.COLUMN_ROW_ID))) {
            throw new IllegalStateException("TVR target MV should have a row id column: " + resultTable.getName());
        }
        return resultTable;
    }
}
