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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class TvrJoinRule extends TvrTransformationRule {

    public TvrJoinRule() {
        super(RuleType.TF_TVR_JOIN, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!isSupportedTvr(input)) {
            return false;
        }
        LogicalJoinOperator join = input.getOp().cast();
        if (!join.isInnerOrCrossJoin()) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = input.getOp().cast();
        OptExpression leftDelta = input.inputAt(0);
        TvrOptMeta leftMeta = leftDelta.getTvrMeta();
        OptExpression rightDelta = input.inputAt(1);
        TvrOptMeta rightMeta = rightDelta.getTvrMeta();

        // TODO: use left table's tvrOptMeta as the root
        // TODO: Use mv as the history state instead of recomputing.
        List<ColumnRefOperator> originalOutputColRefs = input.getRowOutputInfo().getOutputColRefs();
        TvrOptExpression tvrLeftFrom = leftMeta.getFrom();
        TvrOptExpression tvrLeftTo = leftMeta.getTo();
        TvrOptExpression tvrRightFrom = rightMeta.getFrom();
        TvrOptExpression tvrRightTo = rightMeta.getTo();

        // from opt
        TvrLazyOptExpression fromJoin = TvrLazyOptExpression.of(() -> {
            OptExpressionWithOutput fromOpt = newJoinOperator(context, originalOutputColRefs, join, tvrLeftFrom.optExpression(),
                    tvrRightFrom.optExpression());
            return new TvrOptExpression(tvrLeftFrom.tvrVersionRange(), fromOpt.optExpression());
        });
        // to opt
        TvrLazyOptExpression toJoin = TvrLazyOptExpression.of(() -> {
            OptExpressionWithOutput toOpt = newJoinOperator(context, originalOutputColRefs, join, tvrLeftTo.optExpression(),
                    tvrRightTo.optExpression());
            return new TvrOptExpression(tvrLeftTo.tvrVersionRange(), toOpt.optExpression());
        });
        // root opt group
        TvrOptMeta rootOptMeta = new TvrOptMeta(fromJoin, toJoin);

        // delta join
        OptExpression deltaJoin = null;
        if (join.isInnerOrCrossJoin()) {
            OptExpressionWithOutput deltaOutput1 =
                    newJoinOperator(context, originalOutputColRefs, join, tvrLeftFrom.optExpression(), rightDelta);
            OptExpressionWithOutput deltaOutput2 =
                    newJoinOperator(context, originalOutputColRefs, join, leftDelta, tvrRightTo.optExpression());
            deltaJoin = newUnionOperator(rootOptMeta, originalOutputColRefs, Lists.newArrayList(deltaOutput1, deltaOutput2));
        } else {
            throw new IllegalStateException(
                    "Unsupported join type for TVR: " + join.getJoinType() + " in " + input);
        }

        return List.of(deltaJoin);
    }
}
