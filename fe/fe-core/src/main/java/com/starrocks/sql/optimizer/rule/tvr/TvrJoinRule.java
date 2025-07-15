package com.starrocks.sql.optimizer.rule.tvr;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.List;


public class TvrJoinRule extends TransformationRule {

    public TvrJoinRule() {
        super(RuleType.TF_TVR_APPEND_ONLY_JOIN, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private boolean isSupportedTvr(LogicalScanOperator scanOperator) {
        return scanOperator.getTvrTrait().isPresent() && scanOperator.getTvrTrait().get().isAppendOnly();
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(input);
        if (scanOperators.stream().anyMatch(scan -> !isSupportedTvr(scan))) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = input.getOp().cast();
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(input);
        Preconditions.checkState(scanOperators.size() == 2);

        LogicalScanOperator leftScan = scanOperators.get(0);
        LogicalScanOperator rightScan = scanOperators.get(1);

        LogicalAggregationOperator leftAgg = new LogicalAggregationOperator(
                join.getGroupingKeys(),
                join.getAggregations(),
                join.getPredicate(),
                join.getProjection());

        LogicalAggregationOperator rightAgg = new LogicalAggregationOperator(
                join.getGroupingKeys(),
                join.getAggregations(),
                join.getPredicate(),
                join.getProjection());

        OptExpression leftInput = OptExpression.create(leftAgg, OptExpression.create(leftScan));
        OptExpression rightInput = OptExpression.create(rightAgg, OptExpression.create(rightScan));

        return List.of(OptExpression.create(join, leftInput, rightInput));
    }
}
