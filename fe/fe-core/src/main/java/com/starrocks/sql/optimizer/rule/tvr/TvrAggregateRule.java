package com.starrocks.sql.optimizer.rule.tvr;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TvrAggregateRule extends TransformationRule {

    public TvrAggregateRule() {
        super(RuleType.TF_TVR_APPEND_ONLY_AGGREGATE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        List<ColumnRefOperator> groupBys = aggOp.getGroupingKeys();
        if (groupBys.isEmpty()) {
            return false;
        }
        // TODO: aggregate distinct
        if (aggOp.getAggregations().values().stream().anyMatch(call -> call.isDistinct())) {
            return false;
        }
        // TODO: aggregate having
        if (aggOp.getPredicate() != null) {
            return false;
        }

        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(input);
        if (scanOperators.size() != 1) {
            return false;
        }
        LogicalScanOperator scanOperator = scanOperators.get(0);
        if (scanOperator.getTvrTrait().isEmpty()) {
            return false;
        }
        if (!scanOperator.getTvrTrait().get().isAppendOnly()) {
            return false;
        }

        return true;
    }

    private AggregateFunction getIntermediateStateAggFunction(CallOperator call) {
        Preconditions.checkArgument(call.getFunction() instanceof AggregateFunction);
        if (AggregateFunctionRollupUtils.isSupportedAggFunctionPushDown(call)) {
            AggregateFunction aggFunction = (AggregateFunction) call.getFunction();
            return aggFunction;
        }
        // TODO: use agg_state to generate intermediate state agg function
        return null;
    }

    private boolean needsToGenerateIntermediateColumn(CallOperator call) {
        // If the agg function is not supported to push down, we need to generate intermediate column
        return !AggregateFunctionRollupUtils.isSupportedAggFunctionRollup(call);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        // aggregation
        Map<ColumnRefOperator, CallOperator> aggMap = aggOp.getAggregations();
        // output intermediate results rather than final result
        Map<ColumnRefOperator, CallOperator> newAggMap = aggMap.entrySet()
                .stream()
                .map(e -> {
                    CallOperator call = e.getValue();
                    AggregateFunction interStateAggFunction = getIntermediateStateAggFunction(call);
                    if (interStateAggFunction == null) {
                        return e;
                    }
                    CallOperator intermediateCall = new CallOperator(interStateAggFunction.getFunctionName().getFunction(),
                            interStateAggFunction.getReturnType(), call.getChildren(), interStateAggFunction);
                    return Map.entry(e.getKey(), intermediateCall);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        LogicalAggregationOperator newAggOp = new LogicalAggregationOperator(aggOp.getType(),
                aggOp.getGroupingKeys(), newAggMap);
        return List.of(OptExpression.create(newAggOp, input.getInputs().get(0)));
    }
}
