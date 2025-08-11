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

import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrChangeType;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOpUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TvrAggregateRule extends TvrTransformationRule {

    public TvrAggregateRule() {
        super(RuleType.TF_TVR_AGGREGATE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!isSupportedTvr(input)) {
            return false;
        }
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
        return true;
    }

    protected OptExpression doTransformWithMonotonic(OptimizerContext optimizerContext,
                                                     OlapTable aggStateTable,
                                                     LogicalAggregationOperator inputAggOperator,
                                                     OptExpression input) {
        final ColumnRefFactory columnRefFactory = optimizerContext.getColumnRefFactory();

        Preconditions.checkArgument(aggStateTable != null,
                "Aggregate state table must not be null for TVR aggregate rule");
        final Column tvrColumnRowId = aggStateTable.getColumn(TvrOpUtils.COLUMN_ROW_ID);
        Preconditions.checkArgument(tvrColumnRowId != null,
                "TVR column row id must exist in agg state table");
        final LogicalOlapScanOperator aggStateOlapScanOperator = MvRewritePreprocessor.createScanMvOperator(
                aggStateTable, columnRefFactory, Sets.newHashSet());
        ColumnRefOperator aggStateRowIdColumnRef =
                aggStateOlapScanOperator.getColumnMetaToColRefMap().get(tvrColumnRowId);
        List<Column> aggStateTableFullColumns = aggStateTable.getFullSchema();
        List<Column> aggStateTableColumns = aggStateTableFullColumns.stream()
                .filter(col -> col.getName().startsWith(TvrOpUtils.COLUMN_AGG_STATE_PREFIX))
                .collect(Collectors.toList());
        // collect agg state table's aggregate agg state columns
        Map<Column, ColumnRefOperator> aggStateTableColumnMetaToColRefMap =
                aggStateOlapScanOperator.getColumnMetaToColRefMap();
        List<ColumnRefOperator> aggStateTableColumnRefOperators = aggStateTableColumns.stream()
                .map(aggStateTableColumnMetaToColRefMap::get)
                .collect(Collectors.toList());

        // collect input aggregator's grouping keys and aggregations
        final List<ColumnRefOperator> groupingKeys = inputAggOperator.getGroupingKeys();
        final Map<ColumnRefOperator, CallOperator> inputAggMap = inputAggOperator.getAggregations();
        Preconditions.checkArgument(aggStateTableColumns.size() == inputAggMap.size(),
                String.format("Aggregate state table columns size %s must match input aggregate map size %s",
                        aggStateTableColumns.size(), inputAggMap.size()));

        // build eq predicate for delta changes by row id
        List<ScalarOperator> inputAggUniqueKeys = inputAggOperator.getGroupingKeys()
                .stream()
                .map(col -> (ScalarOperator) col)
                .collect(Collectors.toList());
        ScalarOperator eqRowIdOperator = TvrOpUtils.buildRowIdEqBinaryPredicateOp(aggStateRowIdColumnRef,
                inputAggUniqueKeys);

        // old aggregate function to new column ref operator map
        Map<ScalarOperator, ColumnRefOperator> oldToNewColumnRefMap = Maps.newHashMap();
        // output intermediate results rather than final result
        Map<ColumnRefOperator, CallOperator> intermediateAggMap = inputAggMap.entrySet()
                .stream()
                .map(e -> {
                    ColumnRefOperator orgColumnRefOperator = e.getKey();
                    CallOperator call = e.getValue();
                    CallOperator intermediateStateAggregateFunc =
                            AggregateFunctionRollupUtils.getIntermediateStateAggregateFunc(call);
                    Preconditions.checkArgument(intermediateStateAggregateFunc != null,
                            "Intermediate state aggregate function should not be null for: %s", call);

                    // create a new column ref for the intermediate state aggregate function
                    ColumnRefOperator newColumnRefOperator =
                            columnRefFactory.create(orgColumnRefOperator.getName(),
                                    orgColumnRefOperator.getType(), orgColumnRefOperator.isNullable());

                    // map old column ref operator to new column ref operator
                    oldToNewColumnRefMap.put(call, newColumnRefOperator);

                    return Map.entry(newColumnRefOperator, intermediateStateAggregateFunc);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // build input aggregator intermediate aggregation operator
        LogicalAggregationOperator intermediateAggOperator = buildIntermediateAggOperator(groupingKeys,
                intermediateAggMap);

        // delta changes left join agg state scan operator
        LogicalJoinOperator deltaJoinOperator = new LogicalJoinOperator(
                JoinOperator.LEFT_OUTER_JOIN, eqRowIdOperator);
        OptExpression deltaJoinOptExpression = OptExpression.createWithoutTvr(deltaJoinOperator,
                OptExpression.createWithoutTvr(intermediateAggOperator, input.getInputs()),
                OptExpression.createWithoutTvr(aggStateOlapScanOperator));

        // change the aggregation operator into project operator
        Map<ColumnRefOperator, ScalarOperator> projColumnRefMap  = Maps.newHashMap();

        // We can assume that the agg state table's columns are in the same order as the input aggregate map,
        // but this is a bit tricky.
        // TODO: We may align the agg state table's output columns with the insert planner's output column refs in order
        // to get the correct order.
        int i = 0;
        // map old column ref operator to new column ref operator
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : inputAggMap.entrySet()) {
            ColumnRefOperator oldColumnRef = entry.getKey();
            CallOperator callOperator = entry.getValue();

            // intermediate agg state column ref operator
            ColumnRefOperator intermediateAggColumnRef = oldToNewColumnRefMap.get(callOperator);
            if (intermediateAggColumnRef == null) {
                throw new IllegalStateException("New column ref operator should not be null for: " + callOperator);
            }
            ColumnRefOperator aggStateAggStateColumnRef =
                    aggStateTableColumnRefOperators.get(i);
            // _state_union(agg_state_row_id, <old column ref operator>)
            ScalarOperator stateUnionScalarOperator = TvrOpUtils.buildStateUnionScalarOperator(callOperator,
                    intermediateAggColumnRef, aggStateAggStateColumnRef);
            projColumnRefMap.put(oldColumnRef, stateUnionScalarOperator);
            i++;
        }
        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projColumnRefMap);
        return OptExpression.createWithoutTvr(logicalProjectOperator, deltaJoinOptExpression);
    }

    private LogicalAggregationOperator buildIntermediateAggOperator(List<ColumnRefOperator> groupingKeys,
                                                                    Map<ColumnRefOperator, CallOperator> aggMap) {
        LogicalAggregationOperator newAggOp = new LogicalAggregationOperator(AggType.GLOBAL,
                groupingKeys, aggMap);
        return newAggOp;
    }

    private OlapTable getAggregateStateTable(OptimizerContext optimizerContext) {
        // TODO: How to set the logical aggregate operator's aggregate state table?
        // get the aggregate state table
        // checks its schema consistent with the current aggregate's state
        String strMvId = optimizerContext.getSessionVariable().getTvrTargetMvId();
        if (strMvId == null) {
            throw new IllegalStateException("TVR target MV ID is not set in session variable");
        }
        MvId mvId = GsonUtils.GSON.fromJson(strMvId, MvId.class);
        if (mvId == null) {
            throw new IllegalStateException("Failed to parse TVR target MV ID from session variable: " + strMvId);
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
        if (db == null) {
            throw new IllegalStateException("Database with ID " + mvId.getDbId() + " not found");
        }
        OlapTable aggStateOlapTable = (OlapTable) db.getTable(mvId.getId());
        if (aggStateOlapTable == null) {
            throw new IllegalStateException("Aggregate state table with ID " + mvId.getId() + " not found in database "
                    + db.getFullName());
        }
        return aggStateOlapTable;
    }

    @Override
    public OptExpression doTransform(OptExpression input,
                                     OptimizerContext context,
                                     TvrChangeType tvrChangeType) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        // find the agg state table
        OlapTable aggStateOlapTable = getAggregateStateTable(context);
        // handle append only aggregate state table
        OptExpression deltaAggregate;
        if (tvrChangeType == TvrChangeType.MONOTONIC) {
            deltaAggregate = doTransformWithMonotonic(context, aggStateOlapTable, aggOp, input);
        } else {
            throw new IllegalStateException("Unsupported TVR change type for aggregate rule: " + tvrChangeType);
        }

        return deltaAggregate;
    }
}