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

package com.starrocks.sql.analyzer.mv;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOpUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is responsible for analyzing and rewriting the query statement for IVM (Incremental View Maintenance) refresh.
 */
public class IVMAnalyzer {
    public record AggFunctionInfo(FunctionCallExpr aggFunc,
                                  String aggFuncName,
                                  FunctionCallExpr newAggFunc,
                                  String newAggFuncName) {}

    private static MaterializedView.RefreshMode getRefreshMode(CreateMaterializedViewStatement statement) {
        Map<String, String> properties = statement.getProperties();
        if (properties == null) {
            properties = Maps.newHashMap();
            statement.setProperties(properties);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REFRESH_MODE)) {
            String mode = properties.get(PropertyAnalyzer.PROPERTIES_REFRESH_MODE);
            return MaterializedView.RefreshMode.valueOf(mode.toUpperCase());
        } else {
            // Default to INCREMENTAL
            return MaterializedView.RefreshMode.defaultValue();
        }
    }

    private static FunctionCallExpr buildIntermediateAggregateFunc(FunctionCallExpr aggFuncExpr) {
        // <func>_agg_combine(<args>)
        String aggFuncName = aggFuncExpr.getFnName().getFunction();
        String aggStateFuncName = AggStateUtils.aggStateCombineFunctionName(aggFuncName);
        FunctionCallExpr aggStateFuncExpr = new FunctionCallExpr(aggStateFuncName, aggFuncExpr.getChildren());
        return aggStateFuncExpr;
    }

    private static FunctionCallExpr buildStateMergeFuncExpr(AggFunctionInfo aggFunctionInfo) {
        String aggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunctionInfo.aggFuncName);
        String stateMergeFuncName = AggStateUtils.stateMergeFunctionName(aggFuncName);
        SlotRef slotRef = new SlotRef(null, aggFunctionInfo.newAggFuncName);
        return new FunctionCallExpr(stateMergeFuncName, List.of(slotRef));
    }

    public static Optional<QueryStatement> rewrite(ConnectContext connectContext,
                                                   CreateMaterializedViewStatement statement) {
        MaterializedView.RefreshMode refreshMode = getRefreshMode(statement);
        QueryStatement queryStatement = statement.getQueryStatement();
        if (!refreshMode.isIncremental()) {
            return Optional.empty();
        }
        QueryRelation queryRelation = queryStatement.getQueryRelation();
        return rewriteImpl(connectContext, statement, queryStatement, queryRelation);
    }

    /**
     * Rewrite the query relation for incremental view maintenance.
     * NOTE: Only return non-empty Optional if the query relation has been rewritten, otherwise return empty Optional.
     */
    private static Optional<QueryStatement> rewriteImpl(ConnectContext connectContext,
                                                        CreateMaterializedViewStatement statement,
                                                        QueryStatement queryStatement,
                                                        QueryRelation queryRelation) {
        if (queryRelation instanceof SelectRelation) {
            SelectRelation selectRelation = (SelectRelation) queryRelation;
            // For SelectRelation, we rewrite it to support incremental view maintenance.
            return rewriteSelectRelation(connectContext, statement, queryStatement, selectRelation);
        } else if (queryRelation instanceof SetOperationRelation) {
            return rewriteSetRelation(connectContext, statement, queryStatement,
                    (SetOperationRelation) queryRelation);
        } else if (queryRelation instanceof SubqueryRelation) {
            return rewriteSubqueryRelation(connectContext, statement, queryStatement,
                    (SubqueryRelation) queryRelation);
        } else {
            throw new SemanticException("IVMAnalyzer can only handle SelectRelation/UnionRelation, but got: %s",
                    queryRelation.getClass().getSimpleName());
        }
    }

    private static Optional<QueryStatement> rewriteSubqueryRelation(ConnectContext connectContext,
                                                                    CreateMaterializedViewStatement statement,
                                                                    QueryStatement queryStatement,
                                                                    SubqueryRelation subqueryRelation) {
        QueryStatement subQueryStatement = subqueryRelation.getQueryStatement();
        Optional<QueryStatement> rewritten =
                rewriteImpl(connectContext, statement, queryStatement, subQueryStatement.getQueryRelation());
        if (rewritten.isPresent()) {
            throw new SemanticException("IVMAnalyzer does not support subquery relation, " +
                    "but got: %s", subqueryRelation.getClass().getSimpleName());
        }
        return Optional.empty();
    }

    private static Optional<QueryStatement> rewriteSetRelation(ConnectContext connectContext,
                                                               CreateMaterializedViewStatement statement,
                                                               QueryStatement queryStatement,
                                                               SetOperationRelation setOperationRelation) {
        if (!(setOperationRelation instanceof UnionRelation)) {
            throw new SemanticException("IVMAnalyzer can only handle UnionRelation, " +
                    "but got: %s", setOperationRelation.getClass().getSimpleName());
        }
        UnionRelation unionRelation = (UnionRelation) setOperationRelation;
        // For UnionRelation, we only handle the case where all children are SelectRelation.
        List<QueryRelation> children = unionRelation.getRelations();
        for (QueryRelation child : children) {
            if (!(child instanceof SelectRelation)) {
                throw new SemanticException("IVMAnalyzer can only handle SelectRelation/UnionRelation, but got: %s",
                        child.getClass().getSimpleName());
            }
            SelectRelation selectChild = (SelectRelation) child;
            List<FunctionCallExpr> aggregateExprs = selectChild.getAggregate();
            if (CollectionUtils.isNotEmpty(aggregateExprs)) {
                throw new SemanticException("UnionRelation in IVMAnalyzer should not have aggregate functions, " +
                        "but got: %s", aggregateExprs);
            }
        }
        return Optional.empty();
    }

    private static Optional<QueryStatement> rewriteSelectRelation(ConnectContext connectContext,
                                                                  CreateMaterializedViewStatement statement,
                                                                  QueryStatement queryStatement,
                                                                  SelectRelation selectRelation) {
        List<FunctionCallExpr> aggregateExprs = selectRelation.getAggregate();
        if (CollectionUtils.isEmpty(aggregateExprs)) {
            return Optional.empty();
        }

        List<Expr> groupByExprs = selectRelation.getGroupBy();
        if (CollectionUtils.isEmpty(groupByExprs)) {
            // If there are no group by expressions, we cannot apply IVM optimizations.
            throw new SemanticException("IVMAnalyzer requires group by expressions for incremental view maintenance.");
        }
        // new aggregate functions
        List<AggFunctionInfo> newAggFuncInfos = Lists.newArrayList();
        ExprSubstitutionMap substitutionMap = new ExprSubstitutionMap(false);
        for (FunctionCallExpr aggFuncExpr : aggregateExprs) {
            String aggFuncName = aggFuncExpr.getFnName().getFunction();
            // build intermediate aggregate function
            FunctionCallExpr intermediateAggFuncExpr = buildIntermediateAggregateFunc(aggFuncExpr);
            String newAggFuncName = TvrOpUtils.getTvrAggStateColumnName(aggFuncExpr);

            AggFunctionInfo aggFunctionInfo = new AggFunctionInfo(aggFuncExpr, aggFuncName,
                    intermediateAggFuncExpr, newAggFuncName);
            FunctionCallExpr stateMergeFuncExpr = buildStateMergeFuncExpr(aggFunctionInfo);

            newAggFuncInfos.add(aggFunctionInfo);
            substitutionMap.put(aggFuncExpr, stateMergeFuncExpr);
        }

        List<FunctionCallExpr> newAggFuncs = newAggFuncInfos.stream()
                .map(AggFunctionInfo::newAggFunc)
                .toList();
        selectRelation.setAggregate(newAggFuncs);

        // Build the row ID function expression
        FunctionCallExpr rowIdFuncExpr = TvrOpUtils.buildRowIdFuncExpr(groupByExprs);
        SelectList selectList = selectRelation.getSelectList();
        List<SelectListItem> newItems = Lists.newArrayList();
        // add row_id func expr
        newItems.add(new SelectListItem(rowIdFuncExpr, TvrOpUtils.COLUMN_ROW_ID));
        selectList.getItems()
                .stream()
                .forEach(item -> {
                    Expr newExpr = substituteWithMap(item.getExpr().clone(), substitutionMap);
                    newItems.add(new SelectListItem(newExpr, item.getAlias()));
                });
        // add agg state func expr
        for (AggFunctionInfo aggFunctionInfo : newAggFuncInfos) {
            newItems.add(new SelectListItem(aggFunctionInfo.newAggFunc, aggFunctionInfo.newAggFuncName));
        }
        selectList.setItems(newItems);

        List<Expr> newOutputExpressions = Lists.newArrayList();
        newOutputExpressions.add(rowIdFuncExpr);
        selectRelation.getOutputExpression()
                .stream()
                .forEach(expr -> {
                    Expr newExpr = substituteWithMap(expr.clone(), substitutionMap);
                    newOutputExpressions.add(newExpr);
                });
        // add extra exprs
        newAggFuncInfos.stream()
                .forEach(aggFunctionInfo -> newOutputExpressions.add(aggFunctionInfo.newAggFunc));
        selectRelation.setOutputExpr(newOutputExpressions);

        return Optional.of(queryStatement);
    }

    private static Expr substituteWithMap(Expr expr, ExprSubstitutionMap substitutionMap) {
        return expr.substitute(substitutionMap);
    }
}
