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

package com.starrocks.sql.optimizer.rule.tvr.common;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for TVR operations.
 */
public class TvrOpUtils {
    public static final String COLUMN_ROW_ID = "__ROW_ID__";
    public static final String COLUMN_AGG_STATE_PREFIX = "__AGG_STATE";
    public static final String TVR_COLUMN_PREFIX = "__TVR";

    public static String getTvrAggStateColumnName(FunctionCallExpr functionCallExpr) {
        // TODO: format functionCallExpr to a more readable name
        // agg_state column name is like __AGG_STATE_<agg_func_name>
        String exprFuncName = AstToStringBuilder.getAliasName(functionCallExpr, false,
                false);
        return String.format("%s_%s", COLUMN_AGG_STATE_PREFIX, exprFuncName);
    }

    public static ScalarOperator buildRowIdColumnOperator(List<ScalarOperator> uniqueKeys) {
        // build row id operator for agg state table
        Type[] argTypes = uniqueKeys.stream()
                .map(x -> Type.VARCHAR)
                .toArray(Type[]::new);
        Function newFunc = Expr.getBuiltinFunction(FunctionSet.ROW_FINGERPRINT, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (newFunc == null) {
            throw new IllegalArgumentException("Function " + FunctionSet.ROW_FINGERPRINT + " not found");
        }
        // Add cast operator if input types are not VARCHAR
        List<ScalarOperator> castedUniqueKeys = uniqueKeys.stream()
                .map(uniqueKey -> {
                    if (!uniqueKey.getType().isStringType()) {
                        return new CastOperator(Type.VARCHAR, uniqueKey, true);
                    } else {
                        return uniqueKey;
                    }
                })
                .collect(Collectors.toList());
        ScalarOperator rowIdScalarOp = new CallOperator(FunctionSet.ROW_FINGERPRINT,
                Type.VARCHAR, castedUniqueKeys, newFunc);
        return rowIdScalarOp;
    }

    public static FunctionCallExpr buildRowIdFuncExpr(List<Expr> uniqueKeys) {
        // This method is a placeholder for the actual implementation of building a row ID function.
        // The implementation would typically create a FunctionCallExpr that represents the row ID function
        // used in incremental view maintenance (IVM).
        List<Expr> newUniqueKeys = uniqueKeys.stream()
                .map(key -> {
                    if (!key.getType().isStringType()) {
                        return new CastExpr(Type.VARCHAR, key);
                    } else {
                        return key;
                    }
                })
                .collect(Collectors.toList());
        return new FunctionCallExpr(FunctionSet.ROW_FINGERPRINT, newUniqueKeys);
    }

    public static ScalarOperator buildStateUnionScalarOperator(CallOperator aggFunc,
                                                               ScalarOperator intermediateAggScalarOp,
                                                               ScalarOperator aggStateAggStateColumnRef) {
        Preconditions.checkArgument(intermediateAggScalarOp.getType().equals(aggStateAggStateColumnRef.getType()),
                "The type of intermediateAggScalarOp and aggStateTableRowIdScalarOp must be the same");
        // build row id operator for agg state table
        Type[] argTypes = new Type[] { intermediateAggScalarOp.getType(), aggStateAggStateColumnRef.getType() };
        // get the state union function name
        String origAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunc.getFnName());
        String stateUnionFunctionName = AggStateUtils.stateUnionFunctionName(origAggFuncName);
        Function newFunc = Expr.getBuiltinFunction(stateUnionFunctionName, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (newFunc == null) {
            throw new IllegalArgumentException("Function " + stateUnionFunctionName + " not found");
        }
        return new CallOperator(stateUnionFunctionName, intermediateAggScalarOp.getType(),
                List.of(intermediateAggScalarOp, aggStateAggStateColumnRef), newFunc);
    }

    public static ScalarOperator buildRowIdEqBinaryPredicateOp(ColumnRefOperator aggStateRowIdScalarOp,
                                                               List<ScalarOperator> uniqueKeys) {
        // build row id operator for agg state table
        ScalarOperator deltaInputRowIdScalarOp = TvrOpUtils.buildRowIdColumnOperator(uniqueKeys);
        BinaryPredicateOperator eqBinaryPredicateOperator =
                new BinaryPredicateOperator(BinaryType.EQ, aggStateRowIdScalarOp, deltaInputRowIdScalarOp);
        return eqBinaryPredicateOperator;
    }
}
