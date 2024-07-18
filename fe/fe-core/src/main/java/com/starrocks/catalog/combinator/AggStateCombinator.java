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

package com.starrocks.catalog.combinator;

import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.AggStateType;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class AggStateCombinator extends AggregateFunction {
    private final AggregateFunction aggFunc;

    public AggStateCombinator(AggregateFunction aggFunc, AggStateType aggStateType) {
        super(new FunctionName(aggFunc.functionName() + FunctionSet.AGG_STATE_SUFFIX), aggStateType.getArgTypes(), aggStateType,
                aggStateType, false);
        this.aggFunc = Objects.requireNonNull(aggFunc, "nested can not be null");
    }

    public static AggStateCombinator of(AggregateFunction aggFunc) {
        List<Type> argTypes = Arrays.asList(aggFunc.getArgs());
        Type aggStateType = new AggStateType(aggFunc.functionName(), argTypes, aggFunc.isNullable());
        return new AggStateCombinator(aggFunc, (AggStateType) aggStateType);
    }
}
