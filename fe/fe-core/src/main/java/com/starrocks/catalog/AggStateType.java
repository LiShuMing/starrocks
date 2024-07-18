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

package com.starrocks.catalog;

import com.google.api.client.util.Lists;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.thrift.TTypeDesc;

import java.util.List;

/**
 * Describes a MAP type. MAP types have a scalar key and an arbitrarily-typed value.
 */
public class AggStateType extends Type {

    // argument types
    @SerializedName(value = "argTypes")
    private List<Type> argTypes;
    // argument type nullables
    @SerializedName(value = "argTypeNullables")
    private List<Boolean> argTypeNullables;
    // result nullable
    @SerializedName(value = "resultNullable")
    private Boolean resultNullable;
    // agg function's name
    @SerializedName(value = "functionName")
    private String functionName;

    public AggStateType(String functionName,
                        List<Type> argTypes,
                        Boolean resultNullable) {
        java.util.Objects.requireNonNull(argTypes, "argTypes should not be null");
        this.functionName = functionName;
        this.argTypes = argTypes;
        this.resultNullable = resultNullable;
    }

    public AggStateType(String functionName,
                        List<Type> argTypes,
                        List<Boolean> argTypeNullables,
                        Boolean resultNullable) {
        java.util.Objects.requireNonNull(argTypes, "argTypes should not be null");
        java.util.Objects.requireNonNull(argTypeNullables, "argTypeNullables should not be null");
        if (argTypes.size() != argTypeNullables.size()) {
            throw new IllegalStateException("AggStateType's argTypes.size() [" + argTypes.size()
                    + "] != argTypeNullables.size() [" + argTypeNullables.size() + "]");
        }
        this.functionName = functionName;
        this.argTypes = argTypes;
        this.argTypeNullables = argTypeNullables;
        this.resultNullable = resultNullable;
    }

    public List<Type> getArgTypes() {
        return argTypes;
    }

    public List<Boolean> getArgTypeNullables() {
        return argTypeNullables;
    }

    public Boolean getResultNullable() {
        return resultNullable;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(argTypes, argTypeNullables, resultNullable, functionName);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AggStateType)) {
            return false;
        }
        AggStateType other = (AggStateType) o;
        int subTypeNumber = argTypeNullables.size();
        if (subTypeNumber != other.argTypeNullables.size()) {
            return false;
        }
        for (int i = 0; i < subTypeNumber; i++) {
            if (!argTypeNullables.get(i).equals(other.argTypeNullables.get(i))) {
                return false;
            }
            if (!argTypes.get(i).equals(other.argTypes.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toSql(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("AGG_STATE<").append(functionName).append("(");
        for (int i = 0; i < argTypes.size(); i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(argTypes.get(i).toSql());
            if (argTypeNullables.get(i)) {
                stringBuilder.append(" NULL");
            }
        }
        stringBuilder.append(", result_nullable=");
        stringBuilder.append(resultNullable);
        stringBuilder.append(">");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    protected String prettyPrint(int lpad) {
        return Strings.repeat(" ", lpad) + toSql();
    }

    @Override
    public void toThrift(TTypeDesc result) {
    }

    @Override
    public boolean isFullyCompatible(Type other) {
        return false;
    }

    @Override
    public AggStateType clone() {
        AggStateType clone = (AggStateType) super.clone();
        clone.functionName = this.functionName;
        clone.resultNullable = this.resultNullable;
        clone.argTypes = Lists.newArrayList(this.argTypes);
        clone.argTypeNullables = Lists.newArrayList(this.argTypeNullables);
        return clone;
    }

    public String toMysqlDataTypeString() {
        return "binary";
    }

    // This implementation is the same as BE schema_columns_scanner.cpp type_to_string
    public String toMysqlColumnTypeString() {
        return toSql();
    }

    @Override
    protected String toTypeString(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "agg_state<...>";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("AGG_STATE<").append(functionName).append("(");
        for (int i = 0; i < argTypes.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(argTypes.get(i).toSql());
            if (argTypeNullables.get(i)) {
                sb.append(" NULL");
            }
        }
        if (resultNullable) {
            sb.append(" NULL");
        }
        sb.append(">");
        return sb.toString();
    }
}

