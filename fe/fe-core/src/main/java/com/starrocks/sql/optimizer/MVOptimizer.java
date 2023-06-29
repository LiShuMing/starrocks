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

package com.starrocks.sql.optimizer;

import com.google.api.client.util.Sets;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.base.OutputPropertyGroup;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;

import java.util.List;
import java.util.Set;

/**
 * Optimizer's entrance class
 */
public class MVOptimizer {

    public MVOptimizer() {
    }

    public OptExpression extractBestPlanWithMV(PhysicalPropertySet requiredProperty,
                                               Group rootGroup) {
        Set<Integer> groupIds = Sets.newHashSet();
        if (!containMaterializedView(rootGroup, requiredProperty, groupIds)) {
            return null;
        }
        return extractBestPlanWithMVImpl(requiredProperty, rootGroup, groupIds);
    }

    private OptExpression extractBestPlan(PhysicalPropertySet requiredProperty,
                                          Group rootGroup) {
        GroupExpression groupExpression = rootGroup.getBestExpression(requiredProperty);
        if (groupExpression == null) {
            String msg = "no executable plan for this sql. group: %s. required property: %s";
            throw new IllegalArgumentException(String.format(msg, rootGroup, requiredProperty));
        }
        List<PhysicalPropertySet> inputProperties = groupExpression.getInputProperties(requiredProperty);

        List<OptExpression> childPlans = Lists.newArrayList();
        for (int i = 0; i < groupExpression.arity(); ++i) {
            OptExpression childPlan = extractBestPlan(inputProperties.get(i), groupExpression.inputAt(i));
            childPlans.add(childPlan);
        }

        OptExpression expression = OptExpression.create(groupExpression.getOp(),
                childPlans);
        // record inputProperties at optExpression, used for planFragment builder to determine join type
        expression.setRequiredProperties(inputProperties);
        expression.setStatistics(groupExpression.getGroup().getStatistics());
        expression.setCost(groupExpression.getCost(requiredProperty));

        // When build plan fragment, we need the output column of logical property
        expression.setLogicalProperty(rootGroup.getLogicalProperty());
        return expression;
    }

    private OptExpression extractBestPlanWithMVImpl(PhysicalPropertySet requiredProperty,
                                                    Group group,
                                                    Set<Integer> groupIds) {
        if (groupIds.contains(group.getId())) {
            return extractBestPlan(requiredProperty, group);
        }
        Set<GroupExpression> groupExpressions = group.getSatisfyOutputPropertyGroupExpressions(requiredProperty);

        // Choose group expression which its child's group is in the `groupIds`
        GroupExpression mvGroupExpression = null;
        if (groupExpressions.size() == 1) {
            mvGroupExpression = groupExpressions.iterator().next();
        } else {
            for (GroupExpression groupExpression : groupExpressions) {
                if (groupExpression.arity() == 0) {
                    if (containMaterializedView(groupExpression.getOp())) {
                        mvGroupExpression = groupExpression;
                        break;
                    }
                } else {
                    for (int i = 0; i < groupExpression.arity(); ++i) {
                        if (groupIds.contains(groupExpression.getGroup().getId())) {
                            mvGroupExpression = groupExpression;
                            break;
                        }
                    }
                }
            }
        }
        if (mvGroupExpression == null) {
            String msg = "no executable plan for this sql. group: %s. required property: %s";
            throw new IllegalArgumentException(String.format(msg, group, requiredProperty));
        }
        List<PhysicalPropertySet> inputProperties = mvGroupExpression.getInputProperties(requiredProperty);
        List<OptExpression> childPlans = Lists.newArrayList();
        for (int i = 0; i < mvGroupExpression.arity(); ++i) {
            OptExpression childPlan = extractBestPlanWithMVImpl(inputProperties.get(i), mvGroupExpression.inputAt(i), groupIds);
            childPlans.add(childPlan);
        }

        OptExpression expression = OptExpression.create(mvGroupExpression.getOp(),
                childPlans);
        // record inputProperties at optExpression, used for planFragment builder to determine join type
        expression.setRequiredProperties(inputProperties);
        expression.setStatistics(mvGroupExpression.getGroup().getStatistics());
        expression.setCost(mvGroupExpression.getCost(requiredProperty));

        // When build plan fragment, we need the output column of logical property
        expression.setLogicalProperty(group.getLogicalProperty());
        return  expression;
    }

    private boolean containMaterializedView(Group group, PhysicalPropertySet outputProperty, Set<Integer> groupIds) {
        for (GroupExpression groupExpression : group.getSatisfyOutputPropertyGroupExpressions(outputProperty)) {
            if (groupExpression.getInputs().isEmpty()) {
                if (containMaterializedView(groupExpression.getOp())) {
                    groupIds.add(group.getId());
                    return true;
                }
            } else if (groupExpression.hasValidSubPlan()) {
                if (containMaterializedView(groupExpression, outputProperty, groupIds)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean containMaterializedView(GroupExpression groupExpression,
                                            PhysicalPropertySet outputProperty,
                                            Set<Integer> groupIds) {
        for (OutputPropertyGroup outputPropertyGroup : groupExpression.getChildrenOutputProperties(outputProperty)) {
            List<PhysicalPropertySet> childrenOutputProperties = outputPropertyGroup.getChildrenOutputProperties();
            for (int childIndex = 0; childIndex < groupExpression.arity(); ++childIndex) {
                if (containMaterializedView(groupExpression.inputAt(childIndex),
                        childrenOutputProperties.get(childIndex), groupIds)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean containMaterializedView(Operator op) {
        if (op instanceof LogicalScanOperator) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) op;
            if (scanOperator.getTable().isMaterializedView()) {
                return true;
            }
        }
        return false;
    }
}
