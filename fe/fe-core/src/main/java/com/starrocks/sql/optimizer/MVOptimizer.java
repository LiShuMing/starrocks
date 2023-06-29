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
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.base.OutputPropertyGroup;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * MVOptimizer is used to rewrite queries using materialized views even when the estimated query cost of the unrewritten query
 * is lower.
 */
public class MVOptimizer {

    public MVOptimizer() {
    }

    class PathContext {
        private double totalCost;
        private final Set<Integer> mvGroupPath;
        private final Set<GroupExpression> mvGroupExpressionPath;
        public PathContext() {
            this.totalCost = 0;
            this.mvGroupPath = Sets.newHashSet();
            this.mvGroupExpressionPath = Sets.newHashSet();
        }
        public PathContext(PathContext context) {
            this.totalCost = context.totalCost;
            this.mvGroupPath = new HashSet<>(context.getMvGroupPath());
            this.mvGroupExpressionPath = new HashSet<>(context.getMvGroupExpressionPath());
        }
        public void addMVPath(GroupExpression groupExpression) {
            this.mvGroupPath.add(groupExpression.getGroup().getId());
            this.mvGroupExpressionPath.add(groupExpression);
        }
        public Set<Integer> getMvGroupPath() {
            return this.mvGroupPath;
        }
        public Set<GroupExpression> getMvGroupExpressionPath() {
            return this.mvGroupExpressionPath;
        }
        public double getTotalCost() {
            return totalCost;
        }
        public void addCost(double cost) {
            this.totalCost += cost;
        }
    }

    public OptExpression extractBestPlanWithMV(PhysicalPropertySet requiredProperty,
                                               Group rootGroup,
                                               String mvRewriteMode) {
        OptExpression result = extractBestPlan(requiredProperty, rootGroup);
        // If the lowest cost plan contains mv, return it directly.
        if (containMaterializedView(result)) {
            return result;
        }

        List<PhysicalPropertySet> outputProperties =
                rootGroup.getSatisfyRequiredPropertyGroupExpressions(requiredProperty);

        boolean hasMVPlan = false;
        // NOTE: now we use a simple policy to choose the path
        PathContext pathContext = null;
        List<PathContext> mvPaths = Lists.newArrayList();
        PathContext context = new PathContext();
        for (PhysicalPropertySet outputProperty : outputProperties) {
            if (findMVPath(rootGroup, outputProperty, context, mvPaths, true)) {
                hasMVPlan = true;
            }
        }
        if (mvPaths.isEmpty()) {
            hasMVPlan = false;
        } else {
            // TODO: Now the choose method is simple and not exact, just find a lowest cost plan which
            //  contains a materialized view in the shortest path.
            pathContext = mvPaths.get(0);
            for (int i = 1; i < mvPaths.size(); i++) {
                if (mvPaths.get(i).getTotalCost() < pathContext.getTotalCost()) {
                    pathContext = mvPaths.get(i);
                }
            }
        }

        // If there is no plans containing mv, handle it for different mode:
        // - force          : return the lowest cost plan:
        // - force_or_error : throw exception if no plans even in `force` mode
        if (!hasMVPlan || pathContext == null) {
            if (mvRewriteMode.equalsIgnoreCase(SessionVariable.REWRITE_MODE_FORCE_OR_ERROR)) {
                throw new IllegalArgumentException("no executable plan with materialized view for this sql in `force_or_error` " +
                        "mode.");
            } else {
                return result;
            }
        }

        OptExpression forceResult = null;
        for (PhysicalPropertySet outputProperty : outputProperties) {
            forceResult = extractBestPlanWithMVImpl(outputProperty, rootGroup, pathContext);
            if (forceResult != null) {
                break;
            }
        }

        // this should never happen.
        if (forceResult == null || !containMaterializedView(forceResult)) {
            throw new IllegalArgumentException(String.format("no executable plan with materialized view for this sql in `%s` " +
                            "mode.",
                    mvRewriteMode));
        }
        return forceResult;
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
                                                    PathContext pathContext) {
        if (!pathContext.getMvGroupPath().contains(group.getId())) {
            return extractBestPlan(requiredProperty, group);
        }
        Set<GroupExpression> groupExpressions = group.getSatisfyOutputPropertyGroupExpressions(requiredProperty);

        // Choose group expression which its child's group is in the `groupIds`
        GroupExpression mvGroupExpression = null;
        for (GroupExpression groupExpression : groupExpressions) {
            if (pathContext.getMvGroupExpressionPath().contains(groupExpression)) {
                mvGroupExpression = groupExpression;
                break;
            }
        }
        if (mvGroupExpression == null) {
            return null;
        }
        List<PhysicalPropertySet> inputProperties = mvGroupExpression.getInputProperties(requiredProperty);
        List<OptExpression> childPlans = Lists.newArrayList();
        for (int i = 0; i < mvGroupExpression.arity(); ++i) {
            OptExpression childPlan = extractBestPlanWithMVImpl(inputProperties.get(i),
                    mvGroupExpression.inputAt(i), pathContext);
            if (childPlan == null) {
                return null;
            }
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

    private boolean findMVPath(Group group,
                               PhysicalPropertySet outputProperty,
                               PathContext context,
                               List<PathContext> mvPaths,
                               boolean isRootGroup) {
        Set<GroupExpression> groupExpressions = group.getSatisfyOutputPropertyGroupExpressions(outputProperty);
        for (GroupExpression groupExpression : groupExpressions) {
            PathContext newContext = new PathContext(context);
            newContext.addCost(CostModel.calculateCost(groupExpression));
            boolean foundMV = false;
            if (groupExpression.getInputs().isEmpty()) {
                if (containMaterializedView(groupExpression.getOp())) {
                    foundMV = true;
                }
            } else if (groupExpression.hasValidSubPlan()) {
                if (findMVPath(groupExpression, outputProperty, newContext, mvPaths)) {
                    foundMV = true;
                }
            }

            if (foundMV) {
                newContext.addMVPath(groupExpression);
                if (isRootGroup) {
                    mvPaths.add(newContext);
                }
                return true;
            }
        }
        return false;
    }

    private boolean findMVPath(GroupExpression groupExpression,
                               PhysicalPropertySet outputProperty,
                               PathContext context,
                               List<PathContext> mvPaths) {
        for (OutputPropertyGroup outputPropertyGroup : groupExpression.getChildrenOutputProperties(outputProperty)) {
            List<PhysicalPropertySet> childrenOutputProperties = outputPropertyGroup.getChildrenOutputProperties();
            PathContext newPathContext = new PathContext(context);
            boolean foundMV = false;
            for (int childIndex = 0; childIndex < groupExpression.arity(); ++childIndex) {
                if (findMVPath(groupExpression.inputAt(childIndex),
                        childrenOutputProperties.get(childIndex), newPathContext, mvPaths, false)) {
                    context.addMVPath(groupExpression);
                    foundMV = true;
                }
            }
            if (foundMV) {
                return true;
            }
        }
        return false;
    }

    private boolean containMaterializedView(Operator op) {
        if (op instanceof PhysicalScanOperator) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) op;
            if (scanOperator.getTable().isMaterializedView()) {
                return true;
            }
        }
        return false;
    }

    private boolean containMaterializedView(OptExpression optExpression) {
        if (containMaterializedView(optExpression.getOp())) {
            return true;
        }
        for (OptExpression child : optExpression.getInputs()) {
            if (containMaterializedView(child.getOp())) {
                return true;
            }
        }
        return false;
    }
}
