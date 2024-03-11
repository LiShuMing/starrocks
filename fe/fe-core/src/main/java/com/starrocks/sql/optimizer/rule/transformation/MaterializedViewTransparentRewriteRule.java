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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.Table;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MaterializedViewTransparentRewriteRule extends TransformationRule {

    public MaterializedViewTransparentRewriteRule() {
        super(RuleType.TF_MV_TRANSPARENT_REWRITE_RULE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        SessionVariable sessionVariable = context.getSessionVariable();
        if (!sessionVariable.isEnableTransparentMaterializedViewRewrite()) {
            return Collections.emptyList();
        }
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) input.getOp();
        Table table = olapScanOperator.getTable();
        if (!table.isMaterializedView()) {
            return Collections.emptyList();
        }
        MaterializedView mv = (MaterializedView) table;
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        List<MvPlanContext> mvPlanContexts = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv,
                sessionVariable.isEnableMaterializedViewPlanCache());
        if (CollectionUtils.isEmpty(mvPlanContexts)) {
            return Collections.emptyList();
        }
        MvPlanContext mvPlanContext = mvPlanContexts.get(0);
        if (!mvPlanContext.isValidMvPlan() || mvPlanContext.getLogicalPlan() == null) {
            return Collections.emptyList();
        }
        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        Set<Table> queryTables = MvUtils.getAllTables(mvPlan).stream().collect(Collectors.toSet());
        MaterializationContext materializationContext = MvRewritePreprocessor.buildMaterializationContext(context,
                columnRefFactory, mv, mvPlanContext, queryTables);
        OptExpression transparentPlan = MvPartitionCompensator.getMvTransparentPlan(materializationContext, mvPlan);
        if (transparentPlan != null) {
            return Collections.singletonList(transparentPlan);
        }
        return Collections.emptyList();
    }
}
