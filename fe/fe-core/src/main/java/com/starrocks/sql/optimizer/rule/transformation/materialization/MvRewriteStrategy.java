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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerConfig;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.rule.RuleSetType;

public class MvRewriteStrategy {
    public boolean enableMaterializedViewRewrite = false;

    public boolean enableEagerRewrite = false;
    public boolean enableDefaultRewrite = false;
    // Whether enable force rewrite for query plans with join operator by rule based mv rewrite
    public boolean enableForceRBORewrite = false;

    // rbo config
    public boolean enableRBOSingleViewRewrite = false;
    public boolean enableRBOSingleTableRewrite = false;

    // cbo config
    public boolean enableCBOSingleTableRewrite = false;
    public boolean enableCBOMultiTableRewrite = false;

    static class MvStrategyArbitrator {
        private final OptimizerConfig optimizerConfig;
        private final OptimizerContext optimizerContext;
        private final SessionVariable sessionVariable;

        public MvStrategyArbitrator(OptimizerContext optimizerContext,
                                    ConnectContext connectContext) {
            this.optimizerContext = optimizerContext;
            this.optimizerConfig = optimizerContext.getOptimizerConfig();
            // from connectContext rather than optimizerContext
            this.sessionVariable = connectContext.getSessionVariable();
        }

        private boolean isEnableMaterializedViewRewrite() {
            if (sessionVariable.isDisableMaterializedViewRewrite()) {
                return false;
            }
            // if disable isEnableMaterializedViewRewrite, return false.
            if (!sessionVariable.isEnableMaterializedViewRewrite()) {
                return false;
            }
            // if mv candidates are empty, return false.
            if (optimizerContext.getCandidateMvs() == null ||
                    optimizerContext.getCandidateMvs().isEmpty()) {
                return false;
            }
            if (optimizerConfig.isRuleSetTypeDisable(RuleSetType.SINGLE_TABLE_MV_REWRITE) &&
                    optimizerConfig.isRuleSetTypeDisable(RuleSetType.MULTI_TABLE_MV_REWRITE)) {
                return false;
            }
            return true;
        }

        private boolean isEnableRBOSingleViewRewrite() {
            return optimizerContext.getQueryMaterializationContext() != null
                    && optimizerContext.getQueryMaterializationContext().getLogicalTreeWithView() != null
                    && !optimizerConfig.isRuleSetTypeDisable(RuleSetType.SINGLE_TABLE_MV_REWRITE)
                    && optimizerContext.getCandidateMvs().stream().anyMatch(MaterializationContext::isSingleTable);
        }

        private boolean isEnableRBOSingleTableRewrite(OptExpression queryPlan) {
            // if disable single mv rewrite, return false.
            if (optimizerConfig.isRuleSetTypeDisable(RuleSetType.SINGLE_TABLE_MV_REWRITE)) {
                return false;
            }
            // If query only has one table use single table rewrite, view delta only rewrites multi-tables query.
            if (!sessionVariable.isEnableMaterializedViewSingleTableViewDeltaRewrite() &&
                    MvUtils.getAllTables(queryPlan).size() <= 1) {
                return true;
            }
            // If view delta is enabled and there are multi-table mvs, return false.
            // if mv has multi table sources, we will process it in memo to support view delta join rewrite
            if (sessionVariable.isEnableMaterializedViewViewDeltaRewrite() &&
                    optimizerContext.getCandidateMvs().stream().anyMatch(MaterializationContext::hasMultiTables)) {
                return false;
            }
            return true;
        }

        private boolean isEnableCBOSingleTableRewrite(OptExpression queryPlan) {
            if (sessionVariable.isEnableMaterializedViewViewDeltaRewrite() &&
                    optimizerContext.getCandidateMvs().stream().anyMatch(MaterializationContext::hasMultiTables)) {
                return true;
            }
            return false;
        }

        private boolean isEnableCBOMultiTableRewrite(OptExpression queryPlan) {
            if (!sessionVariable.isEnableMaterializedViewSingleTableViewDeltaRewrite() &&
                    MvUtils.getAllTables(queryPlan).size() <= 1) {
                return false;
            }
            return true;
        }
    }

    public static MvRewriteStrategy getMvRewriteStrategy(OptimizerContext optimizerContext,
                                                         ConnectContext connectContext,
                                                         OptExpression queryPlan) {
        MvRewriteStrategy strategy = new MvRewriteStrategy();

        MvStrategyArbitrator arbitrator = new MvStrategyArbitrator(optimizerContext, connectContext);
        strategy.enableMaterializedViewRewrite = arbitrator.isEnableMaterializedViewRewrite();
        // only rewrite when enableMaterializedViewRewrite is enabled
        if (strategy.enableMaterializedViewRewrite) {
            SessionVariable sessionVariable = connectContext.getSessionVariable();
            strategy.enableForceRBORewrite = sessionVariable.isEnableForceRuleBasedMvRewrite();

            // rbo strategies
            strategy.enableRBOSingleViewRewrite = arbitrator.isEnableRBOSingleViewRewrite();
            strategy.enableRBOSingleTableRewrite = arbitrator.isEnableRBOSingleTableRewrite(queryPlan);

            // cbo strategies
            strategy.enableCBOSingleTableRewrite = arbitrator.isEnableCBOSingleTableRewrite(queryPlan);
            strategy.enableCBOMultiTableRewrite = arbitrator.isEnableCBOMultiTableRewrite(queryPlan);

            // make sure eager rewrite is used if necessary.
            int mvRewriteStrategy = sessionVariable.getMaterializedViewRewriteStrategy();
            if (mvRewriteStrategy <= 0) {
                strategy.enableDefaultRewrite = true;
            } else if (mvRewriteStrategy == 1) {
                strategy.enableEagerRewrite = true;
            } else {
                strategy.enableDefaultRewrite = true;
                strategy.enableEagerRewrite = true;
            }
            if (strategy.enableEagerRewrite) {
                strategy.enableEagerRewrite = strategy.enableRBOSingleViewRewrite || strategy.enableRBOSingleTableRewrite
                        || strategy.enableForceRBORewrite;
            }
        }

        return strategy;
    }
}
