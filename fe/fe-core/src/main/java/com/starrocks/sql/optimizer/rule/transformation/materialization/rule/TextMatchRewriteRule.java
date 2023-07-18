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


package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.ParsingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class TextMatchRewriteRule {
    private static final Logger LOG = LogManager.getLogger(TextMatchRewriteRule.class);

    public OptExpression transform(OptExpression tree,
                                   ConnectContext connectContext,
                                   OptimizerContext context,
                                   StatementBase stmt,
                                   ColumnRefFactory columnRefFactory) {
        if (!connectContext.getSessionVariable().isEnableMaterializedViewTextMatchRewrite()) {
            return tree;
        }
        if (stmt == null || Strings.isNullOrEmpty(stmt.getOrigStmt().originStmt)
                || context.getAllCandidateMvs().isEmpty()) {
            return tree;
        }
        QueryStatement queryStmt = (QueryStatement) stmt;
        String query = normalizedQuery(stmt.getOrigStmt().originStmt, connectContext);
        for (MaterializationContext mvContext : context.getAllCandidateMvs()) {
            String mvDefinedQuery = normalizedQuery(mvContext.getMv().getViewDefineSql(), connectContext);
            if (!query.contains(mvDefinedQuery)) {
                continue;
            }

            QueryRelation relation = (QueryRelation) (queryStmt.getQueryRelation());
            List<String> colNames = relation.getColumnOutputNames();
            // NOTE: Only support exact match for text match rewrite,
            // so it's safe to replace original query directly.
            String replaced = String.format(" SELECT `%s` FROM %s ", Joiner.on("`,`").join(colNames),
                    mvContext.getMv().getName());
            query = query.replace(mvDefinedQuery, replaced);
            break;
        }
        LOG.info("After text rewrite, new query: {}", query);

        StatementBase newStmt;
        try {
            List<StatementBase> statementBases =
                    com.starrocks.sql.parser.SqlParser.parse(query, connectContext.getSessionVariable());
            Preconditions.checkState(statementBases.size() == 1);
            newStmt = statementBases.get(0);
        } catch (ParsingException parsingException) {
            LOG.warn("After Text rewrite, parse query {} failed", query);
            return tree;
        }
        Preconditions.checkState(newStmt instanceof QueryStatement);
        Analyzer.analyze(newStmt, connectContext);
        QueryRelation relation = ((QueryStatement) newStmt).getQueryRelation();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, connectContext)
                        .transformWithSelectLimit(relation);
        return logicalPlan.getRoot();
    }

    private String normalizedQuery(String query, ConnectContext connectContext) {
        StatementBase stmt;
        try {
            List<StatementBase> statementBases =
                    com.starrocks.sql.parser.SqlParser.parse(query, connectContext.getSessionVariable());
            Preconditions.checkState(statementBases.size() == 1);
            stmt = statementBases.get(0);
            Analyzer.analyze(stmt, connectContext);
        } catch (ParsingException parsingException) {
            LOG.warn("After Text rewrite, parse query {} failed", query);
            return query;
        }

        return new AstToSQLBuilder.AST2SQLBuilderVisitor(true, true).visit(stmt);
    }
}