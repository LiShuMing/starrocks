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

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class CreateStreamStmt extends DdlStmt {
    private boolean ifNotExists;
    private TableName streamTableName;
    private TableName targetTableName;
    private Map<String, String> properties;
    private QueryPeriod queryPeriod;
    private String comment;

    public CreateStreamStmt(boolean ifNotExists,
                            TableName streamTableName,
                            TableName targetTableName,
                            QueryPeriod queryPeriod,
                            String comment,
                            Map<String, String> properties,
                            NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.streamTableName = streamTableName;
        this.targetTableName = targetTableName;
        this.queryPeriod = queryPeriod;
        this.comment = comment;
        this.properties = properties;
    }

    public String getComment() {
        return comment;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public QueryPeriod getQueryPeriod() {
        return queryPeriod;
    }

    public TableName getStreamTableName() {
        return streamTableName;
    }

    public TableName getTargetTableName() {
        return targetTableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateStreamTableStatement(this, context);
    }
}
