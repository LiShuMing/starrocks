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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.MVVersionManager;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.jupiter.api.BeforeAll;

public abstract class MVIVMTestBase extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    // refresh and get the execute plan for the materialized view
    protected ExecPlan getIVMRefreshedExecPlan(MaterializedView mv) throws Exception {
        TaskRun taskRun = buildMVTaskRun(mv, "test");
        ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
        // update version map
        MVVersionManager.afterTxnCommitted(mv);
        return execPlan;
    }

    public abstract void advanceTableVersionTo(long toVersion);
}
