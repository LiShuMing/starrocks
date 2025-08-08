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
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodName.class)
public class IVMBasedMvRefreshProcessorIcebergTest extends MVIVMIcebergTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
    }

    @Test
    public void testIVMWithScan() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`test_mv1` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"refresh_mode\" = \"incremental\"" +
                        ")\n" +
                        "AS SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;");
        MaterializedView mv = getMv("test_mv1");
        // 1th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.COSTS);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "     TABLE: unpartitioned_db.t0\n" +
                    "     TABLE VERSION: Delta[MIN,1]");
        }
        // 2th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan == null);
        }
        advanceTableVersionTo(2);
        // 3th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "     TABLE: unpartitioned_db.t0\n" +
                    "     TABLE VERSION: Delta[1,2]");
        }
    }

    @Test
    public void testIVMWithScanProjectFilter() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`test_mv1` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"refresh_mode\" = \"incremental\"" +
                        ")\n" +
                        "AS SELECT id * 2 + 1, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` where id > 10;");
        MaterializedView mv = getMv("test_mv1");
        // 1th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.COSTS);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "     TABLE: unpartitioned_db.t0\n" +
                    "     PREDICATES: 1: id > 10\n" +
                    "     MIN/MAX PREDICATES: 1: id > 10\n" +
                    "     TABLE VERSION: Delta[MIN,1]");
        }
        // 2th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan == null);
        }
        advanceTableVersionTo(2);
        // 3th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "  0:IcebergScanNode\n" +
                    "     TABLE: unpartitioned_db.t0\n" +
                    "     PREDICATES: 1: id > 10\n" +
                    "     MIN/MAX PREDICATES: 1: id > 10\n" +
                    "     TABLE VERSION: Delta[1,2]");
        }
    }

    @Test
    public void testIVMWithJoin() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`test_mv1` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"refresh_mode\" = \"incremental\"" +
                        ")\n" +
                        "AS SELECT a.id * 2 + 1, b.data FROM `iceberg0`.`unpartitioned_db`.`t0` a inner join " +
                        "`iceberg0`.`partitioned_db`.`t1` b on a.id=b.id where a.id > 10;");
        MaterializedView mv = getMv("test_mv1");
        // 1th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.COSTS);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "     TABLE: unpartitioned_db.t0\n" +
                    "     PREDICATES: 14: id > 10\n" +
                    "     MIN/MAX PREDICATES: 14: id > 10\n" +
                    "     TABLE VERSION: Delta[MIN,1]");
            PlanTestBase.assertContains(plan, "     TABLE: partitioned_db.t1\n" +
                    "     PREDICATES: 17: id > 10\n" +
                    "     MIN/MAX PREDICATES: 17: id > 10\n" +
                    "     TABLE VERSION: Snapshot@(1)");
        }
        // 2th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan == null);
        }
        advanceTableVersionTo(2);
        // 3th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.COSTS);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "     TABLE: unpartitioned_db.t0\n" +
                    "     PREDICATES: 8: id > 10\n" +
                    "     MIN/MAX PREDICATES: 8: id > 10\n" +
                    "     TABLE VERSION: Snapshot@(1)");
            PlanTestBase.assertContains(plan, "  1:IcebergScanNode\n" +
                    "     TABLE: partitioned_db.t1\n" +
                    "     PREDICATES: 11: id > 10\n" +
                    "     MIN/MAX PREDICATES: 11: id > 10\n" +
                    "     TABLE VERSION: Delta[1,2]");

            PlanTestBase.assertContains(plan, "     TABLE: unpartitioned_db.t0\n" +
                    "     PREDICATES: 14: id > 10\n" +
                    "     MIN/MAX PREDICATES: 14: id > 10\n" +
                    "     TABLE VERSION: Delta[1,2]");
            PlanTestBase.assertContains(plan, "     TABLE: partitioned_db.t1\n" +
                    "     PREDICATES: 17: id > 10\n" +
                    "     MIN/MAX PREDICATES: 17: id > 10\n" +
                    "     TABLE VERSION: Snapshot@(2)");
        }
    }

    @Test
    public void testPartitionedIVMWithScan() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`test_mv1`\n" +
                        "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"refresh_mode\" = \"incremental\"\n" +
                        ")\n" +
                        "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");
        MaterializedView mv = getMv("test_mv1");
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.COSTS);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "  0:IcebergScanNode\n" +
                    "     TABLE: partitioned_db.t1\n" +
                    "     TABLE VERSION: Delta[MIN,1]");
        }
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan == null);
        }
        advanceTableVersionTo(2);
        // 3th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.COSTS);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "  0:IcebergScanNode\n" +
                    "     TABLE: partitioned_db.t1\n" +
                    "     TABLE VERSION: Delta[1,2]");
        }
    }

    @Test
    public void testPartitionedIVMWithAggregate1() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`test_mv1`\n" +
                        "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"refresh_mode\" = \"incremental\"\n" +
                        ")\n" +
                        "AS SELECT date, sum(id), approx_count_distinct(data)  " +
                        " FROM `iceberg0`.`partitioned_db`.`t1` as a group by date;");
        MaterializedView mv = getMv("test_mv1");
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "  0:IcebergScanNode\n" +
                    "     TABLE: partitioned_db.t1\n" +
                    "     TABLE VERSION: Delta[MIN,1]");
            PlanTestBase.assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 19: row_fingerprint = 11: __ROW_ID__");
        }
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan == null);
        }
        advanceTableVersionTo(2);
        // 3th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "  0:IcebergScanNode\n" +
                    "     TABLE: partitioned_db.t1\n" +
                    "     TABLE VERSION: Delta[1,2]");
            PlanTestBase.assertContains(plan, "  6:OlapScanNode\n" +
                    "     TABLE: test_mv1");
            PlanTestBase.assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 19: row_fingerprint = 11: __ROW_ID__");
        }
    }

    @Test
    public void testUnionAll() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`test_mv1`\n" +
                        "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"refresh_mode\" = \"incremental\"\n" +
                        ")\n" +
                        "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a " +
                        " UNION ALL" +
                        "  SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as b;");
        MaterializedView mv = getMv("test_mv1");
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "  3:IcebergScanNode\n" +
                    "     TABLE: unpartitioned_db.t0\n" +
                    "     TABLE VERSION: Delta[MIN,1]");
            PlanTestBase.assertContains(plan, "  0:IcebergScanNode\n" +
                    "     TABLE: partitioned_db.t1\n" +
                    "     TABLE VERSION: Delta[MIN,1]");
        }
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan == null);
        }
        advanceTableVersionTo(2);
        // 3th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "  3:IcebergScanNode\n" +
                    "     TABLE: unpartitioned_db.t0\n" +
                    "     TABLE VERSION: Delta[1,2]");
            PlanTestBase.assertContains(plan, "  0:IcebergScanNode\n" +
                    "     TABLE: partitioned_db.t1\n" +
                    "     TABLE VERSION: Delta[1,2]");
        }
    }

    @Test
    public void testJoinAndAggregate() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`test_mv1` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"refresh_mode\" = \"incremental\"" +
                        ")\n" +
                        "AS SELECT b.data, sum(a.id * 2 + 1) " +
                        "   FROM `iceberg0`.`unpartitioned_db`.`t0` a " +
                        "   inner join `iceberg0`.`partitioned_db`.`t1` b on a.id=b.id " +
                        "   where a.id > 10" +
                        "   GROUP BY b.data;");
        MaterializedView mv = getMv("test_mv1");
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "  18:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 28: row_fingerprint = 23: __ROW_ID__");
            PlanTestBase.assertContains(plan, "  16:OlapScanNode\n" +
                    "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/1");
        }
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan == null);
        }
        advanceTableVersionTo(2);
        // 3th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "     TABLE: unpartitioned_db.t0\n" +
                    "     PREDICATES: 17: id > 10\n" +
                    "     MIN/MAX PREDICATES: 17: id > 10\n" +
                    "     TABLE VERSION: Delta[1,2]");
            PlanTestBase.assertContains(plan, "     TABLE: unpartitioned_db.t0\n" +
                    "     PREDICATES: 11: id > 10\n" +
                    "     MIN/MAX PREDICATES: 11: id > 10\n" +
                    "     TABLE VERSION: Snapshot@(1)");
            PlanTestBase.assertContains(plan, "  18:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 28: row_fingerprint = 23: __ROW_ID__");
        }
    }
}