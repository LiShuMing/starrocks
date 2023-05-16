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

package com.starrocks.planner;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;

public class MaterializedViewManualTest extends MaterializedViewTestBase {

    @BeforeClass
    public static void setUp() throws Exception {
        MaterializedViewTestBase.setUp();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        String tableSQL = "CREATE TABLE `test_partition_expr_tbl1` (\n" +
                "  `order_id` bigint(20) NOT NULL DEFAULT \"-1\" COMMENT \"\",\n" +
                "  `dt` datetime NOT NULL DEFAULT \"1996-01-01 00:00:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`order_id`, `dt`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "PARTITION p2023041015 VALUES [(\"2023-04-10 15:00:00\"), (\"2023-04-10 16:00:00\")),\n" +
                "PARTITION p2023041016 VALUES [(\"2023-04-10 16:00:00\"), (\"2023-04-10 17:00:00\")),\n" +
                "PARTITION p2023041017 VALUES [(\"2023-04-10 17:00:00\"), (\"2023-04-10 18:00:00\")),\n" +
                "PARTITION p2023041018 VALUES [(\"2023-04-10 18:00:00\"), (\"2023-04-10 19:00:00\")),\n" +
                "PARTITION p2023041019 VALUES [(\"2023-04-10 19:00:00\"), (\"2023-04-10 20:00:00\")),\n" +
                "PARTITION p2023041020 VALUES [(\"2023-04-10 20:00:00\"), (\"2023-04-10 21:00:00\")),\n" +
                "PARTITION p2023041021 VALUES [(\"2023-04-10 21:00:00\"), (\"2023-04-10 22:00:00\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`order_id`) BUCKETS 9\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\");";
        starRocksAssert.withTable(tableSQL);
    }

    @Test
    public void testDistinct1() throws Exception {
        String mv = "CREATE MATERIALIZED VIEW `test_distinct_mv1`\n" +
                "DISTRIBUTED BY HASH(`deptno`, `locationid`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ")\n" +
                "AS \n" +
                "SELECT \n" +
                "  `locationid`,\n" +
                "  `deptno`,\n" +
                "  count(DISTINCT `empid`) AS `order_num`\n" +
                "FROM `emps`\n" +
                "GROUP BY `locationid`, `deptno`;";
        starRocksAssert.withMaterializedView(mv);
        sql("select deptno, count(distinct empid) from emps group by deptno")
                .nonMatch("test_distinct_mv1");
        starRocksAssert.dropMaterializedView("test_distinct_mv1");
    }

    @Test
    public void testDistinct2() throws Exception {
        String tableSQL = "CREATE TABLE `test_distinct2` (\n" +
                "  `order_id` bigint(20) NOT NULL DEFAULT \"-1\" COMMENT \"\",\n" +
                "  `dt` datetime NOT NULL DEFAULT \"1996-01-01 00:00:00\" COMMENT \"\",\n" +
                "  `k1`bigint,\n" +
                "  `v1` varchar(256) NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`order_id`, `dt`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "PARTITION p2023041017 VALUES [(\"2023-04-10 17:00:00\"), (\"2023-04-10 18:00:00\")),\n" +
                "PARTITION p2023041021 VALUES [(\"2023-04-10 21:00:00\"), (\"2023-04-10 22:00:00\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`order_id`) BUCKETS 9\n" +
                "PROPERTIES (\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"HOUR\",\n" +
                "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                "\"dynamic_partition.start\" = \"-240\",\n" +
                "\"dynamic_partition.end\" = \"2\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"9\"," +
                "\"replication_num\" = \"1\"" +
                ");";
        starRocksAssert.withTable(tableSQL);
        String mv = "CREATE MATERIALIZED VIEW `test_distinct2_mv`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('hour', `dt`))\n" +
                "DISTRIBUTED BY HASH(`dt`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ")\n" +
                "AS\n" +
                "SELECT \n" +
                "k1, " +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "`dt`\n" +
                "FROM `test_distinct2`\n" +
                "group by dt, k1;";
        starRocksAssert.withMaterializedView(mv);
        sql("select dt, count(distinct order_id) " +
                "from test_partition_expr_tbl1 group by dt")
                .nonMatch("test_partition_expr_mv1");
        starRocksAssert.dropMaterializedView("test_distinct2_mv");
    }

    @Test
    public void testLimit1() throws Exception {
        String sql = "CREATE TABLE `test_limit_1` (\n" +
                "  `id` bigint(20) NULL COMMENT \"\",\n" +
                "  `dt` date NULL COMMENT \"\",\n" +
                "  `hour` varchar(65533) NULL COMMENT \"\"," +
                "  `v1` float NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`, `dt`, `hour`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p202207 VALUES [(\"2022-07-01\"), (\"2022-08-01\")),\n" +
                "PARTITION p202210 VALUES [(\"2022-10-01\"), (\"2022-11-01\")))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 144\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ");";
        starRocksAssert.withTable(sql);
        String mv = "SELECT dt, count(DISTINCT id) from " +
                "test_limit_1 where dt >= '2022-07-01' and dt <= '2022-10-01' group by dt";
        testRewriteOK(mv, "SELECT dt, count(DISTINCT id) from test_limit_1 where " +
                "dt >= '2022-07-01' and dt <= '2022-10-01' group by dt limit 10");
    }

    @Test
    public void testPartitionColumnExpr1() throws Exception {
        String mv = "CREATE MATERIALIZED VIEW `test_partition_expr_mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('hour', ds))\n" +
                "DISTRIBUTED BY HASH(`order_num`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ")\n" +
                "AS\n" +
                "SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS ds\n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "group by ds;";
        starRocksAssert.withMaterializedView(mv);

        sql("SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS ds \n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "WHERE time_slice(`dt`, INTERVAL 5 minute) BETWEEN '2023-04-11' AND '2023-04-12'\n" +
                "group by ds")
                .match("test_partition_expr_mv1");

        sql("SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS ds \n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "WHERE time_slice(`dt`, INTERVAL 5 minute) BETWEEN '2023-04-11' AND '2023-04-12' or " +
                "time_slice(`dt`, INTERVAL 5 minute) BETWEEN '2023-04-12' AND '2023-04-13'\n" +
                "group by ds")
                .match("test_partition_expr_mv1");
        sql("SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS `ts`\n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "WHERE time_slice(dt, interval 5 minute) >= '2023-04-10 17:00:00' and time_slice(dt, interval 5 minute) < '2023-04-10 18:00:00'\n" +
                "group by ts")
                .match("test_partition_expr_mv1");

        sql("SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS `ts`\n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "WHERE `dt` >= '2023-04-10 17:00:00' and `dt` < '2023-04-10 18:00:00'\n" +
                "group by ts").nonMatch("test_partition_expr_mv1");
        starRocksAssert.dropMaterializedView("test_partition_expr_mv1");
    }

    private static void refreshMaterializedViewSync(String sql) throws Exception {
        RefreshMaterializedViewStatement refreshMaterializedViewStatement =
                (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            String dbName = refreshMaterializedViewStatement.getMvName().getDb();
            String mvName = refreshMaterializedViewStatement.getMvName().getTbl();
            boolean force = refreshMaterializedViewStatement.isForceRefresh();
            PartitionRangeDesc range =
                    refreshMaterializedViewStatement.getPartitionRangeDesc();
            MaterializedView materializedView = getMv(dbName, mvName);

            HashMap<String, String> taskRunProperties = new HashMap<>();
            taskRunProperties.put(TaskRun.PARTITION_START, range == null ? null : range.getPartitionStart());
            taskRunProperties.put(TaskRun.PARTITION_END, range == null ? null : range.getPartitionEnd());
            taskRunProperties.put(TaskRun.FORCE, Boolean.toString(force));

            ExecuteOption executeOption = new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(), false, taskRunProperties);
            executeOption.setManual();

            final String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
            if (!taskManager.containTask(mvTaskName)) {
                Task task = TaskBuilder.buildMvTask(materializedView, dbName);
                TaskBuilder.updateTaskInfo(task, materializedView);
                taskManager.createTask(task, false);
            }
            taskManager.executeTaskSync(mvTaskName, executeOption);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testPartitionColumnExpr2() throws Exception {
        String mv = "CREATE MATERIALIZED VIEW `test_partition_expr_mv2`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('hour', `ds`))\n" +
                "DISTRIBUTED BY HASH(`ds`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\")\n" +
                "AS\n" +
                "SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS `ds`\n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "group by ds;";
        starRocksAssert.withMaterializedView(mv);
        refreshMaterializedViewSync("REFRESH MATERIALIZED VIEW test_partition_expr_mv2 \n" +
                "PARTITION START (\"2023-04-10 15:00:00\") END (\"2023-04-10 17:00:00\");");
        refreshMaterializedViewSync("REFRESH MATERIALIZED VIEW test_partition_expr_mv2 \n" +
                "PARTITION START (\"2023-04-10 18:00:00\") END (\"2023-04-10 20:00:00\");");

        sql("SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS ds \n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "WHERE time_slice(`dt`, INTERVAL 5 minute) BETWEEN '2023-04-10 17:00:00' AND '2023-04-10 18:00:00'\n" +
                "group by ds")
                .match("test_partition_expr_mv2");

//        sql("SELECT \n" +
//                "count(DISTINCT `order_id`) AS `order_num`, \n" +
//                "time_slice(`dt`, INTERVAL 5 minute) AS ds \n" +
//                "FROM `test_partition_expr_tbl1`\n" +
//                "WHERE time_slice(`dt`, INTERVAL 5 minute) BETWEEN '2023-04-11' AND '2023-04-12' or " +
//                "time_slice(`dt`, INTERVAL 5 minute) BETWEEN '2023-04-12' AND '2023-04-13'\n" +
//                "group by ds")
//                .match("test_partition_expr_mv1");
//        sql("SELECT \n" +
//                "count(DISTINCT `order_id`) AS `order_num`, \n" +
//                "time_slice(`dt`, INTERVAL 5 minute) AS `ts`\n" +
//                "FROM `test_partition_expr_tbl1`\n" +
//                "WHERE time_slice(dt, interval 5 minute) >= '2023-04-10 17:00:00' and time_slice(dt, interval 5 minute) < '2023-04-10 18:00:00'\n" +
//                "group by ts")
//                .match("test_partition_expr_mv1");
//
//        sql("SELECT \n" +
//                "count(DISTINCT `order_id`) AS `order_num`, \n" +
//                "time_slice(`dt`, INTERVAL 5 minute) AS `ts`\n" +
//                "FROM `test_partition_expr_tbl1`\n" +
//                "WHERE `dt` >= '2023-04-10 17:00:00' and `dt` < '2023-04-10 18:00:00'\n" +
//                "group by ts").nonMatch("test_partition_expr_mv1");
        starRocksAssert.dropMaterializedView("test_partition_expr_mv2");
    }

    @Test
    public void testNullableTestCase1() throws Exception {
        String mv = "create materialized view join_null_mv_2\n" +
                "distributed by hash(empid)\n" +
                "as\n" +
                "select empid, depts.deptno, depts.name from emps_null join depts using (deptno);";
        starRocksAssert.withMaterializedView(mv);
        sql("select empid, emps_null.deptno \n" +
                "from emps_null join depts using (deptno) \n" +
                "where empid < 10")
                .match("join_null_mv_2");
    }

    @Test
    public void testNullableTestCase2() throws Exception {
        String mv = "create materialized view join_null_mv\n" +
                "distributed by hash(empid)\n" +
                "as\n" +
                "select empid, depts_null.deptno, depts_null.name from emps_null join depts_null using (deptno)\n";
        starRocksAssert.withMaterializedView(mv);
        sql("select empid, depts_null.deptno, depts_null.name from emps_null " +
                "join depts_null using (deptno) where depts_null.deptno < 10;")
                .match("join_null_mv");
    }
}
