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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.starrocks.utframe.UtFrameUtils.CREATE_STATISTICS_TABLE_STMT;

public class MaterializedViewTPCHTest extends MaterializedViewTestBase {
    private static final String MATERIALIZED_DB_NAME = "test_tpch_mv";

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext= UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setEnableQueryCache(true);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000000);
        connectContext.getSessionVariable().setEnableOptimizerTraceLog(true);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert
                .withDatabase(MATERIALIZED_DB_NAME)
                .useDatabase(MATERIALIZED_DB_NAME);

        if (!starRocksAssert.databaseExist("_statistics_")) {
            starRocksAssert.withDatabaseWithoutAnalyze(StatsConstants.STATISTICS_DB_NAME)
                    .useDatabase(StatsConstants.STATISTICS_DB_NAME);
            starRocksAssert.withTable(CREATE_STATISTICS_TABLE_STMT);
        }

        executeSqlFile("sql/materialized-view/tpch/ddl_tpch.sql");
        executeSqlFile("sql/materialized-view/tpch/ddl_tpch_mv.sql");
    }

    @Test
    public void testQuery1() {
        runFileUnitTest("materialized-view/tpch/q1");
    }

    @Test
    public void testQuery1_1() {
        runFileUnitTest("materialized-view/tpch/q1-1");
    }

    @Test
    public void testQuery2() {
        runFileUnitTest("materialized-view/tpch/q2");
    }

    @Test
    public void testQuery3() {
        runFileUnitTest("materialized-view/tpch/q3");
    }

    @Test
    public void testQuery4() {
        runFileUnitTest("materialized-view/tpch/q4");
    }

    @Test
    public void testQuery5() {
        runFileUnitTest("materialized-view/tpch/q5");
    }

    @Test
    public void testQuery6() {
        runFileUnitTest("materialized-view/tpch/q6");
    }

    @Test
    public void testQuery7() {
        runFileUnitTest("materialized-view/tpch/q7");
    }

    @Test
    public void testQuery8() {
        runFileUnitTest("materialized-view/tpch/q8");
    }

    @Test
    // TODO: unstable predicates' order
    public void testQuery9() {
        runFileUnitTest("materialized-view/tpch/q9");
    }

    @Test
    public void testQuery10() {
        runFileUnitTest("materialized-view/tpch/q10");
    }

    @Test
    public void testQuery11() {
        runFileUnitTest("materialized-view/tpch/q11");
    }

    @Test
    public void testQuery12() {
        runFileUnitTest("materialized-view/tpch/q12");
    }

    @Test
    public void testQuery13() {
        runFileUnitTest("materialized-view/tpch/q13");
    }

    @Test
    public void testQuery14() {
        runFileUnitTest("materialized-view/tpch/q14");
    }

    @Test
    public void testQuery15() {
        runFileUnitTest("materialized-view/tpch/q15");
    }

    @Test
    public void testQuery15_1() {
        runFileUnitTest("materialized-view/tpch/q15-1");
    }

    @Test
    public void testQuery16() {
        runFileUnitTest("materialized-view/tpch/q16");
    }

    @Test
    public void testQuery16_1() {
        runFileUnitTest("materialized-view/tpch/q16-1");
    }

    @Test
    public void testQuery17() {
        runFileUnitTest("materialized-view/tpch/q17");
    }

    @Test
    public void testQuery18() {
        runFileUnitTest("materialized-view/tpch/q18");
    }

    @Test
    public void testQuery19() {
        runFileUnitTest("materialized-view/tpch/q19");
    }

    @Test
    public void testQuery20() {
        runFileUnitTest("materialized-view/tpch/q20");
    }

    @Test
    public void testQuery20_1() {
        runFileUnitTest("materialized-view/tpch/q20-1");
    }

    @Test
    public void testQuery21() {
        runFileUnitTest("materialized-view/tpch/q21");
    }

    @Test
    public void testQuery22() {
        runFileUnitTest("materialized-view/tpch/q22");
    }

    // TODO: multi mvs will not hit MV's Plan.
    @Ignore
    @Test
    public void testQuery22_1() {
        runFileUnitTest("materialized-view/tpch/q22-1");
    }

    @Test
    public void analyzeTPCHMVs() throws Exception {
        Map<String, Set<String>> mvTableColumnsMap = Maps.newHashMap();
        Map<String, Set<String>> mvTableQueryIds = Maps.newHashMap();
        Pattern mvPattern = Pattern.compile("mv\\[(.*)] columns\\[(.*)] ");
        for (int i = 1; i < 23; i++) {
            String content = getFileContent("sql/materialized-view/tpch/q" + i + ".sql");
            String[] lines = content.split("\n");
            for (String line: lines) {
                if (line.contains("mv[")) {
                    Matcher matcher = mvPattern.matcher(line);
                    if (!matcher.find()) {
                        throw new Exception("bad explain format!!!");
                    }
                    // TODO: how to match multi mvs.
                    String mvTableName = matcher.group(1);
                    List<String> mvTableColumns = splitMVTableColumns(matcher.group(2));

                    mvTableColumnsMap.putIfAbsent(mvTableName, Sets.newTreeSet());
                    mvTableColumnsMap.get(mvTableName).addAll(mvTableColumns);
                    mvTableQueryIds.putIfAbsent(mvTableName, Sets.newTreeSet());
                    mvTableQueryIds.get(mvTableName).add(Integer.toString(i));
                }
            }
        }

        System.out.println("Analyze TPCH MVs Result");
        for (Map.Entry<String, Set<String>> entry : mvTableColumnsMap.entrySet()) {
            String mvTableName = entry.getKey();
            Set<String> mvTableColumns = entry.getValue();
            System.out.println("TableName:" + mvTableName);
            System.out.println("Columns:" + String.join(",", mvTableColumns));
            System.out.println("Queries:" + String.join(",", mvTableQueryIds.get(mvTableName)));
            System.out.println();
        }
    }

    private List<String> splitMVTableColumns(String line) {
        String[] s1 = line.split(",");
        List<String> ret = new ArrayList<>();
        for (String s: s1) {
           String[] s2 = s.split(":");
           ret.add(s2[1].trim());
        }
        return ret;
    }


}
