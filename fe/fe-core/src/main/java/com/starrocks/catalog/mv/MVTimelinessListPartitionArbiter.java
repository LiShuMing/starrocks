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

package com.starrocks.catalog.mv;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.UserException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.common.ListPartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.catalog.MvRefreshArbiter.getMvBaseTableUpdateInfo;

public class MVTimelinessListPartitionArbiter extends MVTimelinessArbiter {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessListPartitionArbiter.class);

    public MVTimelinessListPartitionArbiter(MaterializedView mv) {
        super(mv);
    }


    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInChecked(boolean isQueryRewrite) {

    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInLoose(boolean isQueryRewrite) {
        MvUpdateInfo mvUpdateInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL,
                TableProperty.QueryRewriteConsistencyMode.LOOSE);
        Map<Table, Column> partitionTableAndColumn = mv.getRelatedPartitionTableAndColumn();
        ListPartitionDiff listPartitionDiff = null;
        try {
            if (!collectBaseTablePartitionInfos(partitionTableAndColumn, isQueryRewrite, mvUpdateInfo)) {
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
            }
            Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfos = mvUpdateInfo.getBaseTableUpdateInfos();
            Map<Table, Map<String, List<List<String>>>> refBaseTablePartitionMap = baseTableUpdateInfos.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPartitionNameWithLists()));
            Map<String, List<List<String>>> listPartitionMap = mv.getListPartitionMap();
            Table partitionTable = mv.getDirectTableAndPartitionColumn().first;
            listPartitionDiff = SyncPartitionUtils.getListPartitionDiff(refBaseTablePartitionMap.get(partitionTable),
                    listPartitionMap);
        } catch (Exception e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return null;
        }
        if (listPartitionDiff == null) {
            LOG.warn("Materialized view compute partition difference with base table failed, the diff of range partition" +
                    " is null.");
            return null;
        }
        Map<String, List<List<String>>> adds = listPartitionDiff.getAdds();
        for (Map.Entry<String, List<List<String>>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            mvUpdateInfo.getMvToRefreshPartitionNames().add(mvPartitionName);
        }
        return mvUpdateInfo;
    }

    private boolean collectBaseTablePartitionInfos(Map<Table, Column> partitionInfos,
                                                   boolean isQueryRewrite,
                                                   MvUpdateInfo mvUpdateInfo) {
        for (Map.Entry<Table, Column> entry : partitionInfos.entrySet()) {
            Table table = entry.getKey();

            // TODO: merge getPartitionKeyRange into mvBaseTableUpdateInfo
            // step1.2: check ref base table's updated partition names by checking its ref tables recursively.
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo  = getMvBaseTableUpdateInfo(mv, table, true, isQueryRewrite);
            if (mvBaseTableUpdateInfo == null) {
                return false;
            }

            // TODO: no need to list all partitions, just need to list the changed partitions?
            try {
                Map<String, List<List<String>>> partitionKeyLists = PartitionUtil.getPartitionList(table, entry.getValue());
                for (Map.Entry<String, List<List<String>>> e : partitionKeyLists.entrySet()) {
                    mvBaseTableUpdateInfo.addListPartitionKeys(e.getKey(), e.getValue());
                }
            } catch (UserException e) {
                LOG.warn("Materialized view compute partition difference with base table failed.", e);
                return false;
            }
            mvUpdateInfo.getBaseTableUpdateInfos().put(table, mvBaseTableUpdateInfo);
        }
        return true;
    }
}
