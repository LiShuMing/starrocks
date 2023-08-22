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

package com.starrocks.qe;

import com.google.common.base.Strings;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.thrift.TMaterializedViewStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShowMaterializedViewStatus {
    private long id;
    private String dbName;
    private String name;
    private String refreshType;
    private boolean isActive;

    private String text;
    private long rows;
    private String partitionType;
    private long lastCheckTime;
    private long createTime;
    private long taskId;
    private String taskName;
    private String inactiveReason;

    private TaskRunStatus lastTaskRunStatus;

    public ShowMaterializedViewStatus(long id, String dbName, String name) {
        this.id = id;
        this.dbName = dbName;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRefreshType() {
        return refreshType;
    }

    public void setRefreshType(String refreshType) {
        this.refreshType = refreshType;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public String getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public long getLastCheckTime() {
        return lastCheckTime;
    }

    public void setLastCheckTime(long lastCheckTime) {
        this.lastCheckTime = lastCheckTime;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getInactiveReason() {
        return inactiveReason;
    }

    public void setInactiveReason(String inactiveReason) {
        this.inactiveReason = inactiveReason;
    }

    public TaskRunStatus getLastTaskRunStatus() {
        return lastTaskRunStatus;
    }

    public void setLastTaskRunStatus(TaskRunStatus lastTaskRunStatus) {
        this.lastTaskRunStatus = lastTaskRunStatus;
    }

    /**
     * Return the thrift of show materialized views command from be's request.
     */
    public TMaterializedViewStatus toThrift() {
        TMaterializedViewStatus status = new TMaterializedViewStatus();
        status.setId(String.valueOf(this.id));
        status.setDatabase_name(this.dbName);
        status.setName(this.name);
        status.setRefresh_type(this.refreshType);
        status.setIs_active(this.isActive ? "true" : "false");
        status.setInactive_reason(this.inactiveReason);
        status.setPartition_type(this.partitionType);

        status.setTask_id(String.valueOf(this.taskId));
        status.setTask_name(this.taskName);
        if (lastTaskRunStatus != null) {
            status.setLast_refresh_start_time(TimeUtils.longToTimeString(lastTaskRunStatus.getCreateTime()));
            status.setLast_refresh_finished_time(TimeUtils.longToTimeString(lastTaskRunStatus.getFinishTime()));
            if (lastTaskRunStatus.getFinishTime() > lastTaskRunStatus.getCreateTime()) {
                status.setLast_refresh_duration(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(
                        (lastTaskRunStatus.getFinishTime() - lastTaskRunStatus.getCreateTime()) / 1000D));
            }
            status.setLast_refresh_error_code(String.valueOf(lastTaskRunStatus.getErrorCode()));
            status.setLast_refresh_error_message(Strings.nullToEmpty(lastTaskRunStatus.getErrorMessage()));

            status.setLast_refresh_state(String.valueOf(lastTaskRunStatus.getState()));
            MVTaskRunExtraMessage extraMessage = lastTaskRunStatus.getMvTaskRunExtraMessage();
            status.setLast_refresh_force_refresh(extraMessage.isForceRefresh() ? "true" : "false");
            status.setLast_refresh_start_partition(Strings.nullToEmpty(extraMessage.getPartitionStart()));
            status.setLast_refresh_end_partition(Strings.nullToEmpty(extraMessage.getPartitionEnd()));
            status.setLast_refresh_base_refresh_partitions(
                    Strings.nullToEmpty(extraMessage.getBasePartitionsToRefreshMapString()));
            status.setLast_refresh_mv_refresh_partitions(Strings.nullToEmpty(extraMessage.getMvPartitionsToRefreshString()));
        }

        status.setRows(String.valueOf(this.rows));
        status.setText(this.text);
        return status;
    }

    /**
     * Return show materialized views result set. Note: result set's order should keep same with
     * schema table MaterializedViewsSystemTable's define in the `MaterializedViewsSystemTable` class.
     */
    public List<String> toResultSet() {
        ArrayList<String> resultRow = new ArrayList<>();
        // mv id
        resultRow.add(String.valueOf(this.id));
        // db name
        resultRow.add(dbName);
        // mv name
        resultRow.add(this.name);
        // refresh_type
        resultRow.add(this.refreshType);
        // is_active
        resultRow.add(String.valueOf(this.isActive));
        // inactive reason
        resultRow.add(this.inactiveReason);
        // partition info
        resultRow.add(this.partitionType);
        // task run status
        if (lastTaskRunStatus != null) {
            // task_id
            resultRow.add(String.valueOf(lastTaskRunStatus.getTaskId()));
            // task_name
            resultRow.add(Strings.nullToEmpty(lastTaskRunStatus.getTaskName()));
            // last_refresh_start_time
            resultRow.add(String.valueOf(TimeUtils.longToTimeString(lastTaskRunStatus.getCreateTime())));
            // last_refresh_finished_time
            resultRow.add(String.valueOf(TimeUtils.longToTimeString(lastTaskRunStatus.getFinishTime())));
            // last_refresh_duration(s)
            if (lastTaskRunStatus.getFinishTime() > lastTaskRunStatus.getCreateTime()) {
                resultRow.add(DebugUtil.DECIMAL_FORMAT_SCALE_3
                        .format((lastTaskRunStatus.getFinishTime() - lastTaskRunStatus.getCreateTime()) / 1000D));
            } else {
                resultRow.add("0.000");
            }
            // last_refresh_state
            resultRow.add(String.valueOf(lastTaskRunStatus.getState()));

            MVTaskRunExtraMessage extraMessage = lastTaskRunStatus.getMvTaskRunExtraMessage();
            // force refresh
            resultRow.add(extraMessage.isForceRefresh() ? "true" : "false");
            // last_refresh partition start
            resultRow.add(Strings.nullToEmpty(extraMessage.getPartitionStart()));
            // last_refresh partition end
            resultRow.add(Strings.nullToEmpty(extraMessage.getPartitionEnd()));
            // last_refresh base table refresh map
            resultRow.add(Strings.nullToEmpty(extraMessage.getBasePartitionsToRefreshMapString()));
            // last_refresh mv partitions
            resultRow.add(Strings.nullToEmpty(extraMessage.getMvPartitionsToRefreshString()));
            // last_refresh_code
            resultRow.add(String.valueOf(lastTaskRunStatus.getErrorCode()));
            // last_refresh_reason
            resultRow.add(Strings.nullToEmpty(lastTaskRunStatus.getErrorMessage()));
        } else {
            resultRow.addAll(Collections.nCopies(13, ""));
        }
        resultRow.add(String.valueOf(this.rows));
        resultRow.add(this.text);
        return resultRow;
    }
}
