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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.thrift.TMaterializedViewStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private List<TaskRunStatus> lastJobTaskRunStatus;


    public class RefreshJobStatus {
        private Constants.TaskRunState refreshState;
        private long mvRefreshStartTime;
        private long mvRefreshEndTime;
        private long totalProcessDuration;
        private boolean isForce;
        private List<String> refreshedPartitionStarts;
        private List<String> refreshedPartitionEnds;
        private List<String> refreshedBasePartitionsToRefreshMaps;
        private List<String> refreshedMvPartitionsToRefreshs;
        private String errorCode;
        private String errorMsg;
        private boolean isRefreshFinished;
        public RefreshJobStatus() {
        }

        public long getMvRefreshStartTime() {
            return mvRefreshStartTime;
        }

        public void setMvRefreshStartTime(long mvRefreshStartTime) {
            this.mvRefreshStartTime = mvRefreshStartTime;
        }

        public long getMvRefreshEndTime() {
            return mvRefreshEndTime;
        }

        public void setMvRefreshEndTime(long mvRefreshEndTime) {
            this.mvRefreshEndTime = mvRefreshEndTime;
        }

        public long getTotalProcessDuration() {
            return totalProcessDuration;
        }

        public void setTotalProcessDuration(long totalProcessDuration) {
            this.totalProcessDuration = totalProcessDuration;
        }

        public boolean isForce() {
            return isForce;
        }

        public void setForce(boolean force) {
            isForce = force;
        }

        public List<String> getRefreshedPartitionStarts() {
            return refreshedPartitionStarts;
        }

        public void setRefreshedPartitionStarts(List<String> refreshedPartitionStarts) {
            this.refreshedPartitionStarts = refreshedPartitionStarts;
        }

        public List<String> getRefreshedPartitionEnds() {
            return refreshedPartitionEnds;
        }

        public void setRefreshedPartitionEnds(List<String> refreshedPartitionEnds) {
            this.refreshedPartitionEnds = refreshedPartitionEnds;
        }

        public List<String> getRefreshedBasePartitionsToRefreshMaps() {
            return refreshedBasePartitionsToRefreshMaps;
        }

        public void setRefreshedBasePartitionsToRefreshMaps(List<String> refreshedBasePartitionsToRefreshMaps) {
            this.refreshedBasePartitionsToRefreshMaps = refreshedBasePartitionsToRefreshMaps;
        }

        public List<String> getRefreshedMvPartitionsToRefreshs() {
            return refreshedMvPartitionsToRefreshs;
        }

        public void setRefreshedMvPartitionsToRefreshs(List<String> refreshedMvPartitionsToRefreshs) {
            this.refreshedMvPartitionsToRefreshs = refreshedMvPartitionsToRefreshs;
        }

        public String getErrorCode() {
            return errorCode;
        }

        public void setErrorCode(String errorCode) {
            this.errorCode = errorCode;
        }

        public String getErrorMsg() {
            return errorMsg;
        }

        public void setErrorMsg(String errorMsg) {
            this.errorMsg = errorMsg;
        }

        public Constants.TaskRunState getRefreshState() {
            return refreshState;
        }

        public void setRefreshState(Constants.TaskRunState refreshState) {
            this.refreshState = refreshState;
        }

        public boolean isRefreshFinished() {
            return isRefreshFinished;
        }

        public void setRefreshFinished(boolean refreshFinished) {
            isRefreshFinished = refreshFinished;
        }
    }

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

    public void setLastJobTaskRunStatus(List<TaskRunStatus> lastJobTaskRunStatus) {
        if (lastJobTaskRunStatus != null) {
            // sort by process start time
            lastJobTaskRunStatus.sort(Comparator.comparing(TaskRunStatus::getProcessStartTime));
            this.lastJobTaskRunStatus = lastJobTaskRunStatus;
        }
    }

    public RefreshJobStatus getRefreshJobStatus() {
        if (lastJobTaskRunStatus == null || lastJobTaskRunStatus.isEmpty()) {
            return new RefreshJobStatus();
        }
        RefreshJobStatus status = new RefreshJobStatus();
        TaskRunStatus firstTaskRunStatus = lastJobTaskRunStatus.get(0);
        TaskRunStatus lastTaskRunStatus = lastJobTaskRunStatus.get(lastJobTaskRunStatus.size() - 1);

        long mvRefreshStartTime = firstTaskRunStatus.getProcessStartTime();
        status.setMvRefreshStartTime(mvRefreshStartTime);

        // LAST_REFRESH_STATE
        status.setRefreshState(lastTaskRunStatus.getLastRefreshState());

        MVTaskRunExtraMessage extraMessage = lastTaskRunStatus.getMvTaskRunExtraMessage();
        status.setForce(extraMessage.isForceRefresh());

        // getPartitionStart
        List<String> refreshedPartitionStarts = applyLastJobTaskRunStatusWith(x ->
                x.getMvTaskRunExtraMessage().getPartitionStart());
        status.setRefreshedPartitionStarts(refreshedPartitionStarts);

        // getPartitionEnd
        List<String> refreshedPartitionEnds = applyLastJobTaskRunStatusWith(x ->
                x.getMvTaskRunExtraMessage().getPartitionEnd());
        status.setRefreshedPartitionEnds(refreshedPartitionEnds);

        // getBasePartitionsToRefreshMapString
        List<String> refreshedBasePartitionsToRefreshMaps = applyLastJobTaskRunStatusWith(x ->
                x.getMvTaskRunExtraMessage().getBasePartitionsToRefreshMapString());
        status.setRefreshedBasePartitionsToRefreshMaps(refreshedBasePartitionsToRefreshMaps);

        // getMvPartitionsToRefreshString
        List<String> refreshedMvPartitionsToRefreshs = applyLastJobTaskRunStatusWith(x ->
                x.getMvTaskRunExtraMessage().getMvPartitionsToRefreshString());
        status.setRefreshedMvPartitionsToRefreshs(refreshedMvPartitionsToRefreshs);

        // only updated when refresh is finished
        if (lastTaskRunStatus.isRefreshFinished()) {
            status.setRefreshFinished(true);

            long mvRefreshFinishTime = lastTaskRunStatus.getProcessFinishTime();
            status.setMvRefreshEndTime(mvRefreshFinishTime);

            long totalProcessDuration = lastJobTaskRunStatus.stream()
                    .map(x -> x.calculateRefreshProcessDuration())
                    .collect(Collectors.summingLong(Long::longValue));
            status.setTotalProcessDuration(totalProcessDuration);
            status.setErrorCode(String.valueOf(lastTaskRunStatus.getErrorCode()));
            status.setErrorMsg(Strings.nullToEmpty(lastTaskRunStatus.getErrorMessage()));
        }
        return status;
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
        if (lastJobTaskRunStatus != null && !lastJobTaskRunStatus.isEmpty()) {
            RefreshJobStatus refreshJobStatus = getRefreshJobStatus();
            // start time
            status.setLast_refresh_start_time(TimeUtils.longToTimeString(refreshJobStatus.getMvRefreshStartTime()));
            // LAST_REFRESH_STATE
            status.setLast_refresh_state(String.valueOf(refreshJobStatus.getRefreshState()));
            // is force
            status.setLast_refresh_force_refresh(refreshJobStatus.isForce() ? "true" : "false");
            // partitionStart
            status.setLast_refresh_start_partition(Joiner.on(",").join(refreshJobStatus.getRefreshedPartitionStarts()));
            // partitionEnd
            status.setLast_refresh_end_partition(Joiner.on(",").join(refreshJobStatus.getRefreshedPartitionEnds()));
            // basePartitionsToRefreshMapString
            status.setLast_refresh_base_refresh_partitions(
                    Joiner.on(",").join(refreshJobStatus.getRefreshedBasePartitionsToRefreshMaps()));
            // mvPartitionsToRefreshString
            status.setLast_refresh_mv_refresh_partitions(
                    Joiner.on(",").join(refreshJobStatus.getRefreshedMvPartitionsToRefreshs()));
            // only updated when refresh is finished
            if (refreshJobStatus.isRefreshFinished()) {
                status.setLast_refresh_finished_time(TimeUtils.longToTimeString(refreshJobStatus.getMvRefreshEndTime()));
                status.setLast_refresh_duration(formatDuration(refreshJobStatus.getTotalProcessDuration()));
                status.setLast_refresh_error_code(refreshJobStatus.getErrorCode());
                status.setLast_refresh_error_message(refreshJobStatus.getErrorMsg());
            }
        }

        status.setRows(String.valueOf(this.rows));
        status.setText(this.text);
        return status;
    }
    private List<String> applyLastJobTaskRunStatusWith(Function<TaskRunStatus, String> func) {
        return lastJobTaskRunStatus.stream()
                .map(x -> func.apply(x))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Return show materialized views result set. Note: result set's order should keep same with
     * schema table MaterializedViewsSystemTable's define in the `MaterializedViewsSystemTable` class.
     */
    public List<String> toResultSet() {
        ArrayList<String> resultRow = new ArrayList<>();

        // Add fields to the result set
        addField(resultRow, id);
        addField(resultRow, dbName);
        addField(resultRow, name);
        addField(resultRow, refreshType);
        addField(resultRow, isActive);
        addField(resultRow, inactiveReason);
        addField(resultRow, partitionType);

        if (lastJobTaskRunStatus != null && !lastJobTaskRunStatus.isEmpty()) {
            TaskRunStatus firstTaskRunStatus = lastJobTaskRunStatus.get(0);
            TaskRunStatus lastTaskRunStatus = lastJobTaskRunStatus.get(lastJobTaskRunStatus.size() - 1);

            // Add fields related to task run status
            // task id
            addField(resultRow, lastTaskRunStatus.getTaskId());
            // task name
            addField(resultRow, Strings.nullToEmpty(lastTaskRunStatus.getTaskName()));
            // process start time
            addField(resultRow, TimeUtils.longToTimeString(firstTaskRunStatus.getProcessStartTime()));

            boolean isJobRefreshFinished = lastTaskRunStatus.isRefreshFinished();
            addField(resultRow, isJobRefreshFinished ? TimeUtils.longToTimeString(lastTaskRunStatus.getProcessFinishTime()) : "");
            long totalProcessDuration = lastJobTaskRunStatus.stream()
                    .map(x -> x.calculateRefreshProcessDuration())
                    .collect(Collectors.summingLong(Long::longValue));
            addField(resultRow, isJobRefreshFinished ? formatDuration(totalProcessDuration) : "0.000");

            addField(resultRow, lastTaskRunStatus.getLastRefreshState());

            MVTaskRunExtraMessage extraMessage = firstTaskRunStatus.getMvTaskRunExtraMessage();
            if (extraMessage != null) {
                // Add additional task run information fields
                addField(resultRow, extraMessage.isForceRefresh() ? "true" : "false");
            } else {
                // If there is no additional task run information, fill with empty fields
                addEmptyFields(resultRow, 1);
            }

            // partitionStart
            RefreshJobStatus refreshJobStatus = getRefreshJobStatus();
            addField(resultRow, (Joiner.on(",").join(refreshJobStatus.refreshedPartitionStarts)));

            // partitionEnd
            addField(resultRow, (Joiner.on(",").join(refreshJobStatus.refreshedPartitionEnds)));

            // basePartitionsToRefreshMapString
            addField(resultRow, (Joiner.on(",").join(refreshJobStatus.refreshedBasePartitionsToRefreshMaps)));

            // mvPartitionsToRefreshString
            addField(resultRow, (Joiner.on(",").join(refreshJobStatus.refreshedMvPartitionsToRefreshs)));

            addField(resultRow, lastTaskRunStatus.getErrorCode());
            addField(resultRow, Strings.nullToEmpty(lastTaskRunStatus.getErrorMessage()));
        } else {
            // If there is no task run status, fill with empty fields
            addEmptyFields(resultRow, 13);
        }

        addField(resultRow, rows);
        addField(resultRow, text);

        return resultRow;
    }

    // Add a field to the result set
    private void addField(List<String> resultRow, Object field) {
        if (field == null) {
            resultRow.add("");
        } else {
            resultRow.add(String.valueOf(field));
        }
    }

    // Fill with empty fields
    private void addEmptyFields(List<String> resultRow, int count) {
        resultRow.addAll(Collections.nCopies(count, ""));
    }


    private String formatDuration(long duration) {
        return DebugUtil.DECIMAL_FORMAT_SCALE_3.format(duration / 1000D);
    }
}
