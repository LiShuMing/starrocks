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

#pragma once

#include <memory>
#include <functional>
#include <vector>

#include "util/threadpool.h"
#include "common/status.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/MVMaintenance_types.h"

namespace starrocks {
class ExecEnv;
class TNetworkAddress;
}  // namespace starrocks

namespace starrocks::pipeline {
class QueryContext;

class ExecStateReporter {
public:
    ExecStateReporter();

    static TReportExecStatusParams create_report_exec_status_params(QueryContext* query_ctx,
                                                                    FragmentContext* fragment_ctx, const Status& status,
                                                                    bool done);
    static Status report_exec_status(const TReportExecStatusParams& params, ExecEnv* exec_env,
                                     const TNetworkAddress& fe_addr);

    void submit(std::function<void()>&& report_task);

    // STREAM MV
    static TMVMaintenanceTasks create_report_epoch_params(const QueryContext* query_ctx,
                                                          const std::vector<FragmentContext*>& fragment_ctxs);

    static Status report_epoch(const TMVMaintenanceTasks& params, ExecEnv* exec_env, const TNetworkAddress& fe_addr);

private:
    std::unique_ptr<ThreadPool> _thread_pool;
};
} // namespace starrocks::pipeline
