// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <gtest/gtest.h>

#include "butil/file_util.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/user_function_cache.h"
#include "service/service.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/update_manager.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/timezone_utils.h"

namespace starrocks {
void wait_for_query_contexts_finish(ExecEnv* exec_env, size_t max_loop_cnt_cfg) {
    if (max_loop_cnt_cfg == 0) {
        return;
    }

    size_t running_quries = exec_env->query_context_mgr()->size();
    size_t loop_cnt = 0;

    while (running_quries && loop_cnt < max_loop_cnt_cfg) {
        DLOG(INFO) << running_quries << " fragment(s) are still running...";
        sleep(10);
        running_quries = exec_env->query_context_mgr()->size();
        loop_cnt++;
    }
}

} // namespace starrocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    if (getenv("STARROCKS_HOME") == nullptr) {
        fprintf(stderr, "you need set STARROCKS_HOME environment variable.\n");
        exit(-1);
    }
    // Load config from config file.
    std::string conffile = std::string(getenv("STARROCKS_HOME")) + "/conf/be_test.conf";
    if (!starrocks::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

    std::cout << "load be conf:" << conffile << std::endl;

    //    butil::FilePath curr_dir(std::filesystem::current_path());
    //    butil::FilePath storage_root;
    //    CHECK(butil::CreateNewTempDirectory("tmp_mv_ut_", &storage_root));
    //    starrocks::config::storage_root_path = storage_root.value();
    //    starrocks::config::enable_event_based_compaction_framework = false;

    starrocks::init_glog("be_test", true);

    starrocks::CpuInfo::init();
    starrocks::DiskInfo::init();
    starrocks::MemInfo::init();
    starrocks::UserFunctionCache::instance()->init(starrocks::config::user_function_dir);

    starrocks::vectorized::date::init_date_cache();
    starrocks::TimezoneUtils::init_time_zones();

    std::vector<starrocks::StorePath> paths;
    paths.emplace_back(starrocks::config::storage_root_path);

    auto* exec_env = starrocks::ExecEnv::GetInstance();
    //    // Pagecache is turned on by default, and some test cases require cache to be turned on,
    //    // and some test cases do not. For easy management, we turn cache off during unit test
    //    // initialization. If there are test cases that require Pagecache, it must be responsible
    //    // for managing it.
    //    starrocks::config::disable_storage_page_cache = true;
    exec_env->init_mem_tracker();
    starrocks::ExecEnv::init(exec_env, paths);

    int r = RUN_ALL_TESTS();

    //    while (!starrocks::k_starrocks_exit.load()) {
    //        sleep(10);
    //    }

    //    starrocks::wait_for_query_contexts_finish(exec_env, starrocks::config::loop_count_wait_fragments_finish);

    // clear some trash objects kept in tablet_manager so mem_tracker checks will not fail
    //    starrocks::StorageEngine::instance()->tablet_manager()->start_trash_sweep();
    //    (void)butil::DeleteFile(storage_root, true);
    starrocks::vectorized::TEST_clear_all_columns_this_thread();
    //    // delete engine
    //    starrocks::StorageEngine::instance()->stop();
    // destroy exec env
    starrocks::tls_thread_status.set_mem_tracker(nullptr);
    starrocks::ExecEnv::destroy(exec_env);

    starrocks::shutdown_logging();

    return r;
}
