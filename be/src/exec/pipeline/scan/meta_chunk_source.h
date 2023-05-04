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

#include <stdint.h>
#include <memory>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exec/pipeline/scan/meta_scan_context.h"
#include "common/status.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks {
class MetaScanner;
class RuntimeProfile;
class RuntimeState;
}  // namespace starrocks
namespace starrocks::workgroup {
class WorkGroup;
}  // namespace starrocks::workgroup

namespace starrocks::pipeline {

class MetaChunkSource final : public ChunkSource {
public:
    MetaChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                    MetaScanContextPtr scan_ctx);

    ~MetaChunkSource() override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const override;

    MetaScanContextPtr _scan_ctx;

    std::shared_ptr<MetaScanner> _scanner;
};

} // namespace starrocks::pipeline
