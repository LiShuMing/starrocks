// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_STATETABLE_H
#define STARROCKS_STATETABLE_H

#include "common/status.h"
#include "exec/streaming/streaming_fdw.h"

namespace starrocks::streaming {

class StateTable {
public:
    StateTable() {}
    ~StateTable() = default;

    DatumRowPtr get_row(const DatumRow& keys) {
        // TODO(lism): to be implemented.
        return nullptr;
    }

    Status flush(vectorized::ChunkPtr* chunk);
};

}
#endif //STARROCKS_STATETABLE_H
