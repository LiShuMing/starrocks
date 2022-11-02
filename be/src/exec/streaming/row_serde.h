// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_ROW_SERDE_H
#define STARROCKS_ROW_SERDE_H

#include "exec/streaming/streaming_fdw.h"
#include "runtime/mem_pool.h"

namespace starrocks::streaming {

// DatumRowSerde used to serialize/deserialize a column to/from an in-memory array.
class DatumRowSerde {
public:
    // 0 means does not support the type of column
    static int64_t max_serialized_size(const DatumRow& row);

    // Return nullptr on error.
    static uint8_t* serialize(const DatumRow& row, uint8_t* buff);

    // Return nullptr on error.
    static const uint8_t* deserialize(const uint8_t* buff, DatumRow* column);
};

}
#endif //STARROCKS_ROW_SERDE_H
