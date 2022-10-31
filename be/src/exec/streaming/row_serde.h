// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_ROW_SERDE_H
#define STARROCKS_ROW_SERDE_H

#include "exec/streaming/streaming_fdw.h"
#include "runtime/mem_pool.h"

namespace starrocks::streaming {

class RowSerializer {
public:
    RowSerializer() {}

    ~RowSerializer() = default;

    // serializing one rows
    void serialize_row(const Columns& columns, size_t idx, uint8_t* buff) {
        for (const auto& column: columns) {
            column->serialize(idx, buff);
        }
    }

    // deserialize keys
    void deserialize_row(uint8_t* buffer, DatumRow& row);
private:
    // Store buffers which can be reused in the incremental compute.
    std::unique_ptr<MemPool> _mem_pool;
    // Max serialized size for all group_by keys.
    uint32_t _max_keys_size = 8;
    // Buffer which is used to store group_by keys that can be reused for each chunk.
    uint8_t* _keys_buffer;
    // Group_by keys' serialized sizes for each chunk.
    Buffer<uint32_t> _keys_slice_sizes;
};

}
#endif //STARROCKS_ROW_SERDE_H
