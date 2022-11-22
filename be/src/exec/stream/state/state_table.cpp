// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/state/state_table.h"

namespace starrocks::stream {

// If input is one pk row, result must be in one chunk!
ChunkPtrOr StateTable::get_chunk(const DatumRow& key) {
    auto chunk_or = get_chunk_iter(key);
    if (!chunk_or.ok()) {
        return chunk_or.status();
    }
    auto chunk_iter = chunk_or.value();

    auto t_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1);
    auto status = chunk_iter->get_next(t_chunk.get());
    if (!status.ok() && !status.is_end_of_file()) {
        return status;
    }
#ifdef BE_TEST
    {
        // iterator should reach the end of file.
        status = chunk_iter->get_next(t_chunk.get());
        DCHECK(status.is_end_of_file());
    }
#endif
    chunk_iter->close();
    return t_chunk;
}

std::vector<ChunkPtrOr> StateTable::get_chunks(const std::vector<DatumRow>& keys) {
    std::vector<ChunkPtrOr> ret;
    ret.reserve(keys.size());
    for (auto& key : keys) {
        ret.emplace_back(get_chunk(key));
    }
    return ret;
}

} // namespace starrocks::stream
