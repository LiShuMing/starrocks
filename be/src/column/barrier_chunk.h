// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "column/chunk.h"

namespace starrocks::vectorized {

using UInt8ColumnPtr = std::shared_ptr<vectorized::UInt8Column>;

using BarrierChunk = Chunk;
using BarrierChunkPtr = std::shared_ptr<BarrierChunk>;

enum class TriggerMode { kOffsetTrigger = 0, kProcessTimeTrigger, kManualTrigger };

struct EpochInfo {
    // epoch marker id
    int64_t epoch_id;
    // last lsn offset
    int64_t last_lsn_offset;
    // max binlog duration which this epoch will run
    int64_t max_binlog_ms;
    // max binlog offset which this epoch will run
    int64_t max_offsets;
    TriggerMode trigger_mode;

    std::string debug_string() const {
        std::stringstream ss;
        ss << "epoch_id=" << epoch_id << ", last_lsn_offset=" << last_lsn_offset << ", max_binlog_ms=" << max_binlog_ms
           << ", max_offsets=" << max_offsets << ", trigger_mode=" << (int)(trigger_mode);
        return ss.str();
    }
};

/**
 * BarrierChunk is used for Stream MV which generates a Barrier message.
 */
class BarrierChunkExtraData final : public ChunkExtraData {
public:
    BarrierChunkExtraData(EpochInfo epoch_info) : _epoch_info(std::move(epoch_info)) {}
    virtual ~BarrierChunkExtraData() = default;

    const EpochInfo& epoch_info() const { return _epoch_info; }
    std::vector<ChunkExtraDataMeta> chunk_data_metas() const override { return {}; }
    void filter(const Buffer<uint8_t>& selection) const override {}
    void filter_range(const Buffer<uint8_t>& selection, size_t from, size_t to) const override {}
    ChunkExtraDataPtr clone_empty(size_t size) const override { return nullptr; }
    void append(const ChunkExtraDataPtr& src, size_t offset, size_t count) override {}
    void append_selective(const ChunkExtraDataPtr& src, const uint32_t* indexes, uint32_t from,
                          uint32_t size) override {}
    size_t memory_usage() const override { return 0; }
    size_t bytes_usage(size_t from, size_t size) const override { return 0; }

    // serialize/deserialize to exchange, now only supports encode_level = 0
    // TODO: support encode_level configuration.
    int64_t max_serialized_size(const int encode_level = 0) override { return 0; }
    uint8_t* serialize(uint8_t* buff, bool sorted = false, const int encode_level = 0) override { return buff; }
    const uint8_t* deserialize(const uint8_t* buff, bool sorted = false, const int encode_level = 0) override {
        return buff;
    }

private:
    EpochInfo _epoch_info;
};

class BarrierChunkConverter {
public:
    static BarrierChunkPtr make_barrier_chunk(EpochInfo epoch_info) {
        auto chunk = std::make_shared<Chunk>();
        auto extra_data = std::make_shared<BarrierChunkExtraData>(std::move(epoch_info));
        chunk->set_extra_data(std::move(extra_data));
        return chunk;
    }
    static EpochInfo get_barrier_info(BarrierChunkPtr barrier_chunk) {
        DCHECK(barrier_chunk->has_extra_data());
        auto extra_data = dynamic_cast<BarrierChunkExtraData*>(barrier_chunk->get_extra_data().get());
        return extra_data->epoch_info();
    }
    static bool is_barrier_chunk(const Chunk& chunk) {
        if (chunk.has_extra_data() && typeid(*chunk.get_extra_data()) == typeid(BarrierChunkExtraData)) {
            return true;
        }
        return false;
    }
    static bool is_barrier_chunk(ChunkPtr chunk) {
        if (!chunk) {
            return false;
        }
        return is_barrier_chunk(*chunk);
    }
    static bool is_barrier_chunk(Chunk* chunk) {
        if (!chunk) {
            return false;
        }
        return is_barrier_chunk(*chunk);
    }
};
//class BarrierChunk : public Chunk {
//public:
//    BarrierChunk(int64_t epoch_id) : Chunk(), _epoch_id(epoch_id) {}
//
//    BarrierChunk() = default;
//
//    BarrierChunk(BarrierChunk&& other) = default;
//    BarrierChunk& operator=(BarrierChunk&& other) = default;
//
//    ~BarrierChunk() override = default;
//
//    // Disallow copy and assignment.
//    BarrierChunk(const BarrierChunk& other) = delete;
//    BarrierChunk& operator=(const BarrierChunk& other) = delete;
//
//    std::string debug_string() override { return "[BarrierChunk] epoch_id=" + std::to_string(_epoch_id); }
//
//private:
//    EpochInfo ;
//};

} // namespace starrocks::vectorized
