// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/stream/stream_detail_state.h"
#include "gutil/casts.h"
#include "storage/chunk_helper.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

template <LogicalType PT, typename = guard::Guard>
struct MaxAggregateDataRetractable : public StreamDetailState<PT> {};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, FixedLengthPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;

    MaxAggregateDataRetractable() {}

    T result = RunTimeTypeLimits<PT>::min_value();
    void reset_result() { result = RunTimeTypeLimits<PT>::min_value(); }

    void reset() {
        StreamDetailState<PT>::reset();
        reset_result();
    }
};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, StringPTGuard<PT>> : public StreamDetailState<PT> {
    int32_t size = -1;
    raw::RawVector<uint8_t> buffer;

    bool has_value() const { return buffer.size() > 0; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset_result() {
        buffer.clear();
        size = -1;
    }
    void reset() {
        StreamDetailState<PT>::reset();
        reset_result();
    }
};

template <LogicalType PT, typename = guard::Guard>
struct MinAggregateDataRetractable : public StreamDetailState<PT> {};
template <LogicalType PT>
struct MinAggregateDataRetractable<PT, FixedLengthPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;

    T result = RunTimeTypeLimits<PT>::max_value();
    void reset_result() { result = RunTimeTypeLimits<PT>::max_value(); }
    void reset() {
        StreamDetailState<PT>::reset();
        reset_result();
    }
};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, StringPTGuard<PT>> : public StreamDetailState<PT> {
    int32_t size = -1;
    Buffer<uint8_t> buffer;

    bool has_value() const { return size > -1; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset_result() {
        buffer.clear();
        size = -1;
    }
    void reset() {
        StreamDetailState<PT>::reset();
        reset_result();
    }
};

template <LogicalType PT, typename State, class OP, typename T = RunTimeCppType<PT>, typename = guard::Guard>
class MaxMinAggregateFunctionRetractable final : public MaxMinAggregateFunction<PT, State, OP> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

    AggStateTableKind agg_state_table_kind(bool is_append_only) const override {
        if (is_append_only) {
            return AggStateTableKind::Result;
        } else {
            return AggStateTableKind::Detail_Result;
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(!columns[0]->is_nullable() && !columns[0]->is_binary());
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        T value = column.get_data()[row_num];
        this->data(state).update_rows(value, 1);
        // If is_sync=true, use detail state in the final.
        if (!this->data(state).is_sync()) {
            OP()(this->data(state), value);
        }
    }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        T value = column.get_data()[row_num];
        this->data(state).update_rows(value, -1);

        // reset state to restore from detail
        if (!this->data(state).is_sync() && OP::is_sync(this->data(state), value)) {
            this->data(state).mark_sync(true);
            this->data(state).reset_result();
        }
    }

    void restore_detail(FunctionContext* ctx, size_t num_row, const std::vector<const Column*>& columns,
                        AggDataPtr __restrict state) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        DCHECK((*columns[1]).is_numeric());
        T value = column->get_data()[num_row];
        int64_t count = columns[1]->get(0).get_int64();
        (this->data(state)).update_non_found(value, count);
    }

    void sync_detail(FunctionContext* ctx, AggDataPtr __restrict state, size_t chunk_size,
                     const Columns& columns) const override {
        DCHECK(this->data(state).is_sync());

        // sync incremental data
        if (!this->data(state).is_sync_incremental()) {
            auto& detail_state = this->data(state).detail_state();
            for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
                if (iter->second <= 0) {
                    continue;
                }
                const T& value = iter->first;
#ifdef BE_TEST
                if constexpr (std::is_same_v<T, int64_t>) {
                    VLOG_ROW << "[!is_sync_incremental] sync_detail, value:" << value << ", count:" << iter->second;
                }
#endif
                OP()(this->data(state), value);
            }
            this->data(state).mark_incremental_sync(true);
        }

        // sync previous records from detail state table
        auto& column = down_cast<InputColumnType&>(*columns[0]);
        DCHECK((*columns[1]).is_numeric());
        for (size_t i = 0; i < chunk_size; i++) {
            T value = column.get_data()[i];
            if (this->data(state).find_in_incremental(value)) {
                continue;
            }
            OP()(this->data(state), value);
        }
    }

    void output_detail_sync(FunctionContext* ctx, size_t chunk_size, Column* to,
                            AggDataPtr __restrict state) const override {
        UInt8Column* sync_col = down_cast<UInt8Column*>(to);
        uint8_t is_sync = this->data(state).is_sync();
        sync_col->append(is_sync);
    }

    void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state, const Columns& to,
                       Column* count) const override {
        DCHECK((*to[0]).is_numeric());
        DCHECK((*to[1]).is_numeric());
        InputColumnType* column0 = down_cast<InputColumnType*>(to[0].get());
        Int64Column* column1 = down_cast<Int64Column*>(to[1].get());
        auto& detail_state = this->data(state).detail_state();
        for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
#ifdef BE_TEST
            if constexpr (std::is_same_v<T, int64_t>) {
                VLOG_ROW << "output_detail, value:" << iter->first << ", count:" << iter->second;
            }
#endif
            // is it possible that count is negative?
            DCHECK_LE(0, iter->second);
            column0->append(iter->first);
            column1->append(iter->second);
        }
        Int64Column* count_col = down_cast<Int64Column*>(count);
        count_col->append(detail_state.size());
    }

    std::string get_name() const override { return "retract_maxmin"; }
};

template <LogicalType PT, typename State, class OP>
class MaxMinAggregateFunctionRetractable<PT, State, OP, RunTimeCppType<PT>, StringPTGuard<PT>> final
        : public MaxMinAggregateFunction<PT, State, OP> {
public:
    using T = RunTimeCppType<PT>;

    //    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
    //                size_t row_num) const override {
    //        DCHECK((*columns[0]).is_binary());
    //        Slice value = columns[0]->get(row_num).get_slice();
    //        this->data(state).update_rows(value, 1);
    //        OP()(this->data(state), value);
    //    }
    //
    //    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
    //                 size_t row_num) const override {
    //        DCHECK((*columns[0]).is_binary());
    //        Slice value = columns[0]->get(row_num).get_slice();
    //        this->data(state).update_rows(value, -1);
    //        if (!this->data(state).is_sync() && OP::is_sync(this->data(state), value)) {
    //            this->data(state).mark_sync(true);
    //            this->data(state).reset_result();
    //        }
    //    }
    //
    //    void restore_detail(FunctionContext* ctx, size_t chunk_size, const Columns& columns,
    //                        AggDataPtr __restrict state) const override {
    //        DCHECK((*columns[0]).is_binary());
    //        DCHECK((*columns[1]).is_numeric());
    //        for (size_t i = 0; i < chunk_size; i++) {
    //            Slice value = columns[0]->get(i).get_slice();
    //            int64_t count = columns[1]->get(i).get_int64();
    //            (this->data(state)).update_rows(value, count);
    //        }
    //    }
    //
    //    void sync_detail(FunctionContext* ctx, AggDataPtr __restrict state) const override {
    //        if (this->data(state).is_sync()) {
    //            auto& detail_state = this->data(state).detail_state();
    //            for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
    //                if (iter->second > 0) {
    //                    const Slice& value = iter->first;
    //                    OP()(this->data(state), value);
    //                }
    //            }
    //        }
    //    }
    //
    //    void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state, const Columns& to,
    //                       Column* count) const override {
    //        DCHECK((*to[0]).is_binary());
    //        DCHECK((*to[1]).is_numeric());
    //        BinaryColumn* column0 = down_cast<BinaryColumn*>(to[0].get());
    //        Int64Column* column1 = down_cast<Int64Column*>(to[1].get());
    //        auto detail_state = this->data(state).detail_state();
    //        for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
    //            // is it possible that count is negative?
    //            DCHECK_LE(0, iter->second);
    //            column0->append(iter->first);
    //            column1->append(iter->second);
    //        }
    //        Int64Column* count_col = down_cast<Int64Column*>(count);
    //        count_col->append(detail_state.size());
    //    }

    std::string get_name() const override { return "retract_maxmin"; }
};

} // namespace starrocks::vectorized