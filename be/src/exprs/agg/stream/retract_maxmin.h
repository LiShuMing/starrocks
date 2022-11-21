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
struct MaxAggregateDataRetractable<PT, IntegralPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();

    void reset() { result = std::numeric_limits<T>::lowest(); }
};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, FloatPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();
    void reset() { result = std::numeric_limits<T>::lowest(); }
};

template <>
struct MaxAggregateDataRetractable<TYPE_DECIMALV2, guard::Guard> : public StreamDetailState<TYPE_DECIMALV2> {
    DecimalV2Value result = DecimalV2Value::get_min_decimal();

    void reset() { result = DecimalV2Value::get_min_decimal(); }
};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, DecimalPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;
    T result = get_min_decimal<T>();
    void reset() { result = get_min_decimal<T>(); }
};

template <>
struct MaxAggregateDataRetractable<TYPE_DATETIME, guard::Guard> : public StreamDetailState<TYPE_DATETIME> {
    TimestampValue result = TimestampValue::MIN_TIMESTAMP_VALUE;

    void reset() { result = TimestampValue::MIN_TIMESTAMP_VALUE; }
};

template <>
struct MaxAggregateDataRetractable<TYPE_DATE, guard::Guard> : public StreamDetailState<TYPE_DATE> {
    DateValue result = DateValue::MIN_DATE_VALUE;

    void reset() { result = DateValue::MIN_DATE_VALUE; }
};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, StringPTGuard<PT>> : public StreamDetailState<PT> {
    int32_t size = -1;
    raw::RawVector<uint8_t> buffer;

    bool has_value() const { return buffer.size() > 0; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <LogicalType PT, typename = guard::Guard>
struct MinAggregateDataRetractable : public StreamDetailState<PT> {};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, IntegralPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, FloatPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <>
struct MinAggregateDataRetractable<TYPE_DECIMALV2, guard::Guard> : public StreamDetailState<TYPE_DECIMALV2> {
    DecimalV2Value result = DecimalV2Value::get_max_decimal();

    void reset() { result = DecimalV2Value::get_max_decimal(); }
};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, DecimalPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;
    T result = get_max_decimal<T>();
    void reset() { result = get_max_decimal<T>(); }
};
template <>
struct MinAggregateDataRetractable<TYPE_DATETIME, guard::Guard> : public StreamDetailState<TYPE_DATETIME> {
    TimestampValue result = TimestampValue::MAX_TIMESTAMP_VALUE;

    void reset() { result = TimestampValue::MAX_TIMESTAMP_VALUE; }
};

template <>
struct MinAggregateDataRetractable<TYPE_DATE, guard::Guard> : public StreamDetailState<TYPE_DATE> {
    DateValue result = DateValue::MAX_DATE_VALUE;

    void reset() { result = DateValue::MAX_DATE_VALUE; }
};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, StringPTGuard<PT>> : public StreamDetailState<PT> {
    int32_t size = -1;
    Buffer<uint8_t> buffer;

    bool has_value() const { return size > -1; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <LogicalType PT, typename State, class OP, typename T = RunTimeCppType<PT>, typename = guard::Guard>
class MaxMinAggregateFunctionRetractable final : public MaxMinAggregateFunction<PT, State, OP> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

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
        if (!this->data(state).is_sync()) {
            this->data(state).mark_sync(OP::is_sync(this->data(state), value));
        }
    }

    void restore_detail(FunctionContext* ctx, size_t chunk_size, const Columns& columns,
                        AggDataPtr __restrict state) const override {
        auto& column = down_cast<InputColumnType&>(*columns[0]);
        DCHECK((*columns[1]).is_numeric());
        for (size_t i = 0; i < chunk_size; i++) {
            T value = column.get_data()[i];
            int64_t count = columns[1]->get(i).get_int64();
            (this->data(state)).update_rows(value, count);
        }
    }

    void sync_detail(FunctionContext* ctx, AggDataPtr __restrict state) const override {
        if (this->data(state).is_sync()) {
            auto& detail_state = this->data(state).detail_state();
            for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
                if (iter->second > 0) {
                    const T& value = iter->first;
                    OP()(this->data(state), value);
                }
            }
        }
    }

    void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state, const Columns& to,
                       Column* count) const override {
        DCHECK((*to[0]).is_numeric());
        DCHECK((*to[1]).is_numeric());
        InputColumnType* column0 = down_cast<InputColumnType*>(to[0].get());
        Int64Column* column1 = down_cast<Int64Column*>(to[1].get());
        auto& detail_state = this->data(state).detail_state();
        for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
            // is it possible that count is negative?
            DCHECK_LE(0, iter->second);
            column0->append(iter->first);
            column1->append(iter->second);
        }
        Int64Column* count_col = down_cast<Int64Column*>(count);
        count_col->append(detail_state.size());
    }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(!column->is_nullable() && !column->is_binary());
        const auto* input_column = down_cast<const InputColumnType*>(column);
        T value = input_column->get_data()[row_num];
        OP()(this->data(state), value);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable() && !to->is_binary());
        down_cast<InputColumnType*>(to)->append(this->data(state).result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable() && !to->is_binary());
        down_cast<InputColumnType*>(to)->append(this->data(state).result);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        InputColumnType* column = down_cast<InputColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = this->data(state).result;
        }
    }

    std::string get_name() const override { return "retract_maxmin"; }
};

template <LogicalType PT, typename State, class OP>
class MaxMinAggregateFunctionRetractable<PT, State, OP, RunTimeCppType<PT>, StringPTGuard<PT>> final
        : public MaxMinAggregateFunction<PT, State, OP> {
public:
    using T = RunTimeCppType<PT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK((*columns[0]).is_binary());
        Slice value = columns[0]->get(row_num).get_slice();
        this->data(state).update_rows(value, 1);
        OP()(this->data(state), value);
    }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        DCHECK((*columns[0]).is_binary());
        Slice value = columns[0]->get(row_num).get_slice();
        this->data(state).update_rows(value, -1);
        if (!this->data(state).is_sync()) {
            this->data(state).mark_sync(OP::is_sync(this->data(state), value));
        }
    }

    void restore_detail(FunctionContext* ctx, size_t chunk_size, const Columns& columns,
                        AggDataPtr __restrict state) const override {
        DCHECK((*columns[0]).is_binary());
        DCHECK((*columns[1]).is_numeric());
        for (size_t i = 0; i < chunk_size; i++) {
            Slice value = columns[0]->get(i).get_slice();
            int64_t count = columns[1]->get(i).get_int64();
            (this->data(state)).update_rows(value, count);
        }
    }

    void sync_detail(FunctionContext* ctx, AggDataPtr __restrict state) const override {
        if (this->data(state).is_sync()) {
            auto& detail_state = this->data(state).detail_state();
            for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
                if (iter->second > 0) {
                    const Slice& value = iter->first;
                    OP()(this->data(state), value);
                }
            }
        }
    }

    void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state, const Columns& to,
                       Column* count) const override {
        DCHECK((*to[0]).is_binary());
        DCHECK((*to[1]).is_numeric());
        BinaryColumn* column0 = down_cast<BinaryColumn*>(to[0].get());
        Int64Column* column1 = down_cast<Int64Column*>(to[1].get());
        auto detail_state = this->data(state).detail_state();
        for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
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

} // namespace starrocks::vectorized