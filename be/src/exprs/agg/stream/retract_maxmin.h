// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/stream/detail_retractable.h"
#include "gutil/casts.h"
#include "storage/chunk_helper.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

template <LogicalType PT, typename = guard::Guard>
struct MaxAggregateDataRetractable : public StreamAggregateFunctionState<PT> {};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, IntegralPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();

    void reset() { result = std::numeric_limits<T>::lowest(); }
};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, FloatPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();
    void reset() { result = std::numeric_limits<T>::lowest(); }
};

template <>
struct MaxAggregateDataRetractable<TYPE_DECIMALV2, guard::Guard> : public StreamAggregateFunctionState<TYPE_DECIMALV2> {
    DecimalV2Value result = DecimalV2Value::get_min_decimal();

    void reset() { result = DecimalV2Value::get_min_decimal(); }
};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, DecimalPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = get_min_decimal<T>();
    void reset() { result = get_min_decimal<T>(); }
};

template <>
struct MaxAggregateDataRetractable<TYPE_DATETIME, guard::Guard> : public StreamAggregateFunctionState<TYPE_DATETIME> {
    TimestampValue result = TimestampValue::MIN_TIMESTAMP_VALUE;

    void reset() { result = TimestampValue::MIN_TIMESTAMP_VALUE; }
};

template <>
struct MaxAggregateDataRetractable<TYPE_DATE, guard::Guard> : public StreamAggregateFunctionState<TYPE_DATE> {
    DateValue result = DateValue::MIN_DATE_VALUE;

    void reset() { result = DateValue::MIN_DATE_VALUE; }
};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, StringPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
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
struct MinAggregateDataRetractable : public StreamAggregateFunctionState<PT> {};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, IntegralPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, FloatPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <>
struct MinAggregateDataRetractable<TYPE_DECIMALV2, guard::Guard> : public StreamAggregateFunctionState<TYPE_DECIMALV2> {
    DecimalV2Value result = DecimalV2Value::get_max_decimal();

    void reset() { result = DecimalV2Value::get_max_decimal(); }
};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, DecimalPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = get_max_decimal<T>();
    void reset() { result = get_max_decimal<T>(); }
};
template <>
struct MinAggregateDataRetractable<TYPE_DATETIME, guard::Guard> : public StreamAggregateFunctionState<TYPE_DATETIME> {
    TimestampValue result = TimestampValue::MAX_TIMESTAMP_VALUE;

    void reset() { result = TimestampValue::MAX_TIMESTAMP_VALUE; }
};

template <>
struct MinAggregateDataRetractable<TYPE_DATE, guard::Guard> : public StreamAggregateFunctionState<TYPE_DATE> {
    DateValue result = DateValue::MAX_DATE_VALUE;

    void reset() { result = DateValue::MAX_DATE_VALUE; }
};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, StringPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
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
        if (this->data(state).need_sync) {
            return;
        }
        OP()(this->data(state), value);
    }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        T value = column.get_data()[row_num];
        this->data(state).update_rows(value, -1);
        if (this->data(state).need_sync) {
            return;
        }
        this->data(state).need_sync = OP::need_sync(this->data(state), value);
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
        if (!this->data(state).need_sync) {
            return;
        }
        auto buffer = this->data(state).stream_buffer;
        for (auto iter = buffer.cbegin(); iter != buffer.cend(); iter++) {
            if (iter->second > 0) {
                const T& value = iter->first;
                OP()(this->data(state), value);
            }
        }
    }

    void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state, const Columns& to,
                       Column* count) const override {
        DCHECK((*to[0]).is_numeric());
        DCHECK((*to[1]).is_numeric());
        InputColumnType* column0 = down_cast<InputColumnType*>(to[0].get());
        Int64Column* column1 = down_cast<Int64Column*>(to[1].get());
        auto buffer = this->data(state).stream_buffer;
        for (auto iter = buffer.cbegin(); iter != buffer.cend(); iter++) {
            // is it possible that count is negative?
            DCHECK_LE(0, iter->second);
            column0->append(iter->first);
            column1->append(iter->second);
        }
        Int64Column* count_col = down_cast<Int64Column*>(count);
        count_col->append(buffer.size());
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
        if (this->data(state).need_sync) {
            return;
        }
        this->data(state).need_sync = OP::need_sync(this->data(state), value);
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
        if (!this->data(state).need_sync) {
            return;
        }
        auto buffer = this->data(state).stream_buffer;
        for (auto iter = buffer.cbegin(); iter != buffer.cend(); iter++) {
            if (iter->second > 0) {
                const Slice& value = iter->first;
                OP()(this->data(state), value);
            }
        }
    }

    void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state, const Columns& to,
                       Column* count) const override {
        DCHECK((*to[0]).is_binary());
        DCHECK((*to[1]).is_numeric());
        BinaryColumn* column0 = down_cast<BinaryColumn*>(to[0].get());
        Int64Column* column1 = down_cast<Int64Column*>(to[1].get());
        auto buffer = this->data(state).stream_buffer;
        for (auto iter = buffer.cbegin(); iter != buffer.cend(); iter++) {
            // is it possible that count is negative?
            DCHECK_LE(0, iter->second);
            column0->append(iter->first);
            column1->append(iter->second);
        }
        Int64Column* count_col = down_cast<Int64Column*>(count);
        count_col->append(buffer.size());
    }

    std::string get_name() const override { return "retract_maxmin"; }
};

} // namespace starrocks::vectorized