// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "storage/chunk_helper.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

// TODO: Add a MaxMinAggregateFunctionWithDetailRetractable class;
// TODO: Support detail agg state reusable between different agg stats.
template <PrimitiveType PT>
struct StreamAggregateFunctionState {
    using T = RunTimeCppType<PT>;
    StreamAggregateFunctionState() = default;
    ~StreamAggregateFunctionState() = default;

    void update_rows(const T& value, int64_t num_rows) {
        if constexpr (std::is_same_v<T, int64_t>) {
            std:: cout << "update_rows, value:" << value << ", num_rows:" << num_rows << std::endl;
        }
        auto iter = stream_buffer.find(value);
        if (iter != stream_buffer.end()) {
            std::cout << "update_rows found" << std::endl;
        } else {
            std::cout << "update_rows not found" << std::endl;
        }
        stream_buffer[value] = num_rows;
    }

    std::map<T, int64_t> stream_buffer;
    bool need_sync {false};
};

template <PrimitiveType PT, typename = guard::Guard>
struct MaxAggregateData : public StreamAggregateFunctionState<PT> {};

template <PrimitiveType PT>
struct MaxAggregateData<PT, IntegralPTGuard<PT>> : public StreamAggregateFunctionState<PT>  {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();

    void reset() { result = std::numeric_limits<T>::lowest(); }
};

template <PrimitiveType PT>
struct MaxAggregateData<PT, FloatPTGuard<PT>> :public StreamAggregateFunctionState<PT>  {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();
    void reset() { result = std::numeric_limits<T>::lowest(); }
};

template <>
struct MaxAggregateData<TYPE_DECIMALV2, guard::Guard> :public StreamAggregateFunctionState<TYPE_DECIMALV2>  {
    DecimalV2Value result = DecimalV2Value::get_min_decimal();

    void reset() { result = DecimalV2Value::get_min_decimal(); }
};

template <PrimitiveType PT>
struct MaxAggregateData<PT, DecimalPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = get_min_decimal<T>();
    void reset() { result = get_min_decimal<T>(); }
};

template <>
struct MaxAggregateData<TYPE_DATETIME, guard::Guard>: public StreamAggregateFunctionState<TYPE_DATETIME>  {
    TimestampValue result = TimestampValue::MIN_TIMESTAMP_VALUE;

    void reset() { result = TimestampValue::MIN_TIMESTAMP_VALUE; }
};

template <>
struct MaxAggregateData<TYPE_DATE, guard::Guard> : public StreamAggregateFunctionState<TYPE_DATE> {
    DateValue result = DateValue::MIN_DATE_VALUE;

    void reset() { result = DateValue::MIN_DATE_VALUE; }
};

template <PrimitiveType PT>
struct MaxAggregateData<PT, StringPTGuard<PT>> : public StreamAggregateFunctionState<PT>  {
    int32_t size = -1;
    raw::RawVector<uint8_t> buffer;

    bool has_value() const { return buffer.size() > 0; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <PrimitiveType PT, typename = guard::Guard>
struct MinAggregateData : public StreamAggregateFunctionState<PT> {};

template <PrimitiveType PT>
struct MinAggregateData<PT, IntegralPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <PrimitiveType PT>
struct MinAggregateData<PT, FloatPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <>
struct MinAggregateData<TYPE_DECIMALV2, guard::Guard> : public StreamAggregateFunctionState<TYPE_DECIMALV2> {
    DecimalV2Value result = DecimalV2Value::get_max_decimal();

    void reset() { result = DecimalV2Value::get_max_decimal(); }
};

template <PrimitiveType PT>
struct MinAggregateData<PT, DecimalPTGuard<PT>> : public StreamAggregateFunctionState<PT> {
    using T = RunTimeCppType<PT>;
    T result = get_max_decimal<T>();
    void reset() { result = get_max_decimal<T>(); }
};
template <>
struct MinAggregateData<TYPE_DATETIME, guard::Guard> : public StreamAggregateFunctionState<TYPE_DATETIME> {
    TimestampValue result = TimestampValue::MAX_TIMESTAMP_VALUE;

    void reset() { result = TimestampValue::MAX_TIMESTAMP_VALUE; }
};

template <>
struct MinAggregateData<TYPE_DATE, guard::Guard> : public StreamAggregateFunctionState<TYPE_DATE> {
    DateValue result = DateValue::MAX_DATE_VALUE;

    void reset() { result = DateValue::MAX_DATE_VALUE; }
};

template <PrimitiveType PT>
struct MinAggregateData<PT, StringPTGuard<PT>> : public StreamAggregateFunctionState<PT> {

    int32_t size = -1;
    Buffer<uint8_t> buffer;

    bool has_value() const { return size > -1; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <PrimitiveType PT, typename State, typename = guard::Guard>
struct MaxElement {
    using T = RunTimeCppType<PT>;
    static bool need_sync(State& state, const T& right) {
        return state.result <= right;
    }
    void operator()(State& state, const T& right) const { state.result = std::max<T>(state.result, right); }
};

template <PrimitiveType PT, typename State, typename = guard::Guard>
struct MinElement {
    using T = RunTimeCppType<PT>;
    static bool need_sync(State& state, const T& right) {
        return state.result >= right;
    }
    void operator()(State& state, const T& right) const { state.result = std::min<T>(state.result, right); }
};

template <PrimitiveType PT>
struct MaxElement<PT, MaxAggregateData<PT>, StringPTGuard<PT>> {
    static bool need_sync(MaxAggregateData<PT>& state, const Slice& right) {
        return !state.has_value() || state.slice().compare(right) <= 0;
    }

    void operator()(MaxAggregateData<PT>& state, const Slice& right) const {
        if (!state.has_value() || state.slice().compare(right) < 0) {
            state.buffer.resize(right.size);
            memcpy(state.buffer.data(), right.data, right.size);
            state.size = right.size;
        }
    }
};

template <PrimitiveType PT>
struct MinElement<PT, MinAggregateData<PT>, StringPTGuard<PT>> {
    static bool need_sync(MinAggregateData<PT>& state, const Slice& right) {
        return !state.has_value() || state.slice().compare(right) >= 0;
    }

    void operator()(MinAggregateData<PT>& state, const Slice& right) const {
        if (!state.has_value() || state.slice().compare(right) > 0) {
            state.buffer.resize(right.size);
            memcpy(state.buffer.data(), right.data, right.size);
            state.size = right.size;
        }
    }
};

template <PrimitiveType PT, typename State, class OP, typename T = RunTimeCppType<PT>, typename = guard::Guard>
class MaxMinAggregateFunction final
        : public AggregateFunctionBatchHelper<State, MaxMinAggregateFunction<PT, State, OP, T>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(!columns[0]->is_nullable() && !columns[0]->is_binary());
        std::cout << "min/max update column:" << columns[0]->debug_string()
                  << ", is nullable:" << (typeid(*columns[0]) == typeid(vectorized::NullableColumn))  << std::endl;

        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        T value = column.get_data()[row_num];
        this->data(state).update_rows(value, 1);
        if (this->data(state).need_sync) {
            std::cout  << "need_sync:" << this->data(state).need_sync << std::endl;
            return;
        }
        OP()(this->data(state), value);
        if constexpr (std::is_same_v<T, int64_t>) {
            std::cout  << "result:" << this->data(state).result << std::endl;
        }
    }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        std::cout << "min/max retract column:" << columns[0]->debug_string() << std::endl;
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        T value = column.get_data()[row_num];
        this->data(state).update_rows(value, -1);
        if (this->data(state).need_sync) {
            return;
        }
        this->data(state).need_sync = OP::need_sync(this->data(state), value);
    }

    void restore_detail(FunctionContext* ctx, size_t chunk_size,
                        const Columns& columns, AggDataPtr __restrict state) const override {
        auto& column = down_cast<InputColumnType&>(*columns[0]);
        DCHECK((*columns[1]).is_numeric());
        for (size_t i = 0; i < chunk_size; i++) {
            T value = column.get_data()[i];
            int64_t count = columns[1]->get(i).get_int64();
            std::cout << "min/max restore column0:" << columns[0]->debug_string() << std::endl;
            std::cout << "min/max restore column1:" << columns[1]->debug_string() << std::endl;
            (this->data(state)).update_rows(value, count);
        }
    }

    void sync_detail(FunctionContext* ctx, AggDataPtr __restrict state) const override {
        std::cout << "min/max sync detail:" << this->data(state).need_sync << std::endl;
        if (!this->data(state).need_sync) {
            return;
        }
        auto buffer = this->data(state).stream_buffer;
        for (auto iter = buffer.cbegin(); iter != buffer.cend(); iter++) {
            if constexpr (std::is_same_v<T, int64_t>) {
                std::cout << "min/max sync detail, key:" << iter->first << ", value:" << iter->second << std::endl;
            }
            if (iter->second > 0) {
                const T& value = iter->first;
                OP()(this->data(state), value);
            }
        }
    }

    void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state,
                       const Columns& to, Column* count) const override {
        DCHECK((*to[0]).is_numeric());
        DCHECK((*to[1]).is_numeric());
        InputColumnType* column0 = down_cast<InputColumnType*>(to[0].get());
        Int64Column * column1 = down_cast<Int64Column*>(to[1].get());
        auto buffer = this->data(state).stream_buffer;
        for (auto iter = buffer.cbegin(); iter != buffer.cend(); iter++) {
            // is it possible that count is negative?
            DCHECK_LE(0, iter->second);
            column0->append(iter->first);
            column1->append(iter->second);
        }
        Int64Column * count_col = down_cast<Int64Column*>(count);
        count_col->append(buffer.size());
    }

    StreamStateTableKind stream_state_table_kind(bool need_retract) const override {
        if (need_retract) {
            return StreamStateTableKind::Detail_Result;
        } else {
            return StreamStateTableKind::Result;
        }
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

    std::string get_name() const override { return "maxmin"; }
};

template <PrimitiveType PT, typename State, class OP>
class MaxMinAggregateFunction<PT, State, OP, RunTimeCppType<PT>, StringPTGuard<PT>> final
        : public AggregateFunctionBatchHelper<State, MaxMinAggregateFunction<PT, State, OP, RunTimeCppType<PT>>> {
public:
    using T = RunTimeCppType<PT>;
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK((*columns[0]).is_binary());
        Slice value = columns[0]->get(row_num).get_slice();
        this->data(state).update_rows(value, 1);
        OP()(this->data(state), value);
    }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        std::cout << "min/max retract column:" << columns[0]->debug_string() << std::endl;
        DCHECK((*columns[0]).is_binary());
        Slice value = columns[0]->get(row_num).get_slice();
        this->data(state).update_rows(value, -1);
        if (this->data(state).need_sync) {
            return;
        }
        this->data(state).need_sync = OP::need_sync(this->data(state), value);
    }

    void restore_detail(FunctionContext* ctx, size_t chunk_size,
                        const Columns& columns, AggDataPtr __restrict state) const override {
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

    void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state,
                       const Columns& to, Column * count) const override {
        DCHECK((*to[0]).is_binary());
        DCHECK((*to[1]).is_numeric());
        BinaryColumn* column0 = down_cast<BinaryColumn*>(to[0].get());
        Int64Column * column1 = down_cast<Int64Column*>(to[1].get());
        auto buffer = this->data(state).stream_buffer;
        for (auto iter = buffer.cbegin(); iter != buffer.cend(); iter++) {
            // is it possible that count is negative?
            DCHECK_LE(0, iter->second);
            column0->append(iter->first);
            column1->append(iter->second);
        }
        Int64Column * count_col = down_cast<Int64Column*>(count);
        count_col->append(buffer.size());
    }

    StreamStateTableKind stream_state_table_kind(bool need_retract) const override {
        if (need_retract) {
            return StreamStateTableKind::Detail_Result;
        } else {
            return StreamStateTableKind::Result;
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        Slice value = column->get(row_num).get_slice();
        OP()(this->data(state), value);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        column->append(this->data(state).slice());
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        column->append(this->data(state).slice());
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<BinaryColumn*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->append(this->data(state).slice());
        }
    }

    std::string get_name() const override { return "maxmin"; }
};

} // namespace starrocks::vectorized
