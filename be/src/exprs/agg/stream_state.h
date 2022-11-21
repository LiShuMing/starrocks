// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_STREAM_STATE_H
#define STARROCKS_STREAM_STATE_H

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

template <PrimitiveType PT>
struct StreamAggregateFunctionState {
    using T = RunTimeCppType<PT>;
    StreamAggregateFunctionState() {
        //        buffer = std::make_unique<std::map<T, size_t>>();
    }
    ~StreamAggregateFunctionState() {}

    void do_update(const T& value, size_t num_rows) {
        stream_buffer[value] += num_rows;
    }

    void reset() {
        stream_buffer.clear();
        need_sync = false;
    }

    // Or std::map to use sort properties?
    //    std::unique_ptr<std::map<T, size_t>> buffer;
    std::map<T, size_t> stream_buffer;
    bool need_sync {0};
};

template <PrimitiveType PT, typename State = StreamAggregateFunctionState<PT>, typename T = RunTimeCppType<PT>>
class StreamStateAggregateFunction final
        : public AggregateFunctionBatchHelper<State, StreamAggregateFunctionState<PT>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
//        DCHECK(!columns[0]->is_nullable() && !columns[0]->is_binary());
//        std::cout << "min/max update column:" << columns[0]->debug_string()
//                  << ", is nullable:" << (typeid(*columns[0]) == typeid(vectorized::NullableColumn))  << std::endl;
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        T value = column.get_data()[row_num];
        this->data(state).update_rows(value, 1);
        if (this->data(state).need_sync) {
            std::cout  << "need_sync:" << this->data(state).need_sync << std::endl;
            return;
        }
    }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        std::cout << "min/max retract column:" << columns[0]->debug_string() << std::endl;
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        T value = column.get_data()[row_num];
        this->data(state).update_rows(value, -1);
    }

//    StreamStateTableKind stream_state_table_kind() const override {
//        return StreamStateTableKind::Detail;
//    }

    void sync_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state,
                     StatusOr<vectorized::ChunkIteratorPtr> chunk_iter_or) const override{

    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(!column->is_nullable() && !column->is_binary());
//        const auto* input_column = down_cast<const InputColumnType*>(column);
//        T value = input_column->get_data()[row_num];
//        this->data(state).update_rows(value, -1);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
//        DCHECK(!to->is_nullable() && !to->is_binary());
//        down_cast<InputColumnType*>(to)->append(this->data(state).result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
//        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
//        DCHECK(!to->is_nullable() && !to->is_binary());
//        down_cast<InputColumnType*>(to)->append(this->data(state).result);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
//        DCHECK_GT(end, start);
//        InputColumnType* column = down_cast<InputColumnType*>(dst);
//        for (size_t i = start; i < end; ++i) {
//            column->get_data()[i] = this->data(state).result;
//        }
    }

    std::string get_name() const override { return "stream_state"; }
};
} // namespace starrrocks::vectorized

#endif //STARROCKS_STREAM_STATE_H
