// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exprs/binary_functions.h"

#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "exprs/base64.h"
#include "exprs/encryption_functions.h"
#include "exprs/string_functions.h"
#include "gutil/strings/escaping.h"

namespace starrocks {

namespace {
template <bool is_throw_exception>
static StatusOr<ColumnPtr> _from_hex(const ColumnPtr& src_column) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(src_column);
    const int size = src_column->size();
    ColumnBuilder<TYPE_VARBINARY> result(size);
    for (int row = 0; row < size; ++row) {
        if (src_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        if (src_value.size == 0) {
            result.append_null();
            continue;
        }
        int input_len = src_value.size;
        if (input_len % 2 != 0) {
            if constexpr (is_throw_exception) {
                return Status::InvalidArgument("Input's not a legal hex-encoded value, length is not correct:" +
                                               std::to_string(input_len));
            } else {
                result.append_null();
                continue;
            }
        }

        int hex_len = input_len / 2;
        std::unique_ptr<char[]> p;
        p.reset(new char[hex_len]);

        if (!strings::a2b_hex_checked(src_value.get_data(), p.get(), hex_len)) {
            if constexpr (is_throw_exception) {
                return Status::InvalidArgument("Invalid input's not a legal hex-encoded value:" +
                                               src_value.to_string());
            } else {
                result.append_null();
                continue;
            }
        }
        result.append(Slice(p.get(), hex_len));
    }

    return result.build(src_column->is_constant());
}

template <bool is_throw_exception>
static StatusOr<ColumnPtr> _from_base64(const ColumnPtr& src_column) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(src_column);
    const int size = src_column->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (src_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        if (src_value.size == 0) {
            result.append_null();
            continue;
        } else if (src_value.size > config::max_length_for_to_base64) {
            std::stringstream ss;
            ss << "to_base64 not supported length > " << config::max_length_for_to_base64;
            throw std::runtime_error(ss.str());
        }

        int cipher_len = (size_t)(4.0 * ceil((double)src_value.size / 3.0)) + 1;
        char p[cipher_len];

        int len = base64_encode2((unsigned char*)src_value.data, src_value.size, (unsigned char*)p);
        if (len < 0) {
            if constexpr (is_throw_exception) {
                return Status::InvalidArgument("Invalid input's not a legal base64-encoded value:" +
                                               src_value.to_string());
            } else {
                result.append_null();
                continue;
            }
        }

        result.append(Slice(p, len));
    }

    return result.build(src_column->is_constant());
}

} // end namespace

// to_binary
StatusOr<ColumnPtr> BinaryFunctions::to_binary(FunctionContext* context, const Columns& columns) {
    auto state = reinterpret_cast<BinaryFormatState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    auto& src_column = columns[0];
    const int size = src_column->size();
    ColumnBuilder<TYPE_VARBINARY> result(size);
    auto to_binary_type = state->to_binary_type;
    switch (to_binary_type) {
    case BinaryFormatType::UTF8: {
        return src_column;
        break;
    }
    case BinaryFormatType::ENCODE64:
        return _from_base64<true>(src_column);
    default:
        return _from_hex<true>(src_column);
    }
    return Status::OK();
}

// to_binary_prepare
Status BinaryFunctions::to_binary_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }
    auto* state = new BinaryFormatState();
    context->set_function_state(scope, state);

    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    auto column = context->get_constant_column(1);
    auto to_binary_type = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    std::string to_binary_type_str = to_binary_type.to_string();
    state->to_binary_type = BinaryFormatState::to_binary_format(to_binary_type_str);

    return Status::OK();
}

Status BinaryFunctions::to_binary_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* state = reinterpret_cast<BinaryFormatState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

// to_binary
StatusOr<ColumnPtr> BinaryFunctions::from_binary(FunctionContext* context, const Columns& columns) {
    auto state = reinterpret_cast<BinaryFormatState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    auto& src_column = columns[0];
    const int size = src_column->size();
    ColumnBuilder<TYPE_VARBINARY> result(size);
    auto to_binary_type = state->to_binary_type;
    switch (to_binary_type) {
    case BinaryFormatType::UTF8: {
        return src_column;
        break;
    }
    case BinaryFormatType::ENCODE64:
        return EncryptionFunctions::to_base64(context, columns);
    default:
        return StringFunctions::hex_string(context, columns);
    }
    return Status::OK();
}

// to_binary_prepare
Status BinaryFunctions::from_binary_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }
    auto* state = new BinaryFormatState();
    context->set_function_state(scope, state);

    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    auto column = context->get_constant_column(1);
    auto to_binary_type = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    std::string to_binary_type_str = to_binary_type.to_string();
    state->to_binary_type = BinaryFormatState::to_binary_format(to_binary_type_str);

    return Status::OK();
}

Status BinaryFunctions::from_binary_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* state = reinterpret_cast<BinaryFormatState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

} // namespace starrocks
