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

#pragma once

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <unordered_set>

#include "column/const_column.h"
#include "column/map_column.h"
#include "exprs/array_functions.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

inline TypeDescriptor array_type(const TypeDescriptor& child_type) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.emplace_back(child_type);
    return t;
}

inline TypeDescriptor array_type(const LogicalType& child_type) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.resize(1);
    t.children[0].type = child_type;
    t.children[0].len = child_type == TYPE_VARCHAR ? 10 : child_type == TYPE_CHAR ? 10 : -1;
    return t;
}

class ArrayFunctionsTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}

    TypeDescriptor TYPE_ARRAY_BOOLEAN = array_type(TYPE_BOOLEAN);
    TypeDescriptor TYPE_ARRAY_TINYINT = array_type(TYPE_TINYINT);
    TypeDescriptor TYPE_ARRAY_SMALLINT = array_type(TYPE_SMALLINT);
    TypeDescriptor TYPE_ARRAY_INT = array_type(TYPE_INT);
    TypeDescriptor TYPE_ARRAY_LARGEINT = array_type(TYPE_LARGEINT);
    TypeDescriptor TYPE_ARRAY_FLOAT = array_type(TYPE_FLOAT);
    TypeDescriptor TYPE_ARRAY_DOUBLE = array_type(TYPE_DOUBLE);
    TypeDescriptor TYPE_ARRAY_VARCHAR = array_type(TYPE_VARCHAR);
    TypeDescriptor TYPE_ARRAY_ARRAY_INT = array_type(array_type(TYPE_INT));
    TypeDescriptor TYPE_ARRAY_ARRAY_VARCHAR = array_type(array_type(TYPE_VARCHAR));

    TypeDescriptor TYPE_ARRAY_BIGINT = array_type(TYPE_BIGINT);
    TypeDescriptor TYPE_ARRAY_DATE = array_type(TYPE_DATE);
    TypeDescriptor TYPE_ARRAY_DATETIME = array_type(TYPE_DATETIME);

protected:
    template <typename CppType>
    void _check_array(const Buffer<CppType>& check_values, const DatumArray& value);

    template <typename CppType>
    void _check_array_nullable(const Buffer<CppType>& check_values, const Buffer<uint8_t>& nulls,
                               const DatumArray& value);

    FunctionContext _ctx;
};

template <typename CppType>
void ArrayFunctionsTest::_check_array(const Buffer<CppType>& check_values, const DatumArray& value) {
    ASSERT_EQ(check_values.size(), value.size());
    if constexpr (std::is_same_v<CppType, uint8_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_uint8());
        }
    } else if constexpr (std::is_same_v<CppType, int8_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int8());
        }
    } else if constexpr (std::is_same_v<CppType, int16_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int16());
        }
    } else if constexpr (std::is_same_v<CppType, int32_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int32());
        }
    } else if constexpr (std::is_same_v<CppType, int64_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int64());
        }
    } else if constexpr (std::is_same_v<CppType, int128_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int128());
        }
    } else if constexpr (std::is_same_v<CppType, float>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_float());
        }
    } else if constexpr (std::is_same_v<CppType, double>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_double());
        }
    } else if constexpr (std::is_same_v<CppType, Slice>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_slice());
        }
    } else {
        ASSERT_TRUE(false);
    }
}

template <typename CppType>
void ArrayFunctionsTest::_check_array_nullable(const Buffer<CppType>& check_values, const Buffer<uint8_t>& nulls,
                                               const DatumArray& value) {
    ASSERT_EQ(check_values.size(), value.size());
    if constexpr (std::is_same_v<CppType, int32_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            if (nulls[i]) {
                ASSERT_TRUE(value[i].is_null());
            } else {
                ASSERT_FALSE(value[i].is_null());
                ASSERT_EQ(check_values[i], value[i].get_int32());
            }
        }
    } else if constexpr (std::is_same_v<CppType, Slice>) {
        for (size_t i = 0; i < value.size(); i++) {
            if (nulls[i]) {
                ASSERT_TRUE(value[i].is_null());
            } else {
                ASSERT_FALSE(value[i].is_null());
                ASSERT_EQ(check_values[i], value[i].get_slice());
            }
        }
    } else {
        ASSERT_TRUE(false);
    }
}

} // namespace starrocks
