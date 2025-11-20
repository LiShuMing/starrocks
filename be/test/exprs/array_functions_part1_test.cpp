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

#include "array_functions_base.h"

namespace starrocks {

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_length) {
    // []
    // NULL
    // [NULL]
    // [1]
    // [1, 2]
    {
        MutableColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum((int32_t)1)}));
        c->append_datum(Datum(DatumArray{Datum((int32_t)1), Datum((int32_t)2)}));

        auto result = ArrayFunctions::array_length(nullptr, {c}).value();
        EXPECT_EQ(5, result->size());

        EXPECT_EQ(result->get(0), Datum(0));
        EXPECT_EQ(result->get(1), kNullDatum);
        EXPECT_EQ(result->get(2), Datum(1));
        EXPECT_EQ(result->get(3), Datum(1));
        EXPECT_EQ(result->get(4), Datum(2));
    }

    // []
    // NULL
    // [NULL]
    // ["a"]
    // ["a", "b"]
    {
        MutableColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum("a")}));
        c->append_datum(Datum(DatumArray{Datum("a"), Datum("b")}));

        auto result = ArrayFunctions::array_length(nullptr, {c}).value();
        EXPECT_EQ(5, result->size());

        ASSERT_FALSE(result->get(0).is_null());
        ASSERT_TRUE(result->get(1).is_null());
        ASSERT_FALSE(result->get(2).is_null());
        ASSERT_FALSE(result->get(3).is_null());
        ASSERT_FALSE(result->get(4).is_null());

        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(2, result->get(4).get_int32());
    }

    // []
    // NULL
    // [NULL]
    // [[NULL]]
    // [[]]
    // [[],[]]
    // [[1], [2], [3]]
    {
        MutableColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{Datum()})}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{})}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{}), Datum(DatumArray{})}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{Datum((int32_t)1)}), Datum(DatumArray{Datum((int32_t)2)}),
                                         Datum(DatumArray{Datum((int32_t)3)})}));

        auto result = ArrayFunctions::array_length(nullptr, {c}).value();
        EXPECT_EQ(7, result->size());

        ASSERT_FALSE(result->get(0).is_null());
        ASSERT_TRUE(result->get(1).is_null());
        ASSERT_FALSE(result->get(2).is_null());
        ASSERT_FALSE(result->get(3).is_null());
        ASSERT_FALSE(result->get(4).is_null());
        ASSERT_FALSE(result->get(5).is_null());
        ASSERT_FALSE(result->get(6).is_null());

        auto datum = Datum(DatumArray{DatumArray{Datum()}});
        LOG(INFO) << "datum size=" << datum.get_array().size();

        LOG(INFO) << c->debug_string();
        LOG(INFO) << result->debug_string();
        EXPECT_EQ(0, result->get(0).get_int32());
        ASSERT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(1, result->get(4).get_int32());
        EXPECT_EQ(2, result->get(5).get_int32());
        EXPECT_EQ(3, result->get(6).get_int32());
    }

    // [] only null
    {
        MutableColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true, true, 10);

        auto result = ArrayFunctions::array_length(nullptr, {c}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    // [] only const
    {
        MutableColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        src_column = ConstColumn::create(std::move(src_column), 3);

        auto result = ArrayFunctions::array_length(nullptr, {src_column}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(4, result->get(1).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_cum_sum) {
    // []
    // NULL
    // [NULL]
    // [1]
    // [1,2,3,4,5]
    // [null,null,1, null]
    {
        MutableColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum((int64_t)1)}));
        c->append_datum(Datum(DatumArray{Datum((int64_t)1), Datum((int64_t)2), Datum((int64_t)3), Datum((int64_t)4),
                                         Datum((int64_t)5)}));
        c->append_datum(Datum(DatumArray{Datum(), Datum(), Datum((int64_t)1), Datum()}));

        auto result = ArrayFunctions::array_cum_sum_bigint(nullptr, {c}).value();
        EXPECT_EQ(6, result->size());

        ASSERT_FALSE(result->get(0).is_null());
        ASSERT_TRUE(result->get(1).is_null());
        ASSERT_FALSE(result->get(2).is_null());
        ASSERT_FALSE(result->get(3).is_null());
        ASSERT_FALSE(result->get(4).is_null());
        ASSERT_FALSE(result->get(5).is_null());

        EXPECT_EQ(0, result->get(0).get_array().size());
        EXPECT_EQ(1, result->get(2).get_array().size());
        EXPECT_EQ(1, result->get(3).get_array().size());
        EXPECT_EQ(5, result->get(4).get_array().size());
        EXPECT_EQ(4, result->get(5).get_array().size());
    }

    // [] only null
    {
        auto c = ColumnHelper::create_const_null_column(3);

        auto result = ArrayFunctions::array_cum_sum_bigint(nullptr, {c}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
    }

    // [] only const
    {
        MutableColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
        src_column->append_datum(Datum(DatumArray{Datum((int64_t)1), Datum((int64_t)2), Datum((int64_t)3),
                                                  Datum((int64_t)4), Datum((int64_t)5)}));
        auto c = ConstColumn::create(std::move(src_column), 3);
        auto result = ArrayFunctions::array_cum_sum_bigint(nullptr, {std::move(c)}).value();
        EXPECT_EQ(3, result->size());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_empty_array) {
    // array_contains([], 1)
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{(int32_t)1});

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains([], "abc")
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains(ARRAY<ARRAY<int>>[], [1])
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{Datum{(int32_t)1}}));

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains(ARRAY<ARRAY<int>>[], ARRAY<int>[])
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // multiple lines with const target:
    //  array_contains([], 1);
    //  array_contains([], 1);
    //  array_contains([], 1);
    //  array_contains([], 1);
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        DCHECK(target->is_constant());
        target->append_datum(Datum((int32_t)1));
        target->resize(4);

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
    }
    // multiple lines with different target:
    //  array_contains([], 1);
    //  array_contains([], 2);
    //  array_contains([], NULL);
    //  array_contains([], 3);
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        target->append_datum(Datum((int32_t)1));
        target->append_datum(Datum((int32_t)2));
        target->append_datum(Datum{});
        target->append_datum(Datum((int32_t)3));

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
    }
    // multiple lines with Only-NULL target:
    //  array_contains([], NULL);
    //  array_contains([], NULL);
    //  array_contains([], NULL);
    //  array_contains([], NULL);
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_const_null_column(1);
        target->as_mutable_ptr()->resize(4);

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());

        array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());

        array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_no_null) {
    /// Test class:
    ///  - Both the array elements and targets has NO NULL.

    // array_contains(array<boolean>[], 0) : 0
    // array_contains(array<boolean>[], 1) : 0
    // array_contains(array<boolean>[0], 0) : 1
    // array_contains(array<boolean>[0], 1) : 0
    // array_contains(array<boolean>[1], 0) : 0
    // array_contains(array<boolean>[1], 1) : 1
    // array_contains(array<boolean>[1,0], 0) : 1
    // array_contains(array<boolean>[1,0], 1) : 1
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});

        auto result = ArrayFunctions::array_contains_specific<TYPE_BOOLEAN>(nullptr, {array, target}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
        EXPECT_EQ(0, result->get(4).get_int8());
        EXPECT_EQ(1, result->get(5).get_int8());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(1, result->get(7).get_int8());
    }
    // array_contains([], 3) : 0
    // array_contains([2], 3) : 0
    // array_contains([1, 2, 3], 3) : 1
    // array_contains([3, 2, 1], 3) : 1
    // array_contains([2, 1, 3], 3) : 1
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{3});
        target->resize(5);

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
        EXPECT_EQ(1, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
    }
    // array_contains([], []) : 0
    // array_contains([[]], []) : 1
    // array_contains([["d", "o"], ["r"], ["i", "s"]], []) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["d"]) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["d", "o"]) : 1
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["o", "d"]) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["r"]) : 1
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["ri"]) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["r", "i"]) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["i", "s"]) : 1
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum(DatumArray{})});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});

        MutableColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(DatumArray{"d"});
        target->append_datum(DatumArray{"d", "o"});
        target->append_datum(DatumArray{"o", "d"});
        target->append_datum(DatumArray{"r"});
        target->append_datum(DatumArray{"ri"});
        target->append_datum(DatumArray{"r", "i"});
        target->append_datum(DatumArray{"i", "s"});

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(1, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
        EXPECT_EQ(0, result->get(5).get_int8());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(0, result->get(7).get_int8());
        EXPECT_EQ(0, result->get(8).get_int8());
        EXPECT_EQ(1, result->get(9).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_has_null_element) {
    // array_contains([NULL], "abc")
    // array_contains(["abc", NULL], "abc")
    // array_contains([NULL, "abc"], "abc")
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum{}});
        array->append_datum(DatumArray{"abc", Datum{}});
        array->append_datum(DatumArray{Datum{}, "abc"});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(1, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_has_null_target) {
    // array_contains(["abc", "def"], NULL)
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{"abc", "def"});

        // const-null column.
        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains(ARRAY<TINYINT>[1, 2, 3], 2)
    // array_contains(ARRAY<TINYINT>[1, 2, 3], 4)
    // array_contains(ARRAY<TINYINT>[1, 2, 3], NULL)
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_contains_specific<TYPE_TINYINT>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_has_null_element_and_target) {
    // array_contains([NULL], NULL)
    // array_contains([NULL, "abc"], NULL)
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), "abc"});

        // const-null column.
        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(2, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_EQ(1, result->get(1).get_int8());
    }
    // array_contains([NULL], NULL)
    // array_contains([NULL, [1,2]], NULL)
    // array_contains([NULL, [1,2]], [1,2])
    // array_contains([[1,2], NULL], [1,2])
    // array_contains([[1,2], NULL], NULL)
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), true);
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{1, 2});
        target->append_datum(DatumArray{1, 2});
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_EQ(1, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
        EXPECT_EQ(1, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_nullable_array) {
    // array_contains(["a", "b"], "c")
    // array_contains(NULL, "c")
    // array_contains(["a", "b", "c"], "c")
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains([["a"], ["b"]], ["c"])
    // array_contains(NULL, ["c"])
    // array_contains([["a", "b"], ["c"]], ["c"])
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{DatumArray{"a"}, DatumArray{"b"}});
        array->append_datum(Datum());
        array->append_datum(DatumArray{DatumArray{"a", "b"}, DatumArray{"c"}});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), false);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains(NULL, NULL)
    // array_contains(NULL, ["a"])
    // array_contains(NULL, [NULL])
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(Datum());
        array->append_datum(Datum());
        array->append_datum(Datum());

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), true);
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a"});
        target->append_datum(DatumArray{Datum()});

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_all) {
    // array_contains_all(["a", "b", "c"], ["c"])         -> 1
    // array_contains_all(NULL, ["c"])                    -> NULL
    // array_contains_all(["a", "b", "c"], NULL)          -> NULL
    // array_contains_all(["a", "b", NULL], NULL)         -> NULL
    // array_contains_all(["a", "b", NULL], ["a", NULL])  -> 1
    // array_contains_all(NULL, ["a", NULL])              -> NULL
    // array_contains_all(["a", "b", NULL], [NULL])       -> 1
    // array_contains_all(["a", "b", "c"], ["d"])         -> 0
    // array_contains_all(["a", "b", "c"], ["a", "d"])    -> 0
    // array_contains_all(["a", "b", "c"], ["a", "c"])    -> 1
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(DatumArray{"a", "b", Datum()});
        array->append_datum(DatumArray{"a", "b", Datum()});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", Datum()});
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(DatumArray{"a", "b", "c"});

        MutableColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a", Datum()});
        target->append_datum(DatumArray{"a", Datum()});
        target->append_datum(DatumArray{Datum()});
        target->append_datum(DatumArray{"d"});
        target->append_datum(DatumArray{"a", "d"});
        target->append_datum(DatumArray{"a", "c"});
        FunctionContext ctx;
        auto result = ArrayFunctions::array_contains_all_specific<TYPE_VARCHAR>(&ctx, {array, target}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
        EXPECT_TRUE(result->get(3).is_null());
        EXPECT_EQ(1, result->get(4).get_int8());
        EXPECT_TRUE(result->get(5).is_null());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(0, result->get(7).get_int8());
        EXPECT_EQ(0, result->get(8).get_int8());
        EXPECT_EQ(1, result->get(9).get_int8());
    }
    // array_contains_all([["a"], ["b"]], [["c"]])
    // array_contains_all(NULL, [["c"]])
    // array_contains_all([["a", "b"], ["c"], NULL], [["a", "b"], NULL])
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{Datum(DatumArray{"a"}), Datum(DatumArray{"b"})});
        array->append_datum(Datum());
        array->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum(DatumArray{"c"}), Datum()});

        MutableColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum()});

        auto result = ArrayFunctions::array_contains_all(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_empty_array) {
    // array_position([], 1) : 0
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{(int32_t)1});

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // array_position([], "abc"): 0
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // array_position(ARRAY<ARRAY<int>>[], [1]): 0
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{Datum{(int32_t)1}}));

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // array_position(ARRAY<ARRAY<int>>[], ARRAY<int>[]): 0
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // multiple lines with const target:
    //  array_position([], 1): 0;
    //  array_position([], 1): 0;
    //  array_position([], 1): 0;
    //  array_position([], 1): 0;
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        DCHECK(target->is_constant());
        target->append_datum(Datum((int32_t)1));
        target->resize(4);

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
    }
    // multiple lines with different target:
    //  array_position([], 1): 0;
    //  array_position([], 2): 0;
    //  array_position([], NULL): 0;
    //  array_position([], 3): 0;
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        target->append_datum(Datum((int32_t)1));
        target->append_datum(Datum((int32_t)2));
        target->append_datum(Datum{});
        target->append_datum(Datum((int32_t)3));

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
    }
    // multiple lines with Only-NULL target:
    //  array_position([], NULL): 0;
    //  array_position([], NULL): 0;
    //  array_position([], NULL): 0;
    //  array_position([], NULL): 0;
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_const_null_column(1);
        target->as_mutable_ptr()->resize(4);

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());

        array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());

        array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_no_null) {
    /// Test class:
    ///  - Both the array elements and targets has NO NULL.

    // array_position(array<boolean>[], 0) : 0
    // array_position(array<boolean>[], 1) : 0
    // array_position(array<boolean>[0], 0) : 1
    // array_position(array<boolean>[0], 1) : 0
    // array_position(array<boolean>[1], 0) : 0
    // array_position(array<boolean>[1], 1) : 1
    // array_position(array<boolean>[1,0], 0) : 2
    // array_position(array<boolean>[1,0], 1) : 1
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});

        auto result = ArrayFunctions::array_position_specific<TYPE_BOOLEAN>(nullptr, {array, target}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(1, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
        EXPECT_EQ(0, result->get(4).get_int32());
        EXPECT_EQ(1, result->get(5).get_int32());
        EXPECT_EQ(2, result->get(6).get_int32());
        EXPECT_EQ(1, result->get(7).get_int32());
    }
    // array_position([], 3) : 0
    // array_position([2], 3) : 0
    // array_position([1, 2, 3], 3) : 3
    // array_position([3, 2, 1], 3) : 1
    // array_position([2, 1, 3], 3) : 3
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{3});
        target->resize(5);

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(3, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(3, result->get(4).get_int32());
    }
    // array_position([], []) : 0
    // array_position([[]], []) : 1
    // array_position([["d", "o"], ["r"], ["i", "s"]], []) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["d"]) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["d", "o"]) : 1
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["o", "d"]) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["r"]) : 2
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["ri"]) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["r", "i"]) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["i", "s"]) : 3
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum(DatumArray{})});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});

        MutableColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(DatumArray{"d"});
        target->append_datum(DatumArray{"d", "o"});
        target->append_datum(DatumArray{"o", "d"});
        target->append_datum(DatumArray{"r"});
        target->append_datum(DatumArray{"ri"});
        target->append_datum(DatumArray{"r", "i"});
        target->append_datum(DatumArray{"i", "s"});

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
        EXPECT_EQ(1, result->get(4).get_int32());
        EXPECT_EQ(0, result->get(5).get_int32());
        EXPECT_EQ(2, result->get(6).get_int32());
        EXPECT_EQ(0, result->get(7).get_int32());
        EXPECT_EQ(0, result->get(8).get_int32());
        EXPECT_EQ(3, result->get(9).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_has_null_element) {
    // array_position([NULL], "abc"): 0
    // array_position(["abc", NULL], "abc"): 1
    // array_position([NULL, "abc"], "abc"): 2
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum{}});
        array->append_datum(DatumArray{"abc", Datum{}});
        array->append_datum(DatumArray{Datum{}, "abc"});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
        EXPECT_EQ(2, result->get(2).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_has_null_target) {
    // array_position(["abc", "def"], NULL): 0
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{"abc", "def"});

        // const-null column.
        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // array_position(ARRAY<TINYINT>[1, 2, 3], 2): 2
    // array_position(ARRAY<TINYINT>[1, 2, 3], 4): 0
    // array_position(ARRAY<TINYINT>[1, 2, 3], NULL): 0
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_position_specific<TYPE_TINYINT>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(2, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_has_null_element_and_target) {
    // array_position([NULL], NULL): 1
    // array_position([NULL, "abc"], NULL): 1
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), "abc"});

        // const-null column.
        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 1);

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(2, result->size());
        EXPECT_EQ(1, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
    }
    // array_position([NULL], NULL): 1
    // array_position([NULL, [1,2]], NULL): 1
    // array_position([NULL, [1,2]], [1,2]): 2
    // array_position([[1,2], NULL], [1,2]): 1
    // array_position([[1,2], NULL], NULL): 2
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), true);
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{1, 2});
        target->append_datum(DatumArray{1, 2});
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(1, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
        EXPECT_EQ(2, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(2, result->get(4).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_position_has_null_element_and_target_and_check_return_column_type) {
    // array_position([NULL], NULL): 1
    // array_position([NULL, "abc"], NULL): 1
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), "abc"});

        // const-null column.
        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(2, result->size());
        EXPECT_EQ(1, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
    }
    // array_position([NULL], NULL): 1
    // array_position([NULL, [1,2]], NULL): 1
    // array_position([NULL, [1,2]], [1,2]): 2
    // array_position([[1,2], NULL], [1,2]): 1
    // array_position([[1,2], NULL], NULL): 2
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), true);
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{1, 2});
        target->append_datum(DatumArray{1, 2});
        target->append_datum(Datum());

        auto result = ColumnHelper::cast_to<TYPE_INT>(
                ArrayFunctions::array_position_generic(nullptr, {array, target}).value());
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(1, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
        EXPECT_EQ(2, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(2, result->get(4).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_nullable_array) {
    // array_position(["a", "b"], "c"): 0
    // array_position(NULL, "c"): null
    // array_position(["a", "b", "c"], "c"): 3
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(3, result->get(2).get_int32());
    }
    // array_position([["a"], ["b"]], ["c"]): 0
    // array_position(NULL, ["c"]): null
    // array_position([["a", "b"], ["c"]], ["c"]): 2
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{DatumArray{"a"}, DatumArray{"b"}});
        array->append_datum(Datum());
        array->append_datum(DatumArray{DatumArray{"a", "b"}, DatumArray{"c"}});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), false);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(2, result->get(2).get_int32());
    }
    // array_position(NULL, NULL): null
    // array_position(NULL, ["a"]): null
    // array_position(NULL, [NULL]): null
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(Datum());
        array->append_datum(Datum());
        array->append_datum(Datum());

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), true);
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a"});
        target->append_datum(DatumArray{Datum()});

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
}

} // namespace starrocks
