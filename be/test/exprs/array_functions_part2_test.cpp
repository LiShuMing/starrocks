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

TEST_F(ArrayFunctionsTest, array_remove_empty_array) {
    // array_remove([], 1) -> []
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{(int32_t)1});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
    }

    // array_remove([], "abc") -> []
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
    }

    // array_remove([[]], [1]) -> [[]]
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{Datum{(int32_t)1}}));

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());

        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());
    }

    // array_remove([[]], []) -> []
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());

        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());
    }

    // array_remove([], 1) -> []
    // array_remove([], 1) -> []
    // array_remove([], 1) -> []
    // array_remove([], 1) -> []
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

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());

        // 1st row: array_remove([], 1) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([], 1) -> []
        row = result->get(1).get_array();
        EXPECT_EQ(0, row.size());

        // 3rd row: array_remove([], 1) -> []
        row = result->get(2).get_array();
        EXPECT_EQ(0, row.size());

        // 4th row: array_remove([], 1) -> []
        row = result->get(3).get_array();
        EXPECT_EQ(0, row.size());
    }

    // array_remove([], 1) -> []
    // array_remove([], 2) -> []
    // array_remove([], NULL) -> []
    // array_remove([], 3) -> []
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

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());

        // 1st row: array_remove([], 1) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([], 2) -> []
        row = result->get(1).get_array();
        EXPECT_EQ(0, row.size());

        // 3rd row: array_remove([], NULL) -> []
        row = result->get(2).get_array();
        EXPECT_EQ(0, row.size());

        // 4th row: array_remove([], 3) -> []
        row = result->get(3).get_array();
        EXPECT_EQ(0, row.size());
    }

    // array_remove([], NULL) -> []
    // array_remove([], NULL) -> []
    // array_remove([], NULL) -> []
    // array_remove([], NULL) -> []
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_const_null_column(1);
        target->resize(4);

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
        EXPECT_EQ(0, result->get(1).get_array().size());
        EXPECT_EQ(0, result->get(2).get_array().size());
        EXPECT_EQ(0, result->get(3).get_array().size());

        array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
        EXPECT_EQ(0, result->get(1).get_array().size());
        EXPECT_EQ(0, result->get(2).get_array().size());
        EXPECT_EQ(0, result->get(3).get_array().size());

        array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
        EXPECT_EQ(0, result->get(1).get_array().size());
        EXPECT_EQ(0, result->get(2).get_array().size());
        EXPECT_EQ(0, result->get(3).get_array().size());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_no_null) {
    // array_remove([], false)            -> []
    // array_remove([], true)             -> []
    // array_remove([false], false)       -> []
    // array_remove([false], true)        -> [false]
    // array_remove([true], false)        -> [true]
    // array_remove([true], true)         -> []
    // array_remove([true, false], false) -> [true]
    // array_remove([true, false], true)  -> [false]
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

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(8, result->size());

        // 1st row: array_remove([], false) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([], true) -> []
        row = result->get(1).get_array();
        EXPECT_EQ(0, row.size());

        // 3rd row: array_remove([false], false) -> []
        row = result->get(2).get_array();
        EXPECT_EQ(0, row.size());

        // 4th row: array_remove([false], true) -> [false]
        row = result->get(3).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(0, row[0].get_int8());

        // 5th row: array_remove([true], false) -> [true]
        row = result->get(4).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(1, row[0].get_int8());

        // 6th row: array_remove([true], true) -> []
        row = result->get(5).get_array();
        EXPECT_EQ(0, row.size());

        // 7th row: array_remove([true, false], false) -> [true]
        row = result->get(6).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(1, row[0].get_int8());

        // 8th row: array_remove([true, false], true) -> [false]
        row = result->get(7).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(0, row[0].get_int8());
    }

    // array_remove([], 3) -> []
    // array_remove([2], 3) -> [2]
    // array_remove([1, 2, 3], 3) -> [1, 2]
    // array_remove([3, 2, 1], 3) -> [2, 1]
    // array_remove([2, 1, 3], 3) -> [2, 1]
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

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());

        // 1st row: array_remove([], 3) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([2], 3) -> [2]
        row = result->get(1).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(2, row[0].get_int32());

        // 3rd row: array_remove([1, 2, 3], 3) -> [1, 2]
        row = result->get(2).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(1, row[0].get_int32());
        EXPECT_EQ(2, row[1].get_int32());

        // 4th row: array_remove([3, 2, 1], 3) -> [2, 1]
        row = result->get(3).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(2, row[0].get_int32());
        EXPECT_EQ(1, row[1].get_int32());

        // 5th row: array_remove([2, 1, 3], 3) -> [2, 1]
        row = result->get(4).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(2, row[0].get_int32());
        EXPECT_EQ(1, row[1].get_int32());
    }

    // array_remove([], [])                                      -> []
    // array_remove([[]], [])                                    -> []
    // array_remove([["d", "o"], ["r"], ["i", "s"]], [])         -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["d])       -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["d", "o"]) -> [["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["o", "d"]) -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["r"])      -> [["d", "o"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["ri"])     -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["r", "i"]) -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["i", "s"]) -> [["d", "o"], ["r"]]
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

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(10, result->size());

        // 1st row: array_remove([], []) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([[]], []) -> []
        row = result->get(1).get_array();
        EXPECT_EQ(0, row.size());

        // 3rd row: array_remove([["d", "o"], ["r"], ["i", "s"]], []) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(2).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 4th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["d]) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(3).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 5th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["d", "o"]) -> [["r"], ["i", "s"]]
        row = result->get(4).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(1, row[0].get_array().size()); //["r"]
        EXPECT_EQ("r", row[0].get_array()[0].get_slice());
        EXPECT_EQ(2, row[1].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[1].get_array()[0].get_slice());
        EXPECT_EQ("s", row[1].get_array()[1].get_slice());

        // 6th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["o", "d"]) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(5).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); // ["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 7th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["r"]) -> [["d", "o"], ["i", "s"]]
        row = result->get(6).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(2, row[1].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[1].get_array()[0].get_slice());
        EXPECT_EQ("s", row[1].get_array()[1].get_slice());

        // 8th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["ri"]) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(7).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 9th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["r", "i"]) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(8).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 10th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["i", "s"]) -> [["d", "o"], ["r"]]
        row = result->get(9).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_has_null_element) {
    // array_remove([NULL], "abc")        -> [NULL]
    // array_remove(["abc", NULL], "abc") -> [NULL]
    // array_remove([NULL, "abc"], "abc") -> [NULL]
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum{}});
        array->append_datum(DatumArray{"abc", Datum{}});
        array->append_datum(DatumArray{Datum{}, "abc"});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        // 1st row: array_remove([NULL], "abc") -> [NULL]
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(1, row.size());

        // 2nd row: array_remove(["abc", NULL], "abc") -> [NULL]
        row = result->get(1).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_TRUE(row[0].is_null());

        // 3rd row: array_remove([NULL, "abc"], "abc") -> [NULL]
        row = result->get(2).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_TRUE(row[0].is_null());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_has_null_target) {
    {
        // array_remove(["abc", "def"], NULL) -> ["abc", "def"]
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{"abc", "def"});

        // const-null column.
        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);
        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());

        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("abc", row[0].get_slice());
        EXPECT_EQ("def", row[1].get_slice());
    }

    // array_remove([1, 2, 3], 2) -> [1, 3]
    // array_remove([1, 2, 3], 4) -> [1, 2, 3]
    // array_remove([1, 2, 3], NULL) -> [1, 2, 3]
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        // 1st row: array_remove([1, 2, 3], 2) -> [1, 3]
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(1, row[0].get_int8());
        EXPECT_EQ(3, row[1].get_int8());

        // 2nd row: array_remove([1, 2, 3], 4) -> [1, 2, 3]
        row = result->get(1).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(1, row[0].get_int8());
        EXPECT_EQ(2, row[1].get_int8());
        EXPECT_EQ(3, row[2].get_int8());

        // 3rd row: array_remove([1, 2, 3], NULL) -> [1, 2, 3]
        row = result->get(2).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(1, row[0].get_int8());
        EXPECT_EQ(2, row[1].get_int8());
        EXPECT_EQ(3, row[2].get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_has_null_element_and_target) {
    // array_remove([NULL], NULL)  -> []
    // array_remove([NULL, "abc"], NULL) -> ["abc"]
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), "abc"});

        // const-null column.
        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(2, result->size());

        // 1st row: array_remove([NULL], NULL) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([NULL, "abc"], NULL) -> ["abc"]
        row = result->get(1).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ("abc", row[0].get_slice());
    }

    // array_remove([NULL], NULL)           -> []
    // array_remove([NULL, [1, 2]], NULL)   -> [[1 ,2]]
    // array_remove([NULL, [1, 2]], [1, 2]) -> [NULL]
    // array_remove([[1, 2], NULL], [1, 2]) -> [NULL]
    // array_remove([NULL, [1, 2]], NULL)   -> [[1, 2]]
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

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());

        // 1st row: array_remove([NULL], NULL) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([NULL, [1, 2]], NULL)   -> [[1 ,2]]
        row = result->get(1).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(2, row[0].get_array().size());
        EXPECT_EQ(1, row[0].get_array()[0].get_int32());
        EXPECT_EQ(2, row[0].get_array()[1].get_int32());

        // 3rd row: array_remove([NULL, [1, 2]], [1, 2]) -> [NULL]
        row = result->get(2).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_TRUE(row[0].is_null());

        // 4th row: array_remove([[1, 2], NULL], [1, 2]) -> [NULL]
        row = result->get(3).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_TRUE(row[0].is_null());

        // 5th row: array_remove([NULL, [1, 2]], NULL)   -> [[1, 2]]
        row = result->get(4).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(2, row[0].get_array().size());
        EXPECT_EQ(1, row[0].get_array()[0].get_int32());
        EXPECT_EQ(2, row[0].get_array()[1].get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_nullable_array) {
    {
        // array_remove(["a", "b"], "c")      -> ["a", "b"]
        // array_remove(NULL, "c")            -> NULL
        // array_remove(["a", "b", "c"], "c") -> ["a", "b"]
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        // 1st row: array_remove(["a", "b"], "c")      -> ["a", "b"]
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("a", row[0].get_slice());
        EXPECT_EQ("b", row[1].get_slice());

        // 2nd row: array_remove(NULL, "c") -> NULL
        EXPECT_TRUE(result->get(1).is_null());

        // 3rd row: array_remove(["a", "b", "c"], "c") -> ["a", "b"]
        row = result->get(2).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("a", row[0].get_slice());
        EXPECT_EQ("b", row[1].get_slice());
    }

    // array_remove([["a"], ["b"]], ["c"]) -> [["a"], ["b"]]
    // array_remove(NULL, ["c"])           -> NULL
    // array_remove([["a", "b"], ["c"]])   -> [["a", "b"]]
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{DatumArray{"a"}, DatumArray{"b"}});
        array->append_datum(Datum());
        array->append_datum(DatumArray{DatumArray{"a", "b"}, DatumArray{"c"}});

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), false);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        // 1st row: array_remove([["a"], ["b"]], ["c"]) -> [["a"], ["b"]]
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("a", row[0].get_array()[0].get_slice());
        EXPECT_EQ("b", row[1].get_array()[0].get_slice());

        // 2nd row: array_remove(NULL, ["c"]) -> NULL
        EXPECT_TRUE(result->get(1).is_null());

        // 3rd row: array_remove([["a", "b"], ["c"]])   -> [["a", "b"]]
        row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("a", row[0].get_array()[0].get_slice());
        EXPECT_EQ("b", row[1].get_array()[0].get_slice());
    }

    // array_remove(NULL, NULL)  -> NULL
    // array_remove(NULL, ["a"]) -> NULL
    // array_remove(NULL, [])    -> NULL
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(Datum());
        array->append_datum(Datum());
        array->append_datum(Datum());

        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), true);
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a"});
        target->append_datum(DatumArray{Datum()});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
    // array_remove(NULL,1)
    {
        auto array = ColumnHelper::create_const_null_column(3);
        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_append) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        MutableColumnPtr null = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_append(nullptr, {array, null}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(1, result->get(0).get_array().size());
        EXPECT_TRUE(result->get(0).get_array()[0].is_null());
    }
    // array_append(['abc'], 'def')
    // array_append(['xyz', 'xxx'], 'def')
    // array_append([], 'def')
    // array_append(NULL, 'def')
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"abc"});
        array->append_datum(DatumArray{"xyz", "xxx"});
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum());

        MutableColumnPtr data = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        data->append_datum("def");

        auto result = ArrayFunctions::array_append(nullptr, {array, data}).value();
        EXPECT_EQ(4, result->size());
        // First row.
        EXPECT_EQ(2, result->get(0).get_array().size());
        EXPECT_EQ("abc", result->get(0).get_array()[0].get_slice());
        EXPECT_EQ("def", result->get(0).get_array()[1].get_slice());
        // Second row.
        EXPECT_EQ(3, result->get(1).get_array().size());
        EXPECT_EQ("xyz", result->get(1).get_array()[0].get_slice());
        EXPECT_EQ("xxx", result->get(1).get_array()[1].get_slice());
        EXPECT_EQ("def", result->get(1).get_array()[2].get_slice());
        // Third row.
        EXPECT_EQ(1, result->get(2).get_array().size());
        EXPECT_EQ("def", result->get(2).get_array()[0].get_slice());
        // Last row.
        EXPECT_TRUE(result->get(3).is_null());
    }
    // array_append([], [])                       -> [[]]
    // array_append([[0,1], [2])                  -> [[0,1], [2]]
    // array_append([NULL], [3,4])                -> [NULL, [3,4]]
    // array_append(NULL, NULL)                   -> NULL
    // array_append([[10, 11],[12,13]], [14,15])  -> [[10,11],[12,13],[14,15]]
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum(DatumArray{0, 1})});
        array->append_datum(DatumArray{Datum()});
        array->append_datum(Datum());
        array->append_datum(DatumArray{Datum(DatumArray{10, 11}), Datum(DatumArray{12, 13})});

        MutableColumnPtr data = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        data->append_datum(Datum(DatumArray{}));
        data->append_datum(DatumArray{2});
        data->append_datum(DatumArray{3, 4});
        data->append_datum(Datum());
        data->append_datum(DatumArray{14, 15});

        auto result = ArrayFunctions::array_append(nullptr, {array, data}).value();
        EXPECT_EQ(5, result->size());
        // 1st row.
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(0, row[0].get_array().size());
        // 2nd row
        row = result->get(1).get_array();
        EXPECT_EQ(2, row.size());

        EXPECT_EQ(2, row[0].get_array().size());
        EXPECT_EQ(1, row[1].get_array().size());

        EXPECT_EQ(0, row[0].get_array()[0].get_int32());
        EXPECT_EQ(1, row[0].get_array()[1].get_int32());
        EXPECT_EQ(2, row[1].get_array()[0].get_int32());
        // 3rd row
        row = result->get(2).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_TRUE(row[0].is_null());
        EXPECT_EQ(2, row[1].get_array().size());
        EXPECT_EQ(3, row[1].get_array()[0].get_int32());
        EXPECT_EQ(4, row[1].get_array()[1].get_int32());
        // 4th row
        EXPECT_TRUE(result->get(3).is_null());
        // 5th row
        row = result->get(4).get_array();
        EXPECT_EQ(3, row.size());

        EXPECT_EQ(2, row[0].get_array().size());
        EXPECT_EQ(2, row[1].get_array().size());
        EXPECT_EQ(2, row[2].get_array().size());

        EXPECT_EQ(10, row[0].get_array()[0].get_int32());
        EXPECT_EQ(11, row[0].get_array()[1].get_int32());
        EXPECT_EQ(12, row[1].get_array()[0].get_int32());
        EXPECT_EQ(13, row[1].get_array()[1].get_int32());
        EXPECT_EQ(14, row[2].get_array()[0].get_int32());
        EXPECT_EQ(15, row[2].get_array()[1].get_int32());
    }

    // array_append(NULL,1)
    {
        auto array = ColumnHelper::create_const_null_column(3);
        MutableColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_append(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
}

TEST_F(ArrayFunctionsTest, array_sum_empty_array) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        result = ArrayFunctions::array_sum<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_avg_empty_array) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        result = ArrayFunctions::array_avg<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_min_empty_array) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        result = ArrayFunctions::array_min<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_max_empty_array) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        result = ArrayFunctions::array_max<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_sum_no_null) {
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

        auto result = ArrayFunctions::array_sum<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_EQ(0, result->get(2).get_int64());
        EXPECT_EQ(0, result->get(3).get_int64());
        EXPECT_EQ(1, result->get(4).get_int64());
        EXPECT_EQ(1, result->get(5).get_int64());
        EXPECT_EQ(1, result->get(6).get_int64());
        EXPECT_EQ(1, result->get(7).get_int64());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});
        array->append_datum(DatumArray{1, 2, 3, Datum()});

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_int64());
        EXPECT_EQ(6, result->get(2).get_int64());
        EXPECT_EQ(6, result->get(3).get_int64());
        EXPECT_EQ(6, result->get(4).get_int64());
        EXPECT_EQ(6, result->get(5).get_int64());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t)127, (int8_t)100, (int8_t)-1});
        array->append_datum(DatumArray{(int8_t)-128, (int8_t)-1, (int8_t)10});

        auto result = ArrayFunctions::array_sum<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(226, result->get(1).get_int64());
        EXPECT_EQ(-119, result->get(2).get_int64());
    }
}

TEST_F(ArrayFunctionsTest, array_avg_no_null) {
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

        auto result = ArrayFunctions::array_avg<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_EQ(0, result->get(2).get_double());
        EXPECT_EQ(0, result->get(3).get_double());
        EXPECT_EQ(1, result->get(4).get_double());
        EXPECT_EQ(1, result->get(5).get_double());
        EXPECT_EQ(0.5, result->get(6).get_double());
        EXPECT_EQ(0.5, result->get(7).get_double());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});
        array->append_datum(DatumArray{1, 2, 3, Datum()});

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_double());
        EXPECT_EQ(2, result->get(2).get_double());
        EXPECT_EQ(2, result->get(3).get_double());
        EXPECT_EQ(2, result->get(4).get_double());
        EXPECT_EQ(1.5, result->get(5).get_double());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) true, Datum(), Datum(), (int8_t) false});
        array->append_datum(DatumArray{(int8_t) false, Datum()});
        array->append_datum(DatumArray{(int8_t) true, Datum()});

        auto result = ArrayFunctions::array_avg<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(0.25, result->get(1).get_double());
        EXPECT_EQ(0, result->get(2).get_double());
        EXPECT_EQ(0.5, result->get(3).get_double());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t)-128, (int8_t)127, (int8_t)0, Datum()});
        array->append_datum(DatumArray{(int8_t)127, (int8_t)10, (int8_t)100});

        auto result = ArrayFunctions::array_avg<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(-0.25, result->get(1).get_double());
        EXPECT_EQ(79, result->get(2).get_double());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_SMALLINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int16_t)30000, (int16_t)30000, Datum()});
        array->append_datum(DatumArray{(int16_t)-32768, (int16_t)32767, Datum(), (int16_t)0, (int16_t)1});

        auto result = ArrayFunctions::array_avg<TYPE_SMALLINT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());

        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(20000, result->get(1).get_double());
        EXPECT_EQ(0, result->get(2).get_double());
    }
}

TEST_F(ArrayFunctionsTest, array_min_no_null) {
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

        auto result = ArrayFunctions::array_min<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
        EXPECT_EQ(1, result->get(5).get_int8());
        EXPECT_EQ(0, result->get(6).get_int8());
        EXPECT_EQ(0, result->get(7).get_int8());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_int32());
        EXPECT_EQ(1, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(1, result->get(4).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_max_no_null) {
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

        auto result = ArrayFunctions::array_max<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
        EXPECT_EQ(1, result->get(5).get_int8());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(1, result->get(7).get_int8());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_int32());
        EXPECT_EQ(3, result->get(2).get_int32());
        EXPECT_EQ(3, result->get(3).get_int32());
        EXPECT_EQ(3, result->get(4).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_sum_has_null_element) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)200000000, (int64_t)121, (int64_t)300});
        array->append_datum(DatumArray{(int64_t)33, Datum(), (int64_t)300});

        auto result = ArrayFunctions::array_sum<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int64());
        EXPECT_EQ(1005, result->get(2).get_int64());
        EXPECT_EQ(6, result->get(3).get_int64());
        EXPECT_EQ(200000421, result->get(4).get_int64());
        EXPECT_EQ(333, result->get(5).get_int64());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)200000000, (int128_t)121, (int128_t)300});
        array->append_datum(DatumArray{(int128_t)33, Datum(), (int128_t)300});

        auto result = ArrayFunctions::array_sum<TYPE_LARGEINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int128());
        EXPECT_EQ(1005, result->get(2).get_int128());
        EXPECT_EQ(6, result->get(3).get_int128());
        EXPECT_EQ(200000421, result->get(4).get_int128());
        EXPECT_EQ(333, result->get(5).get_int128());
    }
}

TEST_F(ArrayFunctionsTest, array_avg_has_null_element) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, (int64_t)1, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)2, Datum(), (int64_t)1});

        auto result = ArrayFunctions::array_avg<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_double());
        EXPECT_EQ(335, result->get(2).get_double());
        EXPECT_EQ(2, result->get(3).get_double());
        EXPECT_EQ(1, result->get(4).get_double());
        EXPECT_EQ(1, result->get(5).get_double());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, (int128_t)1, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)2, Datum(), (int128_t)1});

        auto result = ArrayFunctions::array_avg<TYPE_LARGEINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_double());
        EXPECT_EQ(335, result->get(2).get_double());
        EXPECT_EQ(2, result->get(3).get_double());
        EXPECT_EQ(1, result->get(4).get_double());
        EXPECT_EQ(1, result->get(5).get_double());
    }
}

TEST_F(ArrayFunctionsTest, array_min_has_null_element) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, (int64_t)1, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, Datum(), (int64_t)1});

        auto result = ArrayFunctions::array_min<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int64());
        EXPECT_EQ(2, result->get(2).get_int64());
        EXPECT_EQ(1, result->get(3).get_int64());
        EXPECT_EQ(1, result->get(4).get_int64());
        EXPECT_EQ(1, result->get(5).get_int64());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, (int128_t)1, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, Datum(), (int128_t)1});

        auto result = ArrayFunctions::array_min<TYPE_LARGEINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int128());
        EXPECT_EQ(2, result->get(2).get_int128());
        EXPECT_EQ(1, result->get(3).get_int128());
        EXPECT_EQ(1, result->get(4).get_int128());
        EXPECT_EQ(1, result->get(5).get_int128());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_DATE, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 24)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 26)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 28)});
        array->append_datum(DatumArray{Datum(), DateValue::create(1990, 3, 28)});

        auto result = ArrayFunctions::array_min<TYPE_DATE>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(1).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(2).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(3).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(4).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 28), result->get(5).get_date());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_DATETIME, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 34)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 36)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 38)});
        array->append_datum(DatumArray{Datum(), TimestampValue::create(1990, 3, 22, 5, 32, 38)});

        auto result = ArrayFunctions::array_min<TYPE_DATETIME>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(1).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(2).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(3).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(4).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 38), result->get(5).get_timestamp());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{"varchar"});
        array->append_datum(DatumArray{"varchar1", "varchar2"});
        array->append_datum(DatumArray{"varchar1", "varchar3"});
        array->append_datum(DatumArray{"varchar1", "varchar4"});
        array->append_datum(DatumArray{Datum(), "varchar4"});

        auto result = ArrayFunctions::array_min<TYPE_VARCHAR>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ("varchar", result->get(1).get_slice());
        EXPECT_EQ("varchar1", result->get(2).get_slice());
        EXPECT_EQ("varchar1", result->get(3).get_slice());
        EXPECT_EQ("varchar1", result->get(4).get_slice());
        EXPECT_EQ("varchar4", result->get(5).get_slice());
    }
}

TEST_F(ArrayFunctionsTest, array_max_has_null_element) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, (int64_t)1, (int64_t)1});

        auto result = ArrayFunctions::array_max<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int64());
        EXPECT_EQ(1000, result->get(2).get_int64());
        EXPECT_EQ(3, result->get(3).get_int64());
        EXPECT_EQ(1, result->get(4).get_int64());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, (int128_t)1, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)2, (int128_t)1, Datum()});

        auto result = ArrayFunctions::array_max<TYPE_LARGEINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int128());
        EXPECT_EQ(1000, result->get(2).get_int128());
        EXPECT_EQ(3, result->get(3).get_int128());
        EXPECT_EQ(1, result->get(4).get_int128());
        EXPECT_EQ(2, result->get(5).get_int128());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_DATE, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 24)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 26)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 28)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), Datum()});

        auto result = ArrayFunctions::array_max<TYPE_DATE>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(1).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 24), result->get(2).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 26), result->get(3).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 28), result->get(4).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(5).get_date());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_DATETIME, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 34)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 36)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 38)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32), Datum()});

        auto result = ArrayFunctions::array_max<TYPE_DATETIME>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(1).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 34), result->get(2).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 36), result->get(3).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 38), result->get(4).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(5).get_timestamp());
    }

    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{"varchar"});
        array->append_datum(DatumArray{"varchar1", "varchar2"});
        array->append_datum(DatumArray{"varchar1", "varchar3"});
        array->append_datum(DatumArray{"varchar1", "varchar4"});
        array->append_datum(DatumArray{"varchar1", Datum()});

        auto result = ArrayFunctions::array_max<TYPE_VARCHAR>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ("varchar", result->get(1).get_slice());
        EXPECT_EQ("varchar2", result->get(2).get_slice());
        EXPECT_EQ("varchar3", result->get(3).get_slice());
        EXPECT_EQ("varchar4", result->get(4).get_slice());
        EXPECT_EQ("varchar1", result->get(5).get_slice());
    }
}

TEST_F(ArrayFunctionsTest, array_sum_nullable_array) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 30});

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(8, result->get(0).get_int64());
        EXPECT_EQ(54, result->get(1).get_int64());
        EXPECT_EQ(5503, result->get(2).get_int64());
    }
}

TEST_F(ArrayFunctionsTest, array_avg_nullable_array) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 32});

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(4, result->get(0).get_double());
        EXPECT_EQ(27, result->get(1).get_double());
        EXPECT_EQ(1835, result->get(2).get_double());
    }
}

TEST_F(ArrayFunctionsTest, array_min_nullable_array) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 32});

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(3, result->get(0).get_int32());
        EXPECT_EQ(54, result->get(1).get_int32());
        EXPECT_EQ(32, result->get(2).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_max_nullable_array) {
    {
        MutableColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 32});

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(5, result->get(0).get_int32());
        EXPECT_EQ(54, result->get(1).get_int32());
        EXPECT_EQ(5352, result->get(2).get_int32());
    }
}

} // namespace starrocks
