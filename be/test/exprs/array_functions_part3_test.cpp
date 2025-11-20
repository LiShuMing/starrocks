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

TEST_F(ArrayFunctionsTest, array_all_null) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_sum<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_avg<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_min<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_min<TYPE_VARCHAR>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_max<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_max<TYPE_VARCHAR>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_reverse_int) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, 3, 6});
    src_column->append_datum(DatumArray{2, 3, 7, 8});
    src_column->append_datum(DatumArray{4, 3, 2, 1});

    ArrayReverse<LogicalType::TYPE_INT> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<int32_t>({6, 3, 5}, dest_column->get(0).get_array());
    _check_array<int32_t>({8, 7, 3, 2}, dest_column->get(1).get_array());
    _check_array<int32_t>({1, 2, 3, 4}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_string) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"352", "66", "4325"});
    src_column->append_datum(DatumArray{"235", "99", "8", "43251"});
    src_column->append_datum(DatumArray{"44", "33", "22", "112"});

    ArrayReverse<LogicalType::TYPE_VARCHAR> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<Slice>({"4325", "66", "352"}, dest_column->get(0).get_array());
    _check_array<Slice>({"43251", "8", "99", "235"}, dest_column->get(1).get_array());
    _check_array<Slice>({"112", "22", "33", "44"}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_nullable_elements) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 3, 6});
    src_column->append_datum(DatumArray{2, 3, Datum(), Datum()});
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    ArrayReverse<LogicalType::TYPE_INT> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array_nullable<int32_t>({6, 3, 0, 5}, {0, 0, 1, 0}, dest_column->get(0).get_array());
    _check_array_nullable<int32_t>({0, 0, 3, 2}, {1, 1, 0, 0}, dest_column->get(1).get_array());
    _check_array_nullable<int32_t>({0, 0, 0, 0}, {1, 1, 1, 1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_nullable_array) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 3, 6});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    ArrayReverse<LogicalType::TYPE_INT> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array_nullable<int32_t>({6, 3, 0, 5}, {0, 0, 1, 0}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).is_null());
    _check_array_nullable<int32_t>({0, 0, 0, 0}, {1, 1, 1, 1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_only_null) {
    auto src_column = ColumnHelper::create_const_null_column(3);

    ArrayReverse<LogicalType::TYPE_INT> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());
}

TEST_F(ArrayFunctionsTest, array_difference_boolean) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column->append_datum(DatumArray{(uint8_t)5, (uint8_t)3, (uint8_t)6});
    src_column->append_datum(DatumArray{(uint8_t)2, (uint8_t)3, (uint8_t)7, (uint8_t)8});
    src_column->append_datum(DatumArray{(uint8_t)4, (uint8_t)3, (uint8_t)2, (uint8_t)1});

    ArrayDifference<LogicalType::TYPE_BOOLEAN> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<int64_t>({0, -2, 3}, dest_column->get(0).get_array());
    _check_array<int64_t>({0, 1, 4, 1}, dest_column->get(1).get_array());
    _check_array<int64_t>({0, -1, -1, -1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_difference_boolean_with_entry_null) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column->append_datum(DatumArray{(uint8_t)5, Datum(), (uint8_t)6});
    src_column->append_datum(DatumArray{Datum(), (uint8_t)3, (uint8_t)7, (uint8_t)8});
    src_column->append_datum(DatumArray{(uint8_t)4, (uint8_t)3, (uint8_t)2, Datum()});
    src_column->append_datum(Datum());

    ArrayDifference<LogicalType::TYPE_BOOLEAN> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 4);

    ASSERT_EQ(0, dest_column->get(0).get_array()[0].get_int64());
    ASSERT_TRUE(dest_column->get(0).get_array()[1].is_null());
    ASSERT_TRUE(dest_column->get(0).get_array()[2].is_null());

    ASSERT_TRUE(dest_column->get(1).get_array()[0].is_null());
    ASSERT_TRUE(dest_column->get(1).get_array()[1].is_null());
    ASSERT_EQ(4, dest_column->get(1).get_array()[2].get_int64());
    ASSERT_EQ(1, dest_column->get(1).get_array()[3].get_int64());

    ASSERT_EQ(0, dest_column->get(2).get_array()[0].get_int64());
    ASSERT_EQ(-1, dest_column->get(2).get_array()[1].get_int64());
    ASSERT_EQ(-1, dest_column->get(2).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(2).get_array()[3].is_null());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_difference_int) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, 3, 6});
    src_column->append_datum(DatumArray{2, 3, 7, 8});
    src_column->append_datum(DatumArray{4, 3, 2, 1});

    ArrayDifference<LogicalType::TYPE_INT> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<int64_t>({0, -2, 3}, dest_column->get(0).get_array());
    _check_array<int64_t>({0, 1, 4, 1}, dest_column->get(1).get_array());
    _check_array<int64_t>({0, -1, -1, -1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_difference_int_with_entry_null) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 6});
    src_column->append_datum(DatumArray{Datum(), 3, 7, 8});
    src_column->append_datum(DatumArray{4, 3, 2, Datum()});
    src_column->append_datum(Datum());

    ArrayDifference<LogicalType::TYPE_INT> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 4);

    ASSERT_EQ(0, dest_column->get(0).get_array()[0].get_int64());
    ASSERT_TRUE(dest_column->get(0).get_array()[1].is_null());
    ASSERT_TRUE(dest_column->get(0).get_array()[2].is_null());

    ASSERT_TRUE(dest_column->get(1).get_array()[0].is_null());
    ASSERT_TRUE(dest_column->get(1).get_array()[1].is_null());
    ASSERT_EQ(4, dest_column->get(1).get_array()[2].get_int64());
    ASSERT_EQ(1, dest_column->get(1).get_array()[3].get_int64());

    ASSERT_EQ(0, dest_column->get(2).get_array()[0].get_int64());
    ASSERT_EQ(-1, dest_column->get(2).get_array()[1].get_int64());
    ASSERT_EQ(-1, dest_column->get(2).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(2).get_array()[3].is_null());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_difference_bigint) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)2, (int64_t)3, (int64_t)7, (int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});

    ArrayDifference<LogicalType::TYPE_BIGINT> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<int64_t>({(int64_t)0, (int64_t)-2, (int64_t)3}, dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)0, (int64_t)1, (int64_t)4, (int64_t)1}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)0, (int64_t)-1, (int64_t)-1, (int64_t)-1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_difference_bigint_with_entry_null) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, Datum(), (int64_t)6});
    src_column->append_datum(DatumArray{Datum(), (int64_t)3, (int64_t)7, (int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, Datum()});
    src_column->append_datum(Datum());

    ArrayDifference<LogicalType::TYPE_BIGINT> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 4);

    ASSERT_EQ((int64_t)0, dest_column->get(0).get_array()[0].get_int64());
    ASSERT_TRUE(dest_column->get(0).get_array()[1].is_null());
    ASSERT_TRUE(dest_column->get(0).get_array()[2].is_null());

    ASSERT_TRUE(dest_column->get(1).get_array()[0].is_null());
    ASSERT_TRUE(dest_column->get(1).get_array()[1].is_null());
    ASSERT_EQ((int64_t)4, dest_column->get(1).get_array()[2].get_int64());
    ASSERT_EQ((int64_t)1, dest_column->get(1).get_array()[3].get_int64());

    ASSERT_EQ((int64_t)0, dest_column->get(2).get_array()[0].get_int64());
    ASSERT_EQ((int64_t)-1, dest_column->get(2).get_array()[1].get_int64());
    ASSERT_EQ((int64_t)-1, dest_column->get(2).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(2).get_array()[3].is_null());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_difference_double) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)2, (double)3, (double)7, (double)8});
    src_column->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});

    ArrayDifference<LogicalType::TYPE_DOUBLE> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<double>({(double)0, (double)-2, (double)3}, dest_column->get(0).get_array());
    _check_array<double>({(double)0, (double)1, (double)4, (double)1}, dest_column->get(1).get_array());
    _check_array<double>({(double)0, (double)-1, (double)-1, (double)-1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_slice_int) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{(int32_t)5, (int32_t)3, (int32_t)6});
    src_column->append_datum(DatumArray{(int32_t)2, (int32_t)3, (int32_t)7, (int32_t)8});
    src_column->append_datum(DatumArray{(int32_t)4, (int32_t)3, (int32_t)2, (int32_t)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int32_t)4, Datum(), (int32_t)2, (int32_t)1});
    src_column->append_datum(DatumArray{(int32_t)1, (int32_t)2, Datum(), (int32_t)4, (int32_t)5});
    src_column->append_datum(DatumArray{(int32_t)1, (int32_t)2, Datum(), (int32_t)4, (int32_t)5});
    src_column->append_datum(DatumArray{(int32_t)1, (int32_t)2, Datum(), (int32_t)4, (int32_t)5});

    auto offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(-2);
    offset_column->append(-7);
    offset_column->append(-8);

    auto length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);
    length_column->append(3);
    length_column->append(3);
    length_column->append(3);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 8);
    _check_array<int32_t>({(int32_t)5}, dest_column->get(0).get_array());
    _check_array<int32_t>({(int32_t)3, (int32_t)7, (int32_t)8}, dest_column->get(1).get_array());
    _check_array<int32_t>({(int32_t)2, (int32_t)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_int32());
    _check_array<int32_t>({(int32_t)4, (int32_t)5}, dest_column->get(5).get_array());
    _check_array<int32_t>({(int32_t)1}, dest_column->get(6).get_array());
    _check_array<int32_t>({}, dest_column->get(7).get_array());
}

TEST_F(ArrayFunctionsTest, array_slice_bigint) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)2, (int64_t)3, (int64_t)7, (int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, Datum(), (int64_t)2, (int64_t)1});

    auto offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int64_t>({(int64_t)5}, dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)3, (int64_t)7, (int64_t)8}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)2, (int64_t)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_int64());
}

TEST_F(ArrayFunctionsTest, array_slice_float) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_FLOAT, true);
    src_column->append_datum(DatumArray{(float)5, (float)3, (float)6});
    src_column->append_datum(DatumArray{(float)2, (float)3, (float)7, (float)8});
    src_column->append_datum(DatumArray{(float)4, (float)3, (float)2, (float)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(float)4, Datum(), (float)2, (float)1});

    auto offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<float>({(float)5}, dest_column->get(0).get_array());
    _check_array<float>({(float)3, (float)7, (float)8}, dest_column->get(1).get_array());
    _check_array<float>({(float)2, (float)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_float());
}

TEST_F(ArrayFunctionsTest, array_slice_double) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)2, (double)3, (double)7, (double)8});
    src_column->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, Datum(), (double)2, (double)1});

    auto offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<double>({(double)5}, dest_column->get(0).get_array());
    _check_array<double>({(double)3, (double)7, (double)8}, dest_column->get(1).get_array());
    _check_array<double>({(double)2, (double)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_double());
}

TEST_F(ArrayFunctionsTest, array_slice_varchar) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("2"), Slice("3"), Slice("7"), Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("1")});

    auto offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<Slice>({Slice("5")}, dest_column->get(0).get_array());
    _check_array<Slice>({Slice("3"), Slice("7"), Slice("8")}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("2"), Slice("1")}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(Slice("2"), dest_column->get(4).get_array()[1].get_slice());
}

TEST_F(ArrayFunctionsTest, array_slice_bigint_only_offset) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)2, (int64_t)3, (int64_t)7, (int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, Datum(), (int64_t)2, (int64_t)1});

    auto offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int64_t>({(int64_t)5, (int64_t)3, (int64_t)6}, dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)3, (int64_t)7, (int64_t)8}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)2, (int64_t)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_int64());
    ASSERT_EQ(1, dest_column->get(4).get_array()[2].get_int64());
}

TEST_F(ArrayFunctionsTest, array_slice_double_only_offset) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)2, (double)3, (double)7, (double)8});
    src_column->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, Datum(), (double)2, (double)1});

    auto offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<double>({(double)5, (double)3, (double)6}, dest_column->get(0).get_array());
    _check_array<double>({(double)3, (double)7, (double)8}, dest_column->get(1).get_array());
    _check_array<double>({(double)2, (double)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_double());
    ASSERT_EQ(1, dest_column->get(4).get_array()[2].get_double());
}

TEST_F(ArrayFunctionsTest, array_slice_varchar_only_offset) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("2"), Slice("3"), Slice("7"), Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("1")});

    auto offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<Slice>({Slice("5"), Slice("3"), Slice("6")}, dest_column->get(0).get_array());
    _check_array<Slice>({Slice("3"), Slice("7"), Slice("8")}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("2"), Slice("1")}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(Slice("2"), dest_column->get(4).get_array()[1].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(4).get_array()[2].get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_tinyint) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column2->append_datum(DatumArray{(int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)2});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)9});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column3->append_datum(DatumArray{(int8_t)5});
    src_column3->append_datum(DatumArray{(int8_t)2, (int8_t)8});
    src_column3->append_datum(DatumArray{(int8_t)2, (int8_t)1});
    src_column3->append_datum(DatumArray{(int8_t)100});
    src_column3->append_datum(DatumArray{(int8_t)4, Datum(), (int8_t)2, (int8_t)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int8_t>({(int8_t)5, (int8_t)3, (int8_t)6, (int8_t)5, (int8_t)6, (int8_t)5},
                         dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)8, (int8_t)2, (int8_t)2, (int8_t)8}, dest_column->get(1).get_array());
    _check_array<int8_t>({(int8_t)4, (int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1, (int8_t)2, (int8_t)1},
                         dest_column->get(2).get_array());

    ASSERT_TRUE(dest_column->get(3).is_null());

    ASSERT_EQ(4, dest_column->get(4).get_array()[0].get_int8());
    ASSERT_EQ(1, dest_column->get(4).get_array()[1].get_int8());
    ASSERT_EQ(4, dest_column->get(4).get_array()[2].get_int8());
    ASSERT_TRUE(dest_column->get(4).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(4).get_array()[4].get_int8());
    ASSERT_TRUE(dest_column->get(4).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[6].get_int8());
    ASSERT_EQ(1, dest_column->get(4).get_array()[7].get_int8());
}

TEST_F(ArrayFunctionsTest, array_concat_tinyint_not_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)2});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column3->append_datum(DatumArray{(int8_t)5});
    src_column3->append_datum(DatumArray{(int8_t)2, (int8_t)8});
    src_column3->append_datum(DatumArray{(int8_t)2, (int8_t)1});
    src_column3->append_datum(DatumArray{(int8_t)4, Datum(), (int8_t)2, (int8_t)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int8_t>({(int8_t)5, (int8_t)3, (int8_t)6, (int8_t)5, (int8_t)6, (int8_t)5},
                         dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)8, (int8_t)2, (int8_t)2, (int8_t)8}, dest_column->get(1).get_array());
    _check_array<int8_t>({(int8_t)4, (int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1, (int8_t)2, (int8_t)1},
                         dest_column->get(2).get_array());

    ASSERT_EQ(4, dest_column->get(3).get_array()[0].get_int8());
    ASSERT_EQ(1, dest_column->get(3).get_array()[1].get_int8());
    ASSERT_EQ(4, dest_column->get(3).get_array()[2].get_int8());
    ASSERT_TRUE(dest_column->get(3).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(3).get_array()[4].get_int8());
    ASSERT_TRUE(dest_column->get(3).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(3).get_array()[6].get_int8());
    ASSERT_EQ(1, dest_column->get(3).get_array()[7].get_int8());
}

TEST_F(ArrayFunctionsTest, array_concat_bigint) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column2->append_datum(DatumArray{(int64_t)5, (int64_t)6});
    src_column2->append_datum(DatumArray{(int64_t)2});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)9});
    src_column2->append_datum(DatumArray{(int64_t)4, Datum()});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column3->append_datum(DatumArray{(int64_t)5});
    src_column3->append_datum(DatumArray{(int64_t)2, (int64_t)8});
    src_column3->append_datum(DatumArray{(int64_t)2, (int64_t)1});
    src_column3->append_datum(DatumArray{(int64_t)100});
    src_column3->append_datum(DatumArray{(int64_t)4, Datum(), (int64_t)2, (int64_t)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int64_t>({(int64_t)5, (int64_t)3, (int64_t)6, (int64_t)5, (int64_t)6, (int64_t)5},
                          dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)8, (int64_t)2, (int64_t)2, (int64_t)8}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)4, (int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1, (int64_t)2, (int64_t)1},
                          dest_column->get(2).get_array());

    ASSERT_TRUE(dest_column->get(3).is_null());

    ASSERT_EQ(4, dest_column->get(4).get_array()[0].get_int64());
    ASSERT_EQ(1, dest_column->get(4).get_array()[1].get_int64());
    ASSERT_EQ(4, dest_column->get(4).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(4).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(4).get_array()[4].get_int64());
    ASSERT_TRUE(dest_column->get(4).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[6].get_int64());
    ASSERT_EQ(1, dest_column->get(4).get_array()[7].get_int64());
}

TEST_F(ArrayFunctionsTest, array_concat_bigint_not_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column2->append_datum(DatumArray{(int64_t)5, (int64_t)6});
    src_column2->append_datum(DatumArray{(int64_t)2});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column2->append_datum(DatumArray{(int64_t)4, Datum()});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column3->append_datum(DatumArray{(int64_t)5});
    src_column3->append_datum(DatumArray{(int64_t)2, (int64_t)8});
    src_column3->append_datum(DatumArray{(int64_t)2, (int64_t)1});
    src_column3->append_datum(DatumArray{(int64_t)4, Datum(), (int64_t)2, (int64_t)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int64_t>({(int64_t)5, (int64_t)3, (int64_t)6, (int64_t)5, (int64_t)6, (int64_t)5},
                          dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)8, (int64_t)2, (int64_t)2, (int64_t)8}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)4, (int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1, (int64_t)2, (int64_t)1},
                          dest_column->get(2).get_array());

    ASSERT_EQ(4, dest_column->get(3).get_array()[0].get_int64());
    ASSERT_EQ(1, dest_column->get(3).get_array()[1].get_int64());
    ASSERT_EQ(4, dest_column->get(3).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(3).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(3).get_array()[4].get_int64());
    ASSERT_TRUE(dest_column->get(3).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(3).get_array()[6].get_int64());
    ASSERT_EQ(1, dest_column->get(3).get_array()[7].get_int64());
}

TEST_F(ArrayFunctionsTest, array_concat_double) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, (double)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column2->append_datum(DatumArray{(double)5, (double)6});
    src_column2->append_datum(DatumArray{(double)2});
    src_column2->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column2->append_datum(DatumArray{(double)4, (double)9});
    src_column2->append_datum(DatumArray{(double)4, Datum()});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column3->append_datum(DatumArray{(double)5});
    src_column3->append_datum(DatumArray{(double)2, (double)8});
    src_column3->append_datum(DatumArray{(double)2, (double)1});
    src_column3->append_datum(DatumArray{(double)100});
    src_column3->append_datum(DatumArray{(double)4, Datum(), (double)2, (double)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<double>({(double)5, (double)3, (double)6, (double)5, (double)6, (double)5},
                         dest_column->get(0).get_array());
    _check_array<double>({(double)8, (double)2, (double)2, (double)8}, dest_column->get(1).get_array());
    _check_array<double>({(double)4, (double)4, (double)3, (double)2, (double)1, (double)2, (double)1},
                         dest_column->get(2).get_array());

    ASSERT_TRUE(dest_column->get(3).is_null());

    ASSERT_EQ(4, dest_column->get(4).get_array()[0].get_double());
    ASSERT_EQ(1, dest_column->get(4).get_array()[1].get_double());
    ASSERT_EQ(4, dest_column->get(4).get_array()[2].get_double());
    ASSERT_TRUE(dest_column->get(4).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(4).get_array()[4].get_double());
    ASSERT_TRUE(dest_column->get(4).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[6].get_double());
    ASSERT_EQ(1, dest_column->get(4).get_array()[7].get_double());
}

TEST_F(ArrayFunctionsTest, array_concat_double_not_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(DatumArray{(double)4, (double)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column2->append_datum(DatumArray{(double)5, (double)6});
    src_column2->append_datum(DatumArray{(double)2});
    src_column2->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column2->append_datum(DatumArray{(double)4, Datum()});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column3->append_datum(DatumArray{(double)5});
    src_column3->append_datum(DatumArray{(double)2, (double)8});
    src_column3->append_datum(DatumArray{(double)2, (double)1});
    src_column3->append_datum(DatumArray{(double)4, Datum(), (double)2, (double)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<double>({(double)5, (double)3, (double)6, (double)5, (double)6, (double)5},
                         dest_column->get(0).get_array());
    _check_array<double>({(double)8, (double)2, (double)2, (double)8}, dest_column->get(1).get_array());
    _check_array<double>({(double)4, (double)4, (double)3, (double)2, (double)1, (double)2, (double)1},
                         dest_column->get(2).get_array());

    ASSERT_EQ(4, dest_column->get(3).get_array()[0].get_double());
    ASSERT_EQ(1, dest_column->get(3).get_array()[1].get_double());
    ASSERT_EQ(4, dest_column->get(3).get_array()[2].get_double());
    ASSERT_TRUE(dest_column->get(3).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(3).get_array()[4].get_double());
    ASSERT_TRUE(dest_column->get(3).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(3).get_array()[6].get_double());
    ASSERT_EQ(1, dest_column->get(3).get_array()[7].get_double());
}

TEST_F(ArrayFunctionsTest, array_concat_varchar) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("9")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum()});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("100")});
    src_column3->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("1")});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<Slice>({Slice("5"), Slice("3"), Slice("6"), Slice("5"), Slice("6"), Slice("5")},
                        dest_column->get(0).get_array());
    _check_array<Slice>({Slice("8"), Slice("2"), Slice("2"), Slice("8")}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("4"), Slice("4"), Slice("3"), Slice("2"), Slice("1"), Slice("2"), Slice("1")},
                        dest_column->get(2).get_array());

    ASSERT_TRUE(dest_column->get(3).is_null());

    ASSERT_EQ(Slice("4"), dest_column->get(4).get_array()[0].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(4).get_array()[1].get_slice());
    ASSERT_EQ(Slice("4"), dest_column->get(4).get_array()[2].get_slice());
    ASSERT_TRUE(dest_column->get(4).get_array()[3].is_null());
    ASSERT_EQ(Slice("4"), dest_column->get(4).get_array()[4].get_slice());
    ASSERT_TRUE(dest_column->get(4).get_array()[5].is_null());
    ASSERT_EQ(Slice("2"), dest_column->get(4).get_array()[6].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(4).get_array()[7].get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_varchar_not_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum()});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("1")});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<Slice>({Slice("5"), Slice("3"), Slice("6"), Slice("5"), Slice("6"), Slice("5")},
                        dest_column->get(0).get_array());
    _check_array<Slice>({Slice("8"), Slice("2"), Slice("2"), Slice("8")}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("4"), Slice("4"), Slice("3"), Slice("2"), Slice("1"), Slice("2"), Slice("1")},
                        dest_column->get(2).get_array());

    ASSERT_EQ(Slice("4"), dest_column->get(3).get_array()[0].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(3).get_array()[1].get_slice());
    ASSERT_EQ(Slice("4"), dest_column->get(3).get_array()[2].get_slice());
    ASSERT_TRUE(dest_column->get(3).get_array()[3].is_null());
    ASSERT_EQ(Slice("4"), dest_column->get(3).get_array()[4].get_slice());
    ASSERT_TRUE(dest_column->get(3).get_array()[5].is_null());
    ASSERT_EQ(Slice("2"), dest_column->get(3).get_array()[6].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(3).get_array()[7].get_slice());
}

TEST_F(ArrayFunctionsTest, array_overlap_tinyint_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)2});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)9});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_TINYINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(
            ColumnHelper::as_raw_column<NullableColumn>(dest_column)->data_column());
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data().data();

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_TRUE(null_data[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_tinyint) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(DatumArray{(int8_t)99});
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)2});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)9});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_TINYINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(dest_column);

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_FALSE(v->get_data()[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_bigint_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column2->append_datum(DatumArray{(int64_t)5, (int64_t)6});
    src_column2->append_datum(DatumArray{(int64_t)2});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)9});
    src_column2->append_datum(DatumArray{(int64_t)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_BIGINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(
            ColumnHelper::as_raw_column<NullableColumn>(dest_column)->data_column());
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data().data();

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_TRUE(null_data[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_bigint) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(DatumArray{(int64_t)99});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column2->append_datum(DatumArray{(int64_t)5, (int64_t)6});
    src_column2->append_datum(DatumArray{(int64_t)2});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)9});
    src_column2->append_datum(DatumArray{(int64_t)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_BIGINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(dest_column);

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_FALSE(v->get_data()[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_double_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, (double)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column2->append_datum(DatumArray{(double)5, (double)6});
    src_column2->append_datum(DatumArray{(double)2});
    src_column2->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column2->append_datum(DatumArray{(double)4, (double)9});
    src_column2->append_datum(DatumArray{(double)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_DOUBLE> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(
            ColumnHelper::as_raw_column<NullableColumn>(dest_column)->data_column());
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data().data();

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_TRUE(null_data[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_double) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(DatumArray{(double)99});
    src_column->append_datum(DatumArray{(double)4, (double)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column2->append_datum(DatumArray{(double)5, (double)6});
    src_column2->append_datum(DatumArray{(double)2});
    src_column2->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column2->append_datum(DatumArray{(double)4, (double)9});
    src_column2->append_datum(DatumArray{(double)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_DOUBLE> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(dest_column);

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_FALSE(v->get_data()[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_varchar_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("9")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum()});

    ArrayOverlap<LogicalType::TYPE_VARCHAR> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(
            ColumnHelper::as_raw_column<NullableColumn>(dest_column)->data_column());
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data().data();

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_TRUE(null_data[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_varchar) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(DatumArray{Slice("99")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("9")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum()});

    ArrayOverlap<LogicalType::TYPE_VARCHAR> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(dest_column);

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_FALSE(v->get_data()[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_with_onlynull) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});

    auto src_column2 = ColumnHelper::create_const_null_column(1);

    ArrayOverlap<LogicalType::TYPE_TINYINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2});
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->get()->only_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_int) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{(int32_t)5, (int32_t)3, (int32_t)6});
    src_column->append_datum(DatumArray{(int32_t)8});
    src_column->append_datum(DatumArray{(int32_t)4, (int32_t)1});
    src_column->append_datum(Datum());

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column2->append_datum(DatumArray{(int32_t)(5), (int32_t)(6)});
    src_column2->append_datum(DatumArray{(int32_t)(2)});
    src_column2->append_datum(DatumArray{(int32_t)(4), (int32_t)(3), (int32_t)(2), (int32_t)(1)});
    src_column2->append_datum(DatumArray{(int32_t)(4), (int32_t)(9)});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column3->append_datum(DatumArray{(int32_t)(5)});
    src_column3->append_datum(DatumArray{(int32_t)(2), (int32_t)(8)});
    src_column3->append_datum(DatumArray{(int32_t)(4), (int32_t)(1)});
    src_column3->append_datum(DatumArray{(int32_t)(100)});

    ArrayIntersect<LogicalType::TYPE_INT> intersect;
    auto dest_column = intersect.process(nullptr, {src_column, src_column2, src_column3});

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int32_t>({(int32_t)(5)}, dest_column->get(0).get_array());
    _check_array<int32_t>({}, dest_column->get(1).get_array());
    _check_array<int32_t>({(int32_t)(4), (int32_t)(1)}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_int_with_not_null) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
    src_column->append_datum(DatumArray{(int32_t)(5), (int32_t)(3), (int32_t)(6)});
    src_column->append_datum(DatumArray{(int32_t)(8)});
    src_column->append_datum(DatumArray{(int32_t)(4), (int32_t)(1)});
    src_column->append_datum(DatumArray{(int32_t)(4), (int32_t)(22), Datum()});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
    src_column2->append_datum(DatumArray{(int32_t)(5), (int32_t)(6)});
    src_column2->append_datum(DatumArray{(int32_t)(2)});
    src_column2->append_datum(DatumArray{(int32_t)(4), (int32_t)(3), (int32_t)(2), (int32_t)(1)});
    src_column2->append_datum(DatumArray{(int32_t)(4), Datum(), (int32_t)(22), (int32_t)(66)});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
    src_column3->append_datum(DatumArray{(int32_t)(5)});
    src_column3->append_datum(DatumArray{(int32_t)(2), (int32_t)(8)});
    src_column3->append_datum(DatumArray{(int32_t)(4), (int32_t)(1)});
    src_column3->append_datum(DatumArray{(int32_t)(4), Datum(), (int32_t)(2), (int32_t)(22), (int32_t)(1)});

    ArrayIntersect<LogicalType::TYPE_INT> intersect;
    auto dest_column = intersect.process(nullptr, {src_column, src_column2, src_column3});

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int32_t>({(int32_t)(5)}, dest_column->get(0).get_array());
    _check_array<int32_t>({}, dest_column->get(1).get_array());

    {
        std::unordered_set<int32_t> set_expect = {4, 1};
        std::unordered_set<int32_t> set_actual;
        auto result_array = dest_column->get(2).get_array();
        for (auto& i : result_array) {
            set_actual.insert(i.get_int32());
        }
        ASSERT_TRUE(set_expect == set_actual);
    }

    {
        std::unordered_set<int32_t> set_expect = {4, 22};
        std::unordered_set<int32_t> set_actual;
        size_t null_values = 0;
        auto result_array = dest_column->get(3).get_array();
        for (auto& i : result_array) {
            if (i.is_null()) {
                ++null_values;
            } else {
                set_actual.insert(i.get_int32());
            }
        }
        ASSERT_TRUE(set_expect == set_actual);
        ASSERT_EQ(null_values, 1);
    }
}

TEST_F(ArrayFunctionsTest, array_intersect_varchar) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column->append_datum(Datum());

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("9")});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("100")});

    ArrayIntersect<LogicalType::TYPE_VARCHAR> intersect;
    auto dest_column = intersect.process(nullptr, {src_column, src_column2, src_column3});

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<Slice>({Slice("5")}, dest_column->get(0).get_array());
    _check_array<Slice>({}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("4"), Slice("1")}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_varchar_with_not_null) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("22"), Datum()});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum(), Slice("22"), Slice("66")});

    auto src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("22"), Slice("1")});

    ArrayIntersect<LogicalType::TYPE_VARCHAR> intersect;
    auto dest_column = intersect.process(nullptr, {src_column, src_column2, src_column3});

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<Slice>({Slice("5")}, dest_column->get(0).get_array());
    _check_array<Slice>({}, dest_column->get(1).get_array());

    {
        std::unordered_set<std::string> set_expect = {"4", "1"};
        std::unordered_set<std::string> set_actual;
        auto result_array = dest_column->get(2).get_array();
        for (auto& i : result_array) {
            set_actual.insert(i.get_slice().to_string());
        }
        ASSERT_TRUE(set_expect == set_actual);
    }

    {
        std::unordered_set<std::string> set_expect = {"4", "22"};
        std::unordered_set<std::string> set_actual;
        size_t null_values = 0;
        auto result_array = dest_column->get(3).get_array();
        for (auto& i : result_array) {
            if (i.is_null()) {
                ++null_values;
            } else {
                set_actual.insert(i.get_slice().to_string());
            }
        }
        ASSERT_TRUE(set_expect == set_actual);
        ASSERT_EQ(null_values, 1);
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_join_string) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"352", "66", "4325"});
    src_column->append_datum(DatumArray{"235", "99", "8", "43251"});
    src_column->append_datum(DatumArray{"44", "33", "22", "112"});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    Slice null_str("NULL");
    auto null_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(null_str, 3);

    auto dest_column = ArrayJoin::process(nullptr, {src_column, sep_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("352__66__4325"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("235__99__8__43251"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice("44__33__22__112"), dest_column->get(2).get_slice());

    dest_column = ArrayJoin::process(nullptr, {src_column, sep_column, null_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("352__66__4325"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("235__99__8__43251"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice("44__33__22__112"), dest_column->get(2).get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_ws) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"352", "66", "4325"});
    src_column->append_datum(DatumArray{"235", "99", "8", "43251"});
    src_column->append_datum(DatumArray{"44", "33", "22", "112"});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    ColumnPtr dest_column = ArrayFunctions::array_concat_ws(nullptr, {sep_column, src_column}).value();
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("352__66__4325"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("235__99__8__43251"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice("44__33__22__112"), dest_column->get(2).get_slice());
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_join_nullable_elements) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"55", Datum(), "333", "6666"});
    src_column->append_datum(DatumArray{"22", "333", Datum(), Datum()});
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    Slice null_str("NULL");
    auto null_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(null_str, 3);

    auto dest_column = ArrayJoin::process(nullptr, {src_column, sep_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("55__333__6666"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("22__333"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice(""), dest_column->get(2).get_slice());

    dest_column = ArrayJoin::process(nullptr, {src_column, sep_column, null_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("55__NULL__333__6666"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("22__333__NULL__NULL"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice("NULL__NULL__NULL__NULL"), dest_column->get(2).get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_ws_nullable_elements) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"55", Datum(), "333", "6666"});
    src_column->append_datum(DatumArray{"22", "333", Datum(), Datum()});
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    ColumnPtr dest_column = ArrayFunctions::array_concat_ws(nullptr, {sep_column, src_column}).value();
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("55__333__6666"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("22__333"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice(""), dest_column->get(2).get_slice());
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_join_nullable_array) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"5", Datum(), "33", "666"});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    Slice null_str("NULL");
    auto null_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(null_str, 3);

    auto dest_column = ArrayJoin::process(nullptr, {src_column, sep_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("5__33__666"), dest_column->get(0).get_slice());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_EQ(Slice(""), dest_column->get(2).get_slice());

    dest_column = ArrayJoin::process(nullptr, {src_column, sep_column, null_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("5__NULL__33__666"), dest_column->get(0).get_slice());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_EQ(Slice("NULL__NULL__NULL__NULL"), dest_column->get(2).get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_ws_nullable_array) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"5", Datum(), "33", "666"});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    ColumnPtr dest_column = ArrayFunctions::array_concat_ws(nullptr, {sep_column, src_column}).value();
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("5__33__666"), dest_column->get(0).get_slice());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_EQ(Slice(""), dest_column->get(2).get_slice());
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_join_only_null) {
    auto src_column = ColumnHelper::create_const_null_column(3);

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    Slice null_str("NULL");
    auto null_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(null_str, 3);

    auto dest_column = ArrayJoin::process(nullptr, {src_column, sep_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());

    dest_column = ArrayJoin::process(nullptr, {src_column, sep_column, null_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());
}

TEST_F(ArrayFunctionsTest, array_concat_ws_only_null) {
    auto src_column = ColumnHelper::create_const_null_column(3);

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    ColumnPtr dest_column = ArrayFunctions::array_concat_ws(nullptr, {sep_column, src_column}).value();
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());
}

TEST_F(ArrayFunctionsTest, array_filter_tinyint_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int8_t>({(int8_t)(5), (int8_t)(6)}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    _check_array<int8_t>({(int8_t)(4)}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_tinyint) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(DatumArray{(int8_t)99});
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true, false});
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<int8_t>({(int8_t)(3)}, dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)(8)}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_tinyint_with_nullable_notnull) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true, false});
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int8_t>({(int8_t)(3)}, dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)(8)}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_tinyint_notnull_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(DatumArray{(int8_t)99});
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<int8_t>({(int8_t)(5), (int8_t)(6)}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    _check_array<int8_t>({(int8_t)(4)}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_bigint_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int64_t>({(int64_t)(5), (int64_t)(6)}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    _check_array<int64_t>({(int64_t)(4)}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_bigint) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(DatumArray{(int64_t)99});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true});
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<int64_t>({(int64_t)(3)}, dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)(8)}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_double_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, (double)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<double>({(double)(5), (double)(6)}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    _check_array<double>({(double)(4)}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_double) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(DatumArray{(double)99});
    src_column->append_datum(DatumArray{(double)4, (double)1});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true, false, true}); //more one
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<double>({(double)(3)}, dest_column->get(0).get_array());
    _check_array<double>({(double)(8)}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_varchar_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<Slice>({Slice("5"), Slice("6")}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    _check_array<Slice>({Slice("4")}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_varchar) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(DatumArray{Slice("99")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true, false, true}); //more one
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<Slice>({Slice("3")}, dest_column->get(0).get_array());
    _check_array<Slice>({Slice("8")}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_with_onlynull) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});

    auto src_column2 = ColumnHelper::create_const_null_column(1);

    // bool_array is null
    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});
    ASSERT_TRUE(dest_column->get(0).get_array().empty());

    // array is null
    dest_column = filter.process(nullptr, {src_column2, src_column});
    ASSERT_TRUE(dest_column->only_null());

    // all null
    dest_column = filter.process(nullptr, {src_column2, src_column2});
    ASSERT_TRUE(dest_column->only_null());

    // src is nullable & bool_array is null
    auto src_column_nullable = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column_nullable->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column_nullable->append_datum(Datum());
    dest_column = filter.process(nullptr, {src_column_nullable, src_column2});
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data();
    ASSERT_TRUE(null_data.size() == 2);
    ASSERT_TRUE(!null_data.data()[0]);
    ASSERT_TRUE(null_data.data()[1]);
}

TEST_F(ArrayFunctionsTest, array_distinct_only_null) {
    // test only null
    {
        auto src_column = ColumnHelper::create_const_null_column(3);
        auto dest_column = ArrayDistinct<TYPE_VARCHAR>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->only_null());
    }
    // test const
    {
        auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        src_column = ConstColumn::create(std::move(src_column), 3);
        auto dest_column = ArrayDistinct<TYPE_VARCHAR>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666'], ['5','33','666'], ['5','33','666']]");
    }
    // test normal
    {
        auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        auto dest_column = ArrayDistinct<TYPE_VARCHAR>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 1);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666']]");
    }
}

} // namespace starrocks
