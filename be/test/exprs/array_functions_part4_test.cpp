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

TEST_F(ArrayFunctionsTest, array_sortby_tinyint_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)3, (int8_t)4, (int8_t)5});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)2, (int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{});
    src_column->append_datum(DatumArray{Datum(), (int8_t)-23});
    src_column->append_datum(DatumArray{(int8_t)43, (int8_t)23});
    src_column->append_datum(DatumArray{(int8_t)43, (int8_t)23, Datum()});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column2->append_datum(DatumArray{(int8_t)82, (int8_t)1, (int8_t)4});
    src_column2->append_datum(DatumArray{(int8_t)23});
    src_column2->append_datum(Datum());
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)-23, Datum()});
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)6, Datum()});

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 10);
        _check_array<int8_t>({(int8_t)(4), (int8_t)(5), (int8_t)(3)}, dest_column->get(0).get_array());
        ASSERT_TRUE(dest_column->get(1).is_null());
        _check_array<int8_t>({(int8_t)(2), (int8_t)(4)}, dest_column->get(2).get_array());
        ASSERT_TRUE(dest_column->get(3).is_null());
        ASSERT_TRUE(dest_column->get(4).get_array().empty());
        ASSERT_TRUE(dest_column->get(5).is_null());
        ASSERT_TRUE(dest_column->get(6).get_array().empty());
        ASSERT_TRUE(dest_column->get(7).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(7).get_array()[1].get_int8(), (int8_t)(-23));
        _check_array<int8_t>({(int8_t)(23), (int8_t)(43)}, dest_column->get(8).get_array());
        ASSERT_TRUE(dest_column->get(9).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(9).get_array()[1].get_int8(), (int8_t)(43));
        ASSERT_EQ(dest_column->get(9).get_array()[2].get_int8(), (int8_t)(23));
    }
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 10);
        _check_array<int8_t>({(int8_t)(82), (int8_t)(1), (int8_t)(4)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(23)}, dest_column->get(1).get_array());
        ASSERT_TRUE(dest_column->get(2).is_null());
        ASSERT_TRUE(dest_column->get(3).is_null());
        ASSERT_TRUE(dest_column->get(4).get_array().empty());
        ASSERT_TRUE(dest_column->get(5).get_array().empty());
        ASSERT_TRUE(dest_column->get(6).is_null());
        _check_array<int8_t>({(int8_t)(3), (int8_t)(6)}, dest_column->get(7).get_array());
        ASSERT_TRUE(dest_column->get(8).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(8).get_array()[1].get_int8(), (int8_t)(-23));
        ASSERT_TRUE(dest_column->get(9).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(9).get_array()[1].get_int8(), (int8_t)(6));
        ASSERT_EQ(dest_column->get(9).get_array()[2].get_int8(), (int8_t)(3));
    }
}

TEST_F(ArrayFunctionsTest, array_sortby_tinyint) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{});
    src_column->append_datum(DatumArray{(int8_t)125, (int8_t)123});
    src_column->append_datum(DatumArray{Datum()});
    src_column->append_datum(DatumArray{(int8_t)4, Datum()});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)73, (int8_t)30});
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{Datum(), (int8_t)-43});
    src_column2->append_datum(DatumArray{(int8_t)4});
    src_column2->append_datum(DatumArray{Datum(), (int8_t)43});

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});

        ASSERT_TRUE(!dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 6);

        _check_array<int8_t>({(int8_t)(5), (int8_t)(6), (int8_t)(3)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(6), (int8_t)(3), (int8_t)(5)}, dest_column->get(1).get_array());
        ASSERT_TRUE(dest_column->get(2).get_array().empty());
        _check_array<int8_t>({(int8_t)(125), (int8_t)(123)}, dest_column->get(3).get_array());
        ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(5).get_array()[0].get_int8(), (int8_t)(4));
        ASSERT_TRUE(dest_column->get(5).get_array()[1].is_null());
    }
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});

        ASSERT_TRUE(!dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 6);

        _check_array<int8_t>({(int8_t)(73), (int8_t)(3), (int8_t)(30)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(2), (int8_t)(3), (int8_t)(1)}, dest_column->get(1).get_array());
        ASSERT_TRUE(dest_column->get(2).get_array().empty());
        ASSERT_EQ(dest_column->get(3).get_array()[0].get_int8(), (int8_t)(-43));
        ASSERT_TRUE(dest_column->get(3).get_array()[1].is_null());
        ASSERT_EQ(dest_column->get(4).get_array()[0].get_int8(), (int8_t)(4));
        ASSERT_EQ(dest_column->get(5).get_array()[0].get_int8(), (int8_t)(43));
        ASSERT_TRUE(dest_column->get(5).get_array()[1].is_null());
    }
}

TEST_F(ArrayFunctionsTest, array_sortby_tinyint_with_nullable_notnull) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)3, (int8_t)4, (int8_t)5});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)2, (int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{});
    src_column->append_datum(DatumArray{Datum(), Datum()});
    src_column->append_datum(DatumArray{(int8_t)43, (int8_t)23});
    src_column->append_datum(DatumArray{(int8_t)43, (int8_t)23, Datum()});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)82, (int8_t)1, (int8_t)4});
    src_column2->append_datum(DatumArray{(int8_t)23});
    src_column2->append_datum(DatumArray{(int8_t)3, Datum()});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{Datum(), Datum()});
    src_column2->append_datum(DatumArray{(int8_t)-33, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)6, Datum()});
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 10);
        _check_array<int8_t>({(int8_t)(4), (int8_t)(5), (int8_t)(3)}, dest_column->get(0).get_array());
        ASSERT_TRUE(dest_column->get(1).is_null());
        _check_array<int8_t>({(int8_t)(4), (int8_t)(2)}, dest_column->get(2).get_array());
        ASSERT_TRUE(dest_column->get(3).is_null());
        ASSERT_TRUE(dest_column->get(4).get_array().empty());
        ASSERT_TRUE(dest_column->get(5).is_null());
        ASSERT_TRUE(dest_column->get(6).get_array().empty());
        ASSERT_TRUE(dest_column->get(7).get_array()[0].is_null());
        ASSERT_TRUE(dest_column->get(7).get_array()[1].is_null());
        _check_array<int8_t>({(int8_t)(43), (int8_t)(23)}, dest_column->get(8).get_array());
        ASSERT_TRUE(dest_column->get(9).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(9).get_array()[1].get_int8(), (int8_t)(43));
        ASSERT_EQ(dest_column->get(9).get_array()[2].get_int8(), (int8_t)(23));
    }
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});

        ASSERT_TRUE(!dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 10);
        _check_array<int8_t>({(int8_t)(82), (int8_t)(1), (int8_t)(4)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(23)}, dest_column->get(1).get_array());
        ASSERT_EQ(dest_column->get(2).get_array()[0].get_int8(), (int8_t)(3));
        ASSERT_TRUE(dest_column->get(2).get_array()[1].is_null());
        ASSERT_TRUE(dest_column->get(3).get_array()[0].is_null());
        ASSERT_TRUE(dest_column->get(4).get_array().empty());
        ASSERT_TRUE(dest_column->get(5).get_array().empty());
        ASSERT_TRUE(dest_column->get(6).get_array().empty());
        ASSERT_TRUE(dest_column->get(7).get_array()[0].is_null());
        ASSERT_TRUE(dest_column->get(7).get_array()[1].is_null());
        _check_array<int8_t>({(int8_t)(6), (int8_t)(-33)}, dest_column->get(8).get_array());
        ASSERT_TRUE(dest_column->get(9).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(9).get_array()[1].get_int8(), (int8_t)(6));
        ASSERT_EQ(dest_column->get(9).get_array()[2].get_int8(), (int8_t)(3));
    }
}

TEST_F(ArrayFunctionsTest, array_sortby_varchar_with_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    auto src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)3});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});
    src_column2->append_datum(DatumArray{Datum(), (int8_t)4});

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 6);
        _check_array<Slice>({Slice("5"), Slice("3"), Slice("6")}, dest_column->get(0).get_array());
        _check_array<Slice>({Slice("8")}, dest_column->get(1).get_array());
        _check_array<Slice>({Slice("4")}, dest_column->get(2).get_array());
        ASSERT_TRUE(dest_column->get(3).is_null());
        _check_array<Slice>({Slice("1"), Slice("4")}, dest_column->get(4).get_array());
        _check_array<Slice>({Slice("4"), Slice("1")}, dest_column->get(5).get_array());
    }
    {
        ArraySortBy<LogicalType::TYPE_VARCHAR> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 6);
        _check_array<int8_t>({(int8_t)(5), (int8_t)(3), (int8_t)(6)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(3)}, dest_column->get(1).get_array());
        ASSERT_TRUE(dest_column->get(2).is_null());
        ASSERT_TRUE(dest_column->get(3).get_array()[0].is_null());
        ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(4).get_array()[1].get_int8(), (int8_t)(4));
        ASSERT_EQ(dest_column->get(5).get_array()[0].get_int8(), (int8_t)(4));
        ASSERT_TRUE(dest_column->get(5).get_array()[1].is_null());
    }
}

TEST_F(ArrayFunctionsTest, array_sortby_with_only_null) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});

    auto src_column2 = ColumnHelper::create_const_null_column(1);

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});
        _check_array<int8_t>({(int8_t)(5), (int8_t)(3), (int8_t)(6)}, dest_column->get(0).get_array());
    }
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});
        ASSERT_TRUE(dest_column->only_null());
    }

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column2});
        ASSERT_TRUE(dest_column->only_null());
    }
}

TEST_F(ArrayFunctionsTest, array_generate_with_integer_columns) {
    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    start_column->append_datum(Datum((int32_t)3));
    stop_column->append_datum(Datum((int32_t)9));
    step_column->append_datum(Datum((int32_t)2));

    start_column->append_datum(Datum((int32_t)3));
    stop_column->append_datum(Datum((int32_t)9));
    step_column->append_datum(Datum((int32_t)3));

    start_column->append_datum(Datum((int32_t)3));
    stop_column->append_datum(Datum((int32_t)9));
    step_column->append_datum(Datum((int32_t)4));

    start_column->append_datum(Datum((int32_t)9));
    stop_column->append_datum(Datum((int32_t)3));
    step_column->append_datum(Datum((int32_t)-2));

    // if one input is null, then output is null
    start_column->append_datum(Datum());
    stop_column->append_datum(Datum((int32_t)9));
    step_column->append_datum(Datum((int32_t)2));

    start_column->append_datum(Datum((int32_t)10));
    stop_column->append_datum(Datum((int32_t)3));
    step_column->append_datum(Datum((int32_t)6));

    auto dest_column = ArrayGenerate<TYPE_INT>::process(nullptr, {start_column, stop_column, step_column}).value();

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 6);

    _check_array<int32_t>({(int32_t)(3), (int32_t)(5), (int32_t)(7), (int32_t)(9)}, dest_column->get(0).get_array());
    _check_array<int32_t>({(int32_t)(3), (int32_t)(6), (int32_t)(9)}, dest_column->get(1).get_array());
    _check_array<int32_t>({(int32_t)(3), (int32_t)(7)}, dest_column->get(2).get_array());
    _check_array<int32_t>({(int32_t)(9), (int32_t)(7), (int32_t)(5), (int32_t)(3)}, dest_column->get(3).get_array());
    ASSERT_TRUE(dest_column->is_null(4));
    ASSERT_TRUE(dest_column->get(5).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_generate_when_overflow) {
    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);

    start_column->append_datum(Datum((int8_t)9));
    stop_column->append_datum(Datum((int8_t)100));
    step_column->append_datum(Datum((int8_t)88));

    start_column->append_datum(Datum((int8_t)-9));
    stop_column->append_datum(Datum((int8_t)-100));
    step_column->append_datum(Datum((int8_t)-88));

    auto dest_column = ArrayGenerate<TYPE_TINYINT>::process(nullptr, {start_column, stop_column, step_column}).value();

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 2);

    _check_array<int8_t>({(int8_t)(9), (int8_t)(97)}, dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)(-9), (int8_t)(-97)}, dest_column->get(1).get_array());
}

TEST_F(ArrayFunctionsTest, array_distinct_any_type_only_null) {
    // test only null
    {
        auto src_column = ColumnHelper::create_const_null_column(3);
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->only_null());
    }
    // test const
    {
        auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        src_column = ConstColumn::create(std::move(src_column), 3);
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666'], ['5','33','666'], ['5','33','666']]");
    }
    // test array[null]
    {
        auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        src_column->append_datum(DatumArray{"5", Datum(), Datum(), "5", "33", "666", Datum()});
        src_column = ConstColumn::create(std::move(src_column), 1);
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 1);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5',NULL,'33','666']]");
    }
    // test null array
    {
        auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        src_column->append_nulls(2);
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666'], NULL, NULL]");
    }
    // test normal
    {
        auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 1);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666']]");
    }
}

TEST_F(ArrayFunctionsTest, array_intersect_any_type_int) {
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

    auto dest_column =
            ArrayFunctions::array_intersect_any_type(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int32_t>({(int32_t)(5)}, dest_column->get(0).get_array());
    _check_array<int32_t>({}, dest_column->get(1).get_array());
    _check_array<int32_t>({(int32_t)(4), (int32_t)(1)}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_any_type_int_with_not_null) {
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

    auto dest_column =
            ArrayFunctions::array_intersect_any_type(nullptr, {src_column, src_column2, src_column3}).value();

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

TEST_F(ArrayFunctionsTest, array_intersect_any_type_varchar) {
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

    auto dest_column =
            ArrayFunctions::array_intersect_any_type(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<Slice>({Slice("5")}, dest_column->get(0).get_array());
    _check_array<Slice>({}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("4"), Slice("1")}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_any_type_varchar_with_not_null) {
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

TEST_F(ArrayFunctionsTest, array_reverse_any_types_int) {
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

TEST_F(ArrayFunctionsTest, array_reverse_any_types_string) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"352", "66", "4325"});
    src_column->append_datum(DatumArray{"235", "99", "8", "43251"});
    src_column->append_datum(DatumArray{"44", "33", "22", "112"});

    auto dest_column = ArrayFunctions::array_reverse_any_types(nullptr, {src_column}).value();

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<Slice>({"4325", "66", "352"}, dest_column->get(0).get_array());
    _check_array<Slice>({"43251", "8", "99", "235"}, dest_column->get(1).get_array());
    _check_array<Slice>({"112", "22", "33", "44"}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_any_types_nullable_elements) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 3, 6});
    src_column->append_datum(DatumArray{2, 3, Datum(), Datum()});
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    auto dest_column = ArrayFunctions::array_reverse_any_types(nullptr, {src_column}).value();

    ASSERT_EQ(dest_column->size(), 3);
    _check_array_nullable<int32_t>({6, 3, 0, 5}, {0, 0, 1, 0}, dest_column->get(0).get_array());
    _check_array_nullable<int32_t>({0, 0, 3, 2}, {1, 1, 0, 0}, dest_column->get(1).get_array());
    _check_array_nullable<int32_t>({0, 0, 0, 0}, {1, 1, 1, 1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_any_types_nullable_array) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 3, 6});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    auto dest_column = ArrayFunctions::array_reverse_any_types(nullptr, {src_column}).value();

    ASSERT_EQ(dest_column->size(), 3);
    _check_array_nullable<int32_t>({6, 3, 0, 5}, {0, 0, 1, 0}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).is_null());
    _check_array_nullable<int32_t>({0, 0, 0, 0}, {1, 1, 1, 1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_any_types_only_null) {
    auto src_column = ColumnHelper::create_const_null_column(3);

    auto dest_column = ArrayFunctions::array_reverse_any_types(nullptr, {src_column}).value();

    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());
}

TEST_F(ArrayFunctionsTest, array_match_nullable) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column->append_datum(DatumArray{(int8_t)1, (int8_t)1, (int8_t)0});
    src_column->append_datum(DatumArray{(int8_t)0});
    src_column->append_datum(DatumArray{(int8_t)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)1, Datum()});
    src_column->append_datum(DatumArray{(int8_t)0, Datum()});
    src_column->append_datum(DatumArray{});

    auto dest_column = ArrayMatch<true>::process(nullptr, {src_column});
    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 7);
    ASSERT_TRUE(dest_column->get(0).get_int8());
    ASSERT_FALSE(dest_column->get(1).get_int8());
    ASSERT_TRUE(dest_column->get(2).get_int8());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_int8());
    ASSERT_TRUE(dest_column->get(5).is_null());
    ASSERT_FALSE(dest_column->get(6).get_int8());

    dest_column = ArrayMatch<false>::process(nullptr, {src_column});
    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 7);
    ASSERT_FALSE(dest_column->get(0).get_int8());
    ASSERT_FALSE(dest_column->get(1).get_int8());
    ASSERT_TRUE(dest_column->get(2).get_int8());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).is_null());
    ASSERT_FALSE(dest_column->get(5).get_int8());
    ASSERT_TRUE(dest_column->get(6).get_int8());
}

TEST_F(ArrayFunctionsTest, array_match_not_null) {
    auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column->append_datum(DatumArray{(int8_t)1, (int8_t)1, (int8_t)0});
    src_column->append_datum(DatumArray{(int8_t)0});
    src_column->append_datum(DatumArray{(int8_t)1});
    src_column->append_datum(DatumArray{Datum()});
    src_column->append_datum(DatumArray{(int8_t)1, Datum()});
    src_column->append_datum(DatumArray{(int8_t)0, Datum()});
    src_column->append_datum(DatumArray{});

    auto dest_column = ArrayMatch<true>::process(nullptr, {src_column});
    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 7);
    ASSERT_TRUE(dest_column->get(0).get_int8());
    ASSERT_FALSE(dest_column->get(1).get_int8());
    ASSERT_TRUE(dest_column->get(2).get_int8());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_int8());
    ASSERT_TRUE(dest_column->get(5).is_null());
    ASSERT_FALSE(dest_column->get(6).get_int8());

    dest_column = ArrayMatch<false>::process(nullptr, {src_column});
    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 7);
    ASSERT_FALSE(dest_column->get(0).get_int8());
    ASSERT_FALSE(dest_column->get(1).get_int8());
    ASSERT_TRUE(dest_column->get(2).get_int8());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).is_null());
    ASSERT_FALSE(dest_column->get(5).get_int8());
    ASSERT_TRUE(dest_column->get(6).get_int8());
}

TEST_F(ArrayFunctionsTest, array_match_only_null) {
    // test only null
    {
        auto src_column = ColumnHelper::create_const_null_column(3);
        auto dest_column = ArrayMatch<false>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->only_null());

        dest_column = ArrayMatch<true>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->only_null());
    }
    // test const
    {
        auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        src_column->append_datum(DatumArray{(uint8) false, (uint8) true});
        src_column = ConstColumn::create(std::move(src_column), 3);
        auto dest_column = ArrayMatch<false>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_FALSE(dest_column->get(0).get_int8());

        dest_column = ArrayMatch<true>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->get(0).get_int8());
    }
    // test const
    {
        auto src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        src_column->append_datum(DatumArray{});
        src_column = ConstColumn::create(std::move(src_column), 3);
        auto dest_column = ArrayMatch<true>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_FALSE(dest_column->get(0).get_int8());

        dest_column = ArrayMatch<false>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->get(0).get_int8());
    }
}
// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_seq) {
    // array_contains_seq(["a", "b", "c"], ["c"])         -> 1
    // array_contains_seq(NULL, ["c"])                    -> NULL
    // array_contains_seq(["a", "b", "c"], NULL)          -> NULL
    // array_contains_seq(["a", "b", NULL], NULL)         -> NULL
    // array_contains_seq(["a", "b", NULL], ["a", NULL])  -> 0
    // array_contains_seq(NULL, ["a", NULL])              -> NULL
    // array_contains_seq(["a", "b", NULL], [NULL])       -> 1
    // array_contains_seq(["a", "b", "c"], ["d"])         -> 0
    // array_contains_seq(["a", "b", "c"], ["a", "d"])    -> 0
    // array_contains_all(["a", "b", "c"], ["a", "c"])    -> 0
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
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

        auto target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
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
        auto result = ArrayFunctions::array_contains_seq_specific<TYPE_VARCHAR>(&ctx, {array, target}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
        EXPECT_TRUE(result->get(3).is_null());
        EXPECT_EQ(0, result->get(4).get_int8());
        EXPECT_TRUE(result->get(5).is_null());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(0, result->get(7).get_int8());
        EXPECT_EQ(0, result->get(8).get_int8());
        EXPECT_EQ(0, result->get(9).get_int8());
    }

    // array_contains_seq([["a"], ["b"]], [["c"]])
    // array_contains_seq(["a","c"], [["c"]])
    // array_contains_seq([["a", "b"], ["c"]], [["a", "b"]])
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum(DatumArray{"a"}), Datum(DatumArray{"b"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "c"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum(DatumArray{"c"})});

        auto target = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"a", "b"})});

        auto result = ArrayFunctions::array_contains_seq(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains_seq([["a"], ["b"], [NULL]], [["c"]])
    // array_contains_seq([["a","d","c"]], [["e"]])
    // array_contains_seq([["a", "b"], ["c"]], [["a", "b"]])
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{Datum(DatumArray{"a"}), Datum(DatumArray{"b"}), Datum()});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "d", "c"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum(DatumArray{"c"})});

        auto target = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"e"})});
        target->append_datum(DatumArray{Datum(DatumArray{"a", "b"})});

        auto result = ArrayFunctions::array_contains_seq(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains_seq([["a"], ["b"]], [["c"], [NULL]])
    // array_contains_seq([["a","d","c"]], [["e", NULL]])
    // array_contains_seq([["a", "b"], ["c"]], [["a", "b"]])
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum(DatumArray{"a"}), Datum(DatumArray{"b"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "d", "c"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum(DatumArray{"c"})});

        auto target = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        target->append_datum(DatumArray{Datum(DatumArray{"c"}), Datum()});
        target->append_datum(DatumArray{Datum(DatumArray{"e"}), Datum()});
        target->append_datum(DatumArray{Datum(DatumArray{"a", "b"})});

        auto result = ArrayFunctions::array_contains_seq(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
}

template <LogicalType Type>
void array_repeat_test(const Datum& element_0, const Datum& element_1, const Datum& element_2,
                       const Datum& element_null) {
    {
        using CppType = RunTimeCppType<Type>;

        int32_t repeat_count_0 = (int32_t)1;
        int32_t repeat_count_1 = (int32_t)-2;
        int32_t repeat_count_2 = (int32_t)3;
        Datum repeat_count_null;

        // The normal case
        {
            auto src_column = ColumnHelper::create_column(TypeDescriptor(Type), false, false, 0);
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_2);

            auto repeat_count_column = Int32Column::create();
            repeat_count_column->append(repeat_count_0);
            repeat_count_column->append(repeat_count_1);
            repeat_count_column->append(repeat_count_2);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_EQ(dest_column->get(0).get_array().size(), 1);
            if (Type == TYPE_JSON) {
                ASSERT_EQ(element_0.get_json()->get_slice(),
                          dest_column->get(0).get_array()[0].get_json()->get_slice());
                ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
                ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
                ASSERT_EQ(element_2.get_json()->get_slice(),
                          dest_column->get(2).get_array()[0].get_json()->get_slice());
                ASSERT_EQ(element_2.get_json()->get_slice(),
                          dest_column->get(2).get_array()[1].get_json()->get_slice());
                ASSERT_EQ(element_2.get_json()->get_slice(),
                          dest_column->get(2).get_array()[2].get_json()->get_slice());
            } else {
                ASSERT_EQ(element_0.get<CppType>(), dest_column->get(0).get_array()[0].get<CppType>());
                ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
                ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
                ASSERT_EQ(element_2.get<CppType>(), dest_column->get(2).get_array()[0].get<CppType>());
                ASSERT_EQ(element_2.get<CppType>(), dest_column->get(2).get_array()[1].get<CppType>());
                ASSERT_EQ(element_2.get<CppType>(), dest_column->get(2).get_array()[2].get<CppType>());
            }
        }

        // The case for testing NullableColumn
        {
            auto src_column = ColumnHelper::create_column(TypeDescriptor(Type), true, false, 0);
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_null);

            auto repeat_count_column =
                    NullableColumn::create(Int32Column::create(), NullColumn::create(0, std::move(DATUM_NULL)));
            repeat_count_column->append_datum(repeat_count_null);
            repeat_count_column->append_datum(Datum(repeat_count_1));
            repeat_count_column->append_datum(Datum(repeat_count_2));

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_TRUE(dest_column->get(0).is_null());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
            ASSERT_TRUE(dest_column->get(2).get_array()[0].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[1].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[2].is_null());
        }

        // The case for testing ConstColumn
        {
            size_t const_column_row_count = 2;

            auto src_column = ColumnHelper::create_column(TypeDescriptor(Type), false, true, 0);
            for (int i = 0; i < const_column_row_count; i++) {
                src_column->append_datum(element_0);
            }

            auto repeat_count_data_column = Int32Column::create();
            repeat_count_data_column->append(repeat_count_0);
            auto repeat_count_column = ConstColumn::create(std::move(repeat_count_data_column), const_column_row_count);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), const_column_row_count);
            if (Type == TYPE_JSON) {
                ASSERT_EQ(element_0.get_json()->get_slice(),
                          dest_column->get(0).get_array()[0].get_json()->get_slice());
                ASSERT_EQ(element_0.get_json()->get_slice(),
                          dest_column->get(1).get_array()[0].get_json()->get_slice());
            } else {
                ASSERT_EQ(element_0.get<CppType>(), dest_column->get(0).get_array()[0].get<CppType>());
                ASSERT_EQ(element_0.get<CppType>(), dest_column->get(1).get_array()[0].get<CppType>());
            }
        }
    }
}

TEST_F(ArrayFunctionsTest, array_repeat) {
    {
        array_repeat_test<TYPE_INT>(Datum((int32_t)0), Datum((int32_t)1), Datum((int32_t)2), Datum());
        array_repeat_test<TYPE_BIGINT>(Datum((int64_t)0), Datum((int64_t)1), Datum((int64_t)2), Datum());
        array_repeat_test<TYPE_FLOAT>(Datum((float)0), Datum((float)0.1), Datum((float)0.2), Datum());
        array_repeat_test<TYPE_DOUBLE>(Datum((double)0), Datum((double)0.1), Datum((double)0.2), Datum());
        array_repeat_test<TYPE_DECIMALV2>(Datum(DecimalV2Value(std::string("0.0000000000"))),
                                          Datum(DecimalV2Value(std::string("1.0000000000"))),
                                          Datum(DecimalV2Value(std::string("2.0000000000"))), Datum());
        array_repeat_test<TYPE_BOOLEAN>(Datum(true), (false), Datum(false), Datum());
        array_repeat_test<TYPE_DATE>(DateValue::create(2020, 0, 0), DateValue::create(2021, 1, 1),
                                     DateValue::create(2022, 2, 2), Datum());
        array_repeat_test<TYPE_DATETIME>(TimestampValue::create(2020, 0, 0, 0, 0, 0),
                                         TimestampValue::create(2021, 1, 1, 1, 1, 1),
                                         TimestampValue::create(2022, 2, 2, 2, 2, 2), Datum());
        array_repeat_test<TYPE_VARCHAR>(Datum(Slice("0")), Datum(Slice("1")), Datum(Slice("2")), Datum());
        JsonValue json_element_0 = JsonValue::parse("{\"a\": 0}").value();
        JsonValue json_element_1 = JsonValue::parse("{\"b\": 1}").value();
        JsonValue json_element_2 = JsonValue::parse("{\"c\": 2}").value();
        array_repeat_test<TYPE_JSON>(Datum(&json_element_0), Datum(&json_element_1), Datum(&json_element_2), Datum());
    }
}

TEST_F(ArrayFunctionsTest, array_repeat_array) {
    {
        Datum element_0 = DatumArray{(int32_t)0};
        Datum element_1 = DatumArray{Datum()};
        Datum element_2 = DatumArray{Datum(), (int32_t)2};
        Datum element_null;
        int32_t repeat_count_0 = (int32_t)1;
        int32_t repeat_count_1 = (int32_t)-2;
        int32_t repeat_count_2 = (int32_t)3;
        Datum repeat_count_null;

        // The normal case
        {
            auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_2);

            auto repeat_count_column = Int32Column::create();
            repeat_count_column->append(repeat_count_0);
            repeat_count_column->append(repeat_count_1);
            repeat_count_column->append(repeat_count_2);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_EQ(dest_column->get(0).get_array().size(), 1);
            _check_array<int32_t>({(int32_t)0}, dest_column->get(0).get_array()[0].get_array());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            _check_array<int32_t>({}, dest_column->get(1).get_array());
            ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
            ASSERT_TRUE(dest_column->get(2).get_array()[0].get_array()[0].is_null());
            ASSERT_EQ((int32_t)2, dest_column->get(2).get_array()[0].get_array()[1].get_int32());
            ASSERT_TRUE(dest_column->get(2).get_array()[1].get_array()[0].is_null());
            ASSERT_EQ((int32_t)2, dest_column->get(2).get_array()[1].get_array()[1].get_int32());
            ASSERT_TRUE(dest_column->get(2).get_array()[2].get_array()[0].is_null());
            ASSERT_EQ((int32_t)2, dest_column->get(2).get_array()[2].get_array()[1].get_int32());
        }

        // The case for testing NullableColumn
        {
            auto src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_null);

            auto repeat_count_column =
                    NullableColumn::create(Int32Column::create(), NullColumn::create(0, std::move(DATUM_NULL)));
            repeat_count_column->append_datum(repeat_count_null);
            repeat_count_column->append_datum(Datum(repeat_count_1));
            repeat_count_column->append_datum(Datum(repeat_count_2));

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_TRUE(dest_column->get(0).is_null());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            _check_array<int32_t>({}, dest_column->get(1).get_array());
            ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
            ASSERT_TRUE(dest_column->get(2).get_array()[0].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[1].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[2].is_null());
        }

        // The case for testing ConstColumn
        {
            size_t const_column_row_count = 2;

            auto src_data_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
            src_data_column->append_datum(element_0);
            auto src_column = ConstColumn::create(std::move(src_data_column), const_column_row_count);

            auto repeat_count_data_column = Int32Column::create();
            repeat_count_data_column->append(repeat_count_0);
            auto repeat_count_column = ConstColumn::create(std::move(repeat_count_data_column), const_column_row_count);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), const_column_row_count);
            _check_array<int32_t>({(int32_t)0}, dest_column->get(0).get_array()[0].get_array());
        }
    }
}

TEST_F(ArrayFunctionsTest, array_repeat_map) {
    {
        DatumMap element_0;
        DatumMap element_1;
        DatumMap element_2;
        Datum element_null;
        int32_t repeat_count_0 = (int32_t)1;
        int32_t repeat_count_1 = (int32_t)-2;
        int32_t repeat_count_2 = (int32_t)3;
        Datum repeat_count_null;
        element_0[(int32_t)0] = (int32_t)0;
        element_0[(int32_t)1] = (int32_t)11;
        element_0[(int32_t)2] = (int32_t)22;
        element_1[(int32_t)3] = (int32_t)33;
        element_1[(int32_t)4] = (int32_t)44;
        element_1[(int32_t)5] = (int32_t)55;
        element_2[(int32_t)6] = (int32_t)66;
        element_2[(int32_t)7] = (int32_t)77;
        element_2[(int32_t)8] = (int32_t)88;

        // The normal case
        {
            auto offsets = UInt32Column::create();
            auto keys_data = Int32Column::create();
            auto keys_null = NullColumn::create();
            auto keys = NullableColumn::create(std::move(keys_data), std::move(keys_null));
            auto values_data = Int32Column::create();
            auto values_null = NullColumn::create();
            auto values = NullableColumn::create(std::move(values_data), std::move(values_null));
            auto src_column = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_2);

            auto repeat_count_column = Int32Column::create();
            repeat_count_column->append(repeat_count_0);
            repeat_count_column->append(repeat_count_1);
            repeat_count_column->append(repeat_count_2);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_EQ(dest_column->get(0).get_array().size(), repeat_count_0);
            ASSERT_EQ(element_0.find(2)->second.get_int32(),
                      dest_column->get(0).get_array()[0].get<DatumMap>().find(2)->second.get_int32());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            _check_array<int32_t>({}, dest_column->get(1).get_array());
            ASSERT_EQ(dest_column->get(2).get_array().size(), repeat_count_2);
            ASSERT_EQ(element_2.find(8)->second.get_int32(),
                      dest_column->get(2).get_array()[2].get<DatumMap>().find(8)->second.get_int32());
        }

        // The case for testing NullableColumn
        {
            auto offsets = UInt32Column::create();
            auto keys_data = Int32Column::create();
            auto keys_null = NullColumn::create();
            auto keys = NullableColumn::create(std::move(keys_data), std::move(keys_null));
            auto values_data = Int32Column::create();
            auto values_null = NullColumn::create();
            auto values = NullableColumn::create(std::move(values_data), std::move(values_null));
            auto map_column = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));
            auto src_column =
                    NullableColumn::create(std::move(map_column), NullColumn::create(0, std::move(DATUM_NULL)));
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_null);

            auto count_column =
                    NullableColumn::create(Int32Column::create(), NullColumn::create(0, std::move(DATUM_NULL)));
            count_column->append_datum(repeat_count_null);
            count_column->append_datum(Datum(repeat_count_1));
            count_column->append_datum(Datum(repeat_count_2));

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_TRUE(dest_column->get(0).is_null());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            _check_array<int32_t>({}, dest_column->get(1).get_array());
            ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
            ASSERT_TRUE(dest_column->get(2).get_array()[0].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[1].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[2].is_null());
        }

        // The case for testing ConstColumn
        {
            size_t const_column_row_count = 2;
            auto offsets = UInt32Column::create();
            auto keys_data = Int32Column::create();
            auto keys_null = NullColumn::create();
            auto keys = NullableColumn::create(std::move(keys_data), std::move(keys_null));
            auto values_data = Int32Column::create();
            auto values_null = NullColumn::create();
            auto values = NullableColumn::create(std::move(values_data), std::move(values_null));
            auto map_column = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));
            map_column->append_datum(element_0);
            auto src_column = ConstColumn::create(std::move(map_column), const_column_row_count);

            auto repeat_count_data_column = Int32Column::create();
            repeat_count_data_column->append(repeat_count_0);
            auto repeat_count_column = ConstColumn::create(std::move(repeat_count_data_column), const_column_row_count);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), const_column_row_count);
            ASSERT_EQ(element_0.find(2)->second.get_int32(),
                      dest_column->get(0).get_array()[0].get<DatumMap>().find(2)->second.get_int32());
            ASSERT_EQ(element_0.find(2)->second.get_int32(),
                      dest_column->get(1).get_array()[0].get<DatumMap>().find(2)->second.get_int32());
        }
    }
}

TEST_F(ArrayFunctionsTest, array_flatten_int) {
    // array_flatten(NULL): NULL
    // array_flatten([[1, 2], [1, 4]]): [1,2,1,4]
    // array_flatten([[1, 2], [3]]): [1,2,3]
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true);
        array->append_nulls(1);
        array->append_datum(DatumArray{DatumArray{1, 2}, DatumArray{1, 4}});
        array->append_datum(DatumArray{DatumArray{1, 2}, DatumArray{3}});

        auto result = ArrayFunctions::array_flatten(nullptr, {std::move(array)}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_EQ("[1,2,1,4]", result->debug_item(1));
        EXPECT_EQ("[1,2,3]", result->debug_item(2));
    }
}

TEST_F(ArrayFunctionsTest, null_or_empty) {
    // null_or_empty(NULL): 1
    // null_or_empty([1,2]): 0
    // null_or_empty([]): 1
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_nulls(1);
        array->append_datum(DatumArray{{1, 2}});
        array->append_datum(DatumArray{});

        auto result = ArrayFunctions::null_or_empty(nullptr, {std::move(array)}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ("1", result->debug_item(0));
        EXPECT_EQ("0", result->debug_item(1));
        EXPECT_EQ("1", result->debug_item(2));
    }
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(DatumArray{{1, 2}});
        array->append_datum(DatumArray{});

        auto result = ArrayFunctions::null_or_empty(nullptr, {std::move(array)}).value();
        EXPECT_EQ("0", result->debug_item(0));
        EXPECT_EQ("1", result->debug_item(1));
    }
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(DatumArray{{1, 2}});
        auto result = ArrayFunctions::null_or_empty(nullptr, {std::move(array)}).value();
        EXPECT_EQ("0", result->debug_item(0));
    }
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(DatumArray{{1, 2}});
        auto literal = ConstColumn::create(std::move(array), 10);
        auto result = ArrayFunctions::null_or_empty(nullptr, {std::move(literal)}).value();
        EXPECT_EQ("CONST: 0", result->debug_item(0));
    }
    {
        auto null_col = ColumnHelper::create_const_null_column(10);
        auto result = ArrayFunctions::null_or_empty(nullptr, {std::move(null_col)}).value();
        EXPECT_EQ("CONST: 1", result->debug_item(0));
    }
}

// Tests for time series array generation functions
TEST_F(ArrayFunctionsTest, array_generate_date_with_year_unit) {
    // Test DATE type with YEAR unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATE), TypeDescriptor::from_logical_type(TYPE_DATE),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATE)));

    // Set constant column for time unit
    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("year"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case 1: 2020-01-01 to 2023-01-01, step 1 year
    start_column->append_datum(DateValue::create(2020, 1, 1));
    stop_column->append_datum(DateValue::create(2023, 1, 1));
    step_column->append_datum(Datum((int32_t)1));

    // Test case 2: 2020-06-15 to 2025-06-15, step 2 years
    start_column->append_datum(DateValue::create(2020, 6, 15));
    stop_column->append_datum(DateValue::create(2025, 6, 15));
    step_column->append_datum(Datum((int32_t)2));

    // Test case 3: Reverse direction - 2023-01-01 to 2020-01-01, step 1 year
    start_column->append_datum(DateValue::create(2023, 1, 1));
    stop_column->append_datum(DateValue::create(2020, 1, 1));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATE>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 3);

    // Verify test case 1: should generate [2020-01-01, 2021-01-01, 2022-01-01, 2023-01-01]
    auto array1 = dest_column->get(0).get_array();
    ASSERT_EQ(array1.size(), 4);
    EXPECT_EQ(DateValue::create(2020, 1, 1), array1[0].get_date());
    EXPECT_EQ(DateValue::create(2021, 1, 1), array1[1].get_date());
    EXPECT_EQ(DateValue::create(2022, 1, 1), array1[2].get_date());
    EXPECT_EQ(DateValue::create(2023, 1, 1), array1[3].get_date());

    // Verify test case 2: should generate [2020-06-15, 2022-06-15, 2024-06-15]
    auto array2 = dest_column->get(1).get_array();
    ASSERT_EQ(array2.size(), 3);
    EXPECT_EQ(DateValue::create(2020, 6, 15), array2[0].get_date());
    EXPECT_EQ(DateValue::create(2022, 6, 15), array2[1].get_date());
    EXPECT_EQ(DateValue::create(2024, 6, 15), array2[2].get_date());

    // Verify test case 3: should generate [2023-01-01, 2022-01-01, 2021-01-01, 2020-01-01]
    auto array3 = dest_column->get(2).get_array();
    ASSERT_EQ(array3.size(), 4);
    EXPECT_EQ(DateValue::create(2023, 1, 1), array3[0].get_date());
    EXPECT_EQ(DateValue::create(2022, 1, 1), array3[1].get_date());
    EXPECT_EQ(DateValue::create(2021, 1, 1), array3[2].get_date());
    EXPECT_EQ(DateValue::create(2020, 1, 1), array3[3].get_date());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_date_with_month_unit) {
    // Test DATE type with MONTH unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATE), TypeDescriptor::from_logical_type(TYPE_DATE),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATE)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("month"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-15 to 2020-04-15, step 1 month
    start_column->append_datum(DateValue::create(2020, 1, 15));
    stop_column->append_datum(DateValue::create(2020, 4, 15));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATE>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate [2020-01-15, 2020-02-15, 2020-03-15, 2020-04-15]
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 4);
    EXPECT_EQ(DateValue::create(2020, 1, 15), array[0].get_date());
    EXPECT_EQ(DateValue::create(2020, 2, 15), array[1].get_date());
    EXPECT_EQ(DateValue::create(2020, 3, 15), array[2].get_date());
    EXPECT_EQ(DateValue::create(2020, 4, 15), array[3].get_date());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_date_with_day_unit) {
    // Test DATE type with DAY unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATE), TypeDescriptor::from_logical_type(TYPE_DATE),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATE)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("day"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 to 2020-01-05, step 1 day
    start_column->append_datum(DateValue::create(2020, 1, 1));
    stop_column->append_datum(DateValue::create(2020, 1, 5));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATE>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate [2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05]
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 5);
    EXPECT_EQ(DateValue::create(2020, 1, 1), array[0].get_date());
    EXPECT_EQ(DateValue::create(2020, 1, 2), array[1].get_date());
    EXPECT_EQ(DateValue::create(2020, 1, 3), array[2].get_date());
    EXPECT_EQ(DateValue::create(2020, 1, 4), array[3].get_date());
    EXPECT_EQ(DateValue::create(2020, 1, 5), array[4].get_date());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_datetime_with_hour_unit) {
    // Test DATETIME type with HOUR unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_DATETIME),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATETIME)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("hour"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 10:00:00 to 2020-01-01 14:00:00, step 2 hours
    start_column->append_datum(TimestampValue::create(2020, 1, 1, 10, 0, 0));
    stop_column->append_datum(TimestampValue::create(2020, 1, 1, 14, 0, 0));
    step_column->append_datum(Datum((int32_t)2));

    auto dest_column =
            ArrayGenerate<TYPE_DATETIME>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate [10:00:00, 12:00:00, 14:00:00]
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 3);
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 0, 0), array[0].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 12, 0, 0), array[1].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 14, 0, 0), array[2].get_timestamp());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_datetime_with_minute_unit) {
    // Test DATETIME type with MINUTE unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_DATETIME),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATETIME)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("minute"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 10:00:00 to 2020-01-01 10:05:00, step 1 minute
    start_column->append_datum(TimestampValue::create(2020, 1, 1, 10, 0, 0));
    stop_column->append_datum(TimestampValue::create(2020, 1, 1, 10, 5, 0));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATETIME>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate 6 timestamps
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 6);
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 0, 0), array[0].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 1, 0), array[1].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 2, 0), array[2].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 3, 0), array[3].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 4, 0), array[4].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 5, 0), array[5].get_timestamp());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_datetime_with_second_unit) {
    // Test DATETIME type with SECOND unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_DATETIME),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATETIME)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("second"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 10:00:00 to 2020-01-01 10:00:03, step 1 second
    start_column->append_datum(TimestampValue::create(2020, 1, 1, 10, 0, 0));
    stop_column->append_datum(TimestampValue::create(2020, 1, 1, 10, 0, 3));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATETIME>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate 4 timestamps
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 4);
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 0, 0), array[0].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 0, 1), array[1].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 0, 2), array[2].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 0, 3), array[3].get_timestamp());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_date_with_null_values) {
    // Test DATE type with NULL values
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATE), TypeDescriptor::from_logical_type(TYPE_DATE),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATE)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("day"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case 1: Normal case
    start_column->append_datum(DateValue::create(2020, 1, 1));
    stop_column->append_datum(DateValue::create(2020, 1, 3));
    step_column->append_datum(Datum((int32_t)1));

    // Test case 2: NULL start
    start_column->append_datum(Datum());
    stop_column->append_datum(DateValue::create(2020, 1, 3));
    step_column->append_datum(Datum((int32_t)1));

    // Test case 3: NULL stop
    start_column->append_datum(DateValue::create(2020, 1, 1));
    stop_column->append_datum(Datum());
    step_column->append_datum(Datum((int32_t)1));

    // Test case 4: step = 0
    start_column->append_datum(DateValue::create(2020, 1, 1));
    stop_column->append_datum(DateValue::create(2020, 1, 3));
    step_column->append_datum(Datum((int32_t)0));

    auto dest_column =
            ArrayGenerate<TYPE_DATE>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 4);

    // Verify test case 1: should generate [2020-01-01, 2020-01-02, 2020-01-03]
    auto array1 = dest_column->get(0).get_array();
    ASSERT_EQ(array1.size(), 3);

    // Verify test case 2: should be NULL
    ASSERT_TRUE(dest_column->is_null(1));

    // Verify test case 3: should be NULL
    ASSERT_TRUE(dest_column->is_null(2));

    // Verify test case 4: should be empty array
    auto array4 = dest_column->get(3).get_array();
    ASSERT_EQ(array4.size(), 0);

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_date_with_week_quarter_units) {
    // Test DATE type with WEEK and QUARTER units
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATE), TypeDescriptor::from_logical_type(TYPE_DATE),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATE)));

    // Test WEEK unit
    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("week"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 to 2020-01-22, step 1 week
    start_column->append_datum(DateValue::create(2020, 1, 1));
    stop_column->append_datum(DateValue::create(2020, 1, 22));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATE>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate [2020-01-01, 2020-01-08, 2020-01-15, 2020-01-22]
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 4);
    EXPECT_EQ(DateValue::create(2020, 1, 1), array[0].get_date());
    EXPECT_EQ(DateValue::create(2020, 1, 8), array[1].get_date());
    EXPECT_EQ(DateValue::create(2020, 1, 15), array[2].get_date());
    EXPECT_EQ(DateValue::create(2020, 1, 22), array[3].get_date());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATE>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_datetime_with_year_unit) {
    // Test DATETIME type with YEAR unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_DATETIME),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATETIME)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("year"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 10:30:45 to 2023-01-01 10:30:45, step 1 year
    start_column->append_datum(TimestampValue::create(2020, 1, 1, 10, 30, 45));
    stop_column->append_datum(TimestampValue::create(2023, 1, 1, 10, 30, 45));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATETIME>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate 4 timestamps
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 4);
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 10, 30, 45), array[0].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2021, 1, 1, 10, 30, 45), array[1].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2022, 1, 1, 10, 30, 45), array[2].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2023, 1, 1, 10, 30, 45), array[3].get_timestamp());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_datetime_with_month_unit) {
    // Test DATETIME type with MONTH unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_DATETIME),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATETIME)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("month"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-15 08:20:30 to 2020-04-15 08:20:30, step 1 month
    start_column->append_datum(TimestampValue::create(2020, 1, 15, 8, 20, 30));
    stop_column->append_datum(TimestampValue::create(2020, 4, 15, 8, 20, 30));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATETIME>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate 4 timestamps
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 4);
    EXPECT_EQ(TimestampValue::create(2020, 1, 15, 8, 20, 30), array[0].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 2, 15, 8, 20, 30), array[1].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 3, 15, 8, 20, 30), array[2].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 4, 15, 8, 20, 30), array[3].get_timestamp());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_datetime_with_day_unit) {
    // Test DATETIME type with DAY unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_DATETIME),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATETIME)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("day"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 12:00:00 to 2020-01-03 12:00:00, step 1 day
    start_column->append_datum(TimestampValue::create(2020, 1, 1, 12, 0, 0));
    stop_column->append_datum(TimestampValue::create(2020, 1, 3, 12, 0, 0));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATETIME>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate 3 timestamps
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 3);
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 12, 0, 0), array[0].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 2, 12, 0, 0), array[1].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 3, 12, 0, 0), array[2].get_timestamp());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_datetime_with_week_unit) {
    // Test DATETIME type with WEEK unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_DATETIME),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATETIME)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("week"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 09:00:00 to 2020-01-22 09:00:00, step 1 week
    start_column->append_datum(TimestampValue::create(2020, 1, 1, 9, 0, 0));
    stop_column->append_datum(TimestampValue::create(2020, 1, 22, 9, 0, 0));
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATETIME>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate 4 timestamps (every 7 days)
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 4);
    EXPECT_EQ(TimestampValue::create(2020, 1, 1, 9, 0, 0), array[0].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 8, 9, 0, 0), array[1].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 15, 9, 0, 0), array[2].get_timestamp());
    EXPECT_EQ(TimestampValue::create(2020, 1, 22, 9, 0, 0), array[3].get_timestamp());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_datetime_with_millisecond_unit) {
    // Test DATETIME type with MILLISECOND unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_DATETIME),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATETIME)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("millisecond"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 10:00:00.000 to 2020-01-01 10:00:00.003, step 1 millisecond
    // Note: TimestampValue stores microseconds, so 1ms = 1000us
    auto start_ts = TimestampValue::create(2020, 1, 1, 10, 0, 0);
    auto stop_ts = TimestampValue::create(2020, 1, 1, 10, 0, 0);
    stop_ts.set_timestamp(stop_ts.timestamp() + 3000); // Add 3 milliseconds (3000 microseconds)

    start_column->append_datum(start_ts);
    stop_column->append_datum(stop_ts);
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATETIME>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate 4 timestamps (0ms, 1ms, 2ms, 3ms)
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 4);

    auto expected_ts = TimestampValue::create(2020, 1, 1, 10, 0, 0);
    EXPECT_EQ(expected_ts.timestamp(), array[0].get_timestamp().timestamp());

    expected_ts.set_timestamp(expected_ts.timestamp() + 1000);
    EXPECT_EQ(expected_ts.timestamp(), array[1].get_timestamp().timestamp());

    expected_ts.set_timestamp(expected_ts.timestamp() + 1000);
    EXPECT_EQ(expected_ts.timestamp(), array[2].get_timestamp().timestamp());

    expected_ts.set_timestamp(expected_ts.timestamp() + 1000);
    EXPECT_EQ(expected_ts.timestamp(), array[3].get_timestamp().timestamp());

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

TEST_F(ArrayFunctionsTest, array_generate_datetime_with_microsecond_unit) {
    // Test DATETIME type with MICROSECOND unit
    std::vector<FunctionContext::TypeDesc> arg_types = {
            TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_DATETIME),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto ctx = FunctionContext::create_test_context(
            std::move(arg_types), TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_DATETIME)));

    auto unit_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("microsecond"), 1);
    ctx->set_constant_columns({nullptr, nullptr, nullptr, unit_column});

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::prepare(ctx, FunctionContext::FRAGMENT_LOCAL).ok());

    auto start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    auto step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    // Test case: 2020-01-01 10:00:00.000000 to 2020-01-01 10:00:00.000005, step 1 microsecond
    auto start_ts = TimestampValue::create(2020, 1, 1, 10, 0, 0);
    auto stop_ts = TimestampValue::create(2020, 1, 1, 10, 0, 0);
    stop_ts.set_timestamp(stop_ts.timestamp() + 5); // Add 5 microseconds

    start_column->append_datum(start_ts);
    stop_column->append_datum(stop_ts);
    step_column->append_datum(Datum((int32_t)1));

    auto dest_column =
            ArrayGenerate<TYPE_DATETIME>::process(ctx, {start_column, stop_column, step_column, unit_column}).value();

    ASSERT_EQ(dest_column->size(), 1);

    // Verify: should generate 6 timestamps (0us, 1us, 2us, 3us, 4us, 5us)
    auto array = dest_column->get(0).get_array();
    ASSERT_EQ(array.size(), 6);

    auto expected_ts = TimestampValue::create(2020, 1, 1, 10, 0, 0);
    for (int i = 0; i < 6; i++) {
        EXPECT_EQ(expected_ts.timestamp(), array[i].get_timestamp().timestamp());
        expected_ts.set_timestamp(expected_ts.timestamp() + 1);
    }

    ASSERT_TRUE(ArrayGenerate<TYPE_DATETIME>::close(ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    delete ctx;
}

} // namespace starrocks
