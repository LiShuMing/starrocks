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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <random>

#include "column/array_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/function_helper.h"
#include "exprs/mock_vectorized_expr.h"
#include "exprs/string_functions.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "types/large_int_value.h"

namespace starrocks {

PARALLEL_TEST(VecStringFunctionsTest, moneyFormatDouble) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    double moneys[] = {1234.456, 1234.45, 1234.4, 1234.454};
    std::string results[] = {"1,234.46", "1,234.45", "1,234.40", "1,234.45"};

    Columns columns;
    auto money = DoubleColumn::create();

    for (double i : moneys) money->append(i);

    columns.emplace_back(money);
    ColumnPtr result = StringFunctions::money_format_double(ctx.get(), columns).value();
    auto v = ColumnHelper::as_raw_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(moneys) / sizeof(moneys[0]); ++i) ASSERT_EQ(results[i], v->get_data()[i].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, moneyFormatBigInt) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    int64_t moneys[] = {123456, -123456, 9223372036854775807};
    std::string results[] = {"123,456.00", "-123,456.00", "9,223,372,036,854,775,807.00"};

    Columns columns;
    auto money = Int64Column::create();

    for (long i : moneys) money->append(i);

    columns.emplace_back(money);
    ColumnPtr result = StringFunctions::money_format_bigint(ctx.get(), columns).value();
    auto v = ColumnHelper::as_raw_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(moneys) / sizeof(moneys[0]); ++i) ASSERT_EQ(results[i], v->get_data()[i].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, moneyFormatLargeInt) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    std::string str[] = {"170141183460469231731617303715884105727", "170141183460469231731687303715884105727",
                         "170141183460469231731687303715884105723"};
    __int128 moneys[sizeof(str) / sizeof(str[0])];
    for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
        std::stringstream ss;
        ss << str[i];
        ss >> moneys[i];
    }
    std::string results[] = {"170,141,183,460,469,231,731,617,303,715,884,105,727.00",
                             "170,141,183,460,469,231,731,687,303,715,884,105,727.00",
                             "170,141,183,460,469,231,731,687,303,715,884,105,723.00"};

    Columns columns;
    auto money = Int128Column::create();

    for (__int128 i : moneys) {
        money->append(i);
    }

    columns.emplace_back(money);
    ColumnPtr result = StringFunctions::money_format_largeint(ctx.get(), columns).value();
    auto v = ColumnHelper::as_raw_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(moneys) / sizeof(moneys[0]); ++i) ASSERT_EQ(results[i], v->get_data()[i].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, moneyFormatDecimalV2Value) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    std::string str[] = {"3333333333.2222222222", "-740740740.71604938271975308642"};
    DecimalV2Value moneys[sizeof(str) / sizeof(str[0])];
    for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
        moneys[i] = DecimalV2Value(str[i]);
    }
    std::string results[] = {"3,333,333,333.22", "-740,740,740.72"};

    Columns columns;
    auto money = DecimalColumn::create();

    for (auto i : moneys) {
        money->append(i);
    }

    columns.emplace_back(money);
    ColumnPtr result = StringFunctions::money_format_decimalv2val(ctx.get(), columns).value();
    auto v = ColumnHelper::as_raw_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(moneys) / sizeof(moneys[0]); ++i) ASSERT_EQ(results[i], v->get_data()[i].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, parseUrlNullable) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto data = BinaryColumn::create();
    auto null = NullColumn::create();

    std::string strs[] = {"http://cccccc:password@hostname/dsfsf?vdv=value#xcvxv",
                          "http://werwrw:sdf@sdfsceesvdsdvs/ccvwfewf?cvx=value#sdfs",
                          "http://vdvsv:df23@hostname/path?cvxvv=value#dsfs"};

    for (auto& i : strs) {
        str->append(i);
    }

    data->append("PATH");
    data->append("HOST");
    data->append("PROTOCOL");
    null->append(0);
    null->append(0);
    null->append(1);

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(data, null));

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    auto result = StringFunctions::parse_url(context, columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    ASSERT_EQ("/dsfsf", v->get_data()[0].to_string());
    ASSERT_EQ("sdfsceesvdsdvs", v->get_data()[1].to_string());
    ASSERT_TRUE(result->is_null(2));

    ASSERT_TRUE(StringFunctions::parse_url_close(context,
                                                 FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, parseUrlOnlyNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    ColumnPtr part = ColumnHelper::create_const_null_column(1);

    std::string strs[] = {"http://cccccc:password@hostname/dsfsf?vdv=value#xcvxv",
                          "http://werwrw:sdf@hostname/path?cvx=value#sdfs",
                          "http://vdvsv:df23@hostname/path?cvxvv=value#dsfs"};

    for (auto& i : strs) {
        str->append(i);
    }

    columns.emplace_back(str);
    columns.emplace_back(part);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    auto result = StringFunctions::parse_url(context, columns).value();

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_TRUE(result->is_null(i));
    }

    ASSERT_TRUE(StringFunctions::parse_url_close(context,
                                                 FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, parseUrlForConst) {
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto part = ColumnHelper::create_const_column<TYPE_VARCHAR>("AUTHORITY", 1);

        std::string strs[] = {"http://username:password@hostname/path?arg=value#anchor",
                              "http://starrockssss:apache/csdwwww?arg=value#anchor",
                              "http://wobushinidehao:kjkljq/wfefefe?arg=value#anchor"};

        std::string res[] = {"username:password@hostname", "starrockssss:apache", "wobushinidehao:kjkljq"};

        for (auto& i : strs) {
            str->append(i);
        }

        columns.emplace_back(str);
        columns.emplace_back(part);

        context->set_constant_columns(columns);

        ASSERT_TRUE(
                StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

        auto result = StringFunctions::parse_url(context, columns).value();
        auto v = ColumnHelper::as_column<BinaryColumn>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i].to_string());
        }

        ASSERT_TRUE(StringFunctions::parse_url_close(
                            context, FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto part = ColumnHelper::create_const_column<TYPE_VARCHAR>("PATH", 1);

        std::string strs[] = {"http://useraadfname:password@hostname/path?arg=value#anchor",
                              "http://starrockssxxxss:apache/csdwwww?arg=value#anchor",
                              "http://wobushxinidehao:kjksljq/wfefefe?arg=value#anchor"};

        std::string res[] = {"/path", "/csdwwww", "/wfefefe"};

        for (auto& i : strs) {
            str->append(i);
        }

        columns.emplace_back(str);
        columns.emplace_back(part);

        context->set_constant_columns(columns);

        ASSERT_TRUE(
                StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

        auto result = StringFunctions::parse_url(context, columns).value();
        auto v = ColumnHelper::as_column<BinaryColumn>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i].to_string());
        }

        ASSERT_TRUE(StringFunctions::parse_url_close(
                            context, FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, parseUrl) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto part = BinaryColumn::create();

    std::string strs[] = {"http://username:password@hostname/path?arg=value#anchor"};

    std::string parts[] = {"AUTHORITY", "FILE", "HOST", "PATH", "QUERY", "REF", "USERINFO", "PROTOCOL"};

    std::string res[] = {"username:password@hostname",
                         "/path?arg=value",
                         "hostname",
                         "/path",
                         "arg=value",
                         "anchor",
                         "username:password",
                         "http"};

    for (auto& i : parts) {
        str->append(strs[0]);
        part->append(i);
    }

    columns.emplace_back(str);
    columns.emplace_back(part);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    auto result = StringFunctions::parse_url(context, columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::parse_url_close(context,
                                                 FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, hex_intTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto ints = Int64Column::create();

    int64_t values[] = {21, 16, 256, 514};
    std::string strs[] = {"15", "10", "100", "202"};

    for (long value : values) {
        ints->append(value);
    }

    columns.emplace_back(ints);

    ColumnPtr result = StringFunctions::hex_int(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(strs[j], v->get_data()[j].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, hex_stringTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto ints = BinaryColumn::create();

    std::string values[] = {"21", "16", "256", "514"};
    std::string strs[] = {"3231", "3136", "323536", "353134"};

    for (auto& value : values) {
        ints->append(value);
    }

    columns.emplace_back(ints);

    ColumnPtr result = StringFunctions::hex_string(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(strs[j], v->get_data()[j].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, unhexTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto ints = BinaryColumn::create();

    std::string strs[] = {"21", "16", "256", "514"};
    std::string values[] = {"3231", "3136", "323536", "353134"};

    for (auto& value : values) {
        ints->append(value);
    }

    columns.emplace_back(ints);

    ColumnPtr result = StringFunctions::unhex(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(strs[j], v->get_data()[j].to_string());
    }
}

static void test_left_and_right_not_const(
        std::vector<std::tuple<std::string, int, std::string, std::string>> const& cases) {
    // left_not_const and right_not_const
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
    Columns columns;
    auto str_col = BinaryColumn::create();
    auto len_col = Int32Column::create();
    for (auto& c : cases) {
        auto s = std::get<0>(c);
        auto len = std::get<1>(c);
        str_col->append(Slice(s));
        len_col->append(len);
    }
    columns.emplace_back(str_col);
    columns.emplace_back(len_col);
    ColumnPtr left_result = StringFunctions::left(context.get(), columns).value();
    ColumnPtr right_result = StringFunctions::right(context.get(), columns).value();
    auto* binary_left_result = down_cast<const BinaryColumn*>(left_result.get());
    auto* binary_right_result = down_cast<const BinaryColumn*>(right_result.get());
    ASSERT_TRUE(binary_left_result != nullptr);
    ASSERT_TRUE(binary_right_result != nullptr);
    const auto size = cases.size();
    ASSERT_TRUE(binary_right_result != nullptr);
    ASSERT_EQ(binary_left_result->size(), size);
    ASSERT_EQ(binary_right_result->size(), size);
    auto state = std::make_unique<SubstrState>();
    for (auto i = 0; i < size; ++i) {
        auto left_expect = std::get<2>(cases[i]);
        auto right_expect = std::get<3>(cases[i]);
        auto left_actual = binary_left_result->get_slice(i).to_string();
        auto right_actual = binary_right_result->get_slice(i).to_string();
        ASSERT_EQ(left_actual, left_expect);
        ASSERT_EQ(right_actual, right_expect);
    }

    // left_const and right_const
    for (auto i = 0; i < size; ++i) {
        auto [s, len, left_expect, right_expect] = cases[i];
        str_col->resize(0);
        len_col->resize(0);
        str_col->append(Slice(s));
        len_col->append(len);
        columns.resize(0);
        columns.emplace_back(str_col);
        columns.emplace_back(ConstColumn::create(len_col, 1));

        auto substr_state = std::make_unique<SubstrState>();
        context->set_function_state(FunctionContext::FRAGMENT_LOCAL, substr_state.get());
        substr_state->is_const = true;
        substr_state->pos = 1;
        substr_state->len = len;
        left_result = StringFunctions::left(context.get(), columns).value();
        substr_state->pos = -len;
        right_result = StringFunctions::right(context.get(), columns).value();
        binary_left_result = down_cast<const BinaryColumn*>(left_result.get());
        binary_right_result = down_cast<const BinaryColumn*>(right_result.get());
        ASSERT_TRUE(binary_left_result != nullptr);
        ASSERT_TRUE(binary_right_result != nullptr);
        ASSERT_EQ(binary_left_result->size(), 1);
        ASSERT_EQ(binary_right_result->size(), 1);
        ASSERT_EQ(binary_left_result->get_slice(0).to_string(), left_expect);
        ASSERT_EQ(binary_right_result->get_slice(0).to_string(), right_expect);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, leftAndRightNotConstASCIITest) {
    std::vector<std::tuple<std::string, int, std::string, std::string>> cases{
            {"", 0, "", ""},
            {"", 1, "", ""},
            {"", -1, "", ""},
            {"", INT_MAX, "", ""},
            {"", INT_MIN, "", ""},
            {"a", 0, "", ""},
            {"a", 1, "a", "a"},
            {"a", 10, "a", "a"},
            {"a", INT_MAX, "a", "a"},
            {"a", -1, "", ""},
            {"a", INT_MIN, "", ""},
            {"All fingers are thumbs", 0, "", ""},
            {"All fingers are thumbs", 1, "A", "s"},
            {"All fingers are thumbs", 10, "All finger", "are thumbs"},
            {"All fingers are thumbs", 22, "All fingers are thumbs", "All fingers are thumbs"},
            {"All fingers are thumbs", 23, "All fingers are thumbs", "All fingers are thumbs"},
            {"All fingers are thumbs", 100, "All fingers are thumbs", "All fingers are thumbs"},
            {"All fingers are thumbs", INT_MAX, "All fingers are thumbs", "All fingers are thumbs"},
            {"All fingers are thumbs", -1, "", ""},
            {"All fingers are thumbs", -7, "", ""},
            {"All fingers are thumbs", INT_MIN, "", ""},
    };
    test_left_and_right_not_const(cases);
}
PARALLEL_TEST(VecStringFunctionsTest, leftAndRightNotConstUtf8Test) {
    std::vector<std::tuple<std::string, int, std::string, std::string>> cases{
            {"", 0, "", ""},
            {"", 1, "", ""},
            {"", -1, "", ""},
            {"", INT_MAX, "", ""},
            {"", INT_MIN, "", ""},
            {"a", 0, "", ""},
            {"a", 1, "a", "a"},
            {"a", 10, "a", "a"},
            {"a", INT_MAX, "a", "a"},
            {"a", -1, "", ""},
            {"a", INT_MIN, "", ""},
            {"三十年众生牛马，六十年诸佛龙象", 0, "", ""},
            {"三十年众生牛马，六十年诸佛龙象", 1, "三", "象"},
            {"三十年众生牛马，六十年诸佛龙象", 2, "三十", "龙象"},
            {"三十年众生牛马，六十年诸佛龙象", 7, "三十年众生牛马", "六十年诸佛龙象"},
            {"三十年众生牛马，六十年诸佛龙象", 16, "三十年众生牛马，六十年诸佛龙象", "三十年众生牛马，六十年诸佛龙象"},
            {"三十年众生牛马，六十年诸佛龙象", 20, "三十年众生牛马，六十年诸佛龙象", "三十年众生牛马，六十年诸佛龙象"},
            {"三十年众生牛马，六十年诸佛龙象", INT_MAX, "三十年众生牛马，六十年诸佛龙象",
             "三十年众生牛马，六十年诸佛龙象"},
            {"三十年众生牛马，六十年诸佛龙象", -1, "", ""},
            {"三十年众生牛马，六十年诸佛龙象", INT_MIN, "", ""},
            {"a三b十c年d众e生f牛g马", 0, "", ""},
            {"a三b十c年d众e生f牛g马", 1, "a", "马"},
            {"a三b十c年d众e生f牛g马", 7, "a三b十c年d", "众e生f牛g马"},
            {"a三b十c年d众e生f牛g马", 14, "a三b十c年d众e生f牛g马", "a三b十c年d众e生f牛g马"},
            {"a三b十c年d众e生f牛g马", 100, "a三b十c年d众e生f牛g马", "a三b十c年d众e生f牛g马"},
            {"a三b十c年d众e生f牛g马", -1, "", ""},
            {"a三b十c年d众e生f牛g马", -111, "", ""},
            {"a三b十c年d众e生f牛g马", INT_MAX, "a三b十c年d众e生f牛g马", "a三b十c年d众e生f牛g马"},
    };
    test_left_and_right_not_const(cases);
}

static void test_left_and_right_const(
        const ColumnPtr& str_col,
        std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string>>> const& cases) {
    for (auto& c : cases) {
        auto [len, left_expect, right_expect] = c;
        Columns columns;
        auto len_col = Int32Column::create();
        len_col->append(len);
        columns.emplace_back(str_col);
        columns.emplace_back(ConstColumn::create(len_col, 1));
        auto substr_state = std::make_unique<SubstrState>();
        std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
        context->set_function_state(FunctionContext::FRAGMENT_LOCAL, substr_state.get());
        substr_state->is_const = true;
        substr_state->pos = 1;
        substr_state->len = len;
        auto left_result = StringFunctions::left(context.get(), columns).value();
        auto right_result = StringFunctions::right(context.get(), columns).value();
        auto binary_left_result = down_cast<const BinaryColumn*>(left_result.get());
        auto binary_right_result = down_cast<const BinaryColumn*>(right_result.get());
        ASSERT_TRUE(binary_left_result != nullptr);
        ASSERT_TRUE(binary_right_result != nullptr);
        const auto size = str_col->size();
        ASSERT_EQ(binary_left_result->size(), size);
        ASSERT_EQ(binary_right_result->size(), size);
        for (auto i = 0; i < size; ++i) {
            ASSERT_EQ(binary_left_result->get_slice(i).to_string(), left_expect[i]);
            ASSERT_EQ(binary_right_result->get_slice(i).to_string(), right_expect[i]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, leftAndRightConstASCIITest) {
    auto str_col = BinaryColumn::create();
    str_col->append("");
    str_col->append("a");
    str_col->append("ABCDEFG_HIJKLMN");

    std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string>>> cases = {
            {0, {"", "", ""}, {"", "", ""}},
            {1, {"", "a", "A"}, {"", "a", "N"}},
            {2, {"", "a", "AB"}, {"", "a", "MN"}},
            {15, {"", "a", "ABCDEFG_HIJKLMN"}, {"", "a", "ABCDEFG_HIJKLMN"}},
            {16, {"", "a", "ABCDEFG_HIJKLMN"}, {"", "a", "ABCDEFG_HIJKLMN"}},
            {INT_MAX, {"", "a", "ABCDEFG_HIJKLMN"}, {"", "a", "ABCDEFG_HIJKLMN"}},
            {-1, {"", "", ""}, {"", "", ""}},
            {-11, {"", "", ""}, {"", "", ""}},
            {-111, {"", "", ""}, {"", "", ""}},
            {INT_MIN, {"", "", ""}, {"", "", ""}},
    };
    test_left_and_right_const(str_col, cases);
}

PARALLEL_TEST(VecStringFunctionsTest, leftAndRightConstUtf8Test) {
    auto str_col = BinaryColumn::create();
    str_col->append("");
    str_col->append("a");
    str_col->append("三十年众生牛马，六十年诸佛龙象");
    str_col->append("a三b十c年d众e生f牛g马");

    std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string>>> cases = {
            {0, {"", "", "", ""}, {"", "", "", ""}},
            {1, {"", "a", "三", "a"}, {"", "a", "象", "马"}},
            {2, {"", "a", "三十", "a三"}, {"", "a", "龙象", "g马"}},
            {14,
             {"", "a", "三十年众生牛马，六十年诸佛龙", "a三b十c年d众e生f牛g马"},
             {"", "a", "十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {15,
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"},
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {16,
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"},
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {100,
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"},
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {INT_MAX,
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"},
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {-1, {"", "", "", ""}, {"", "", "", ""}},
            {-11, {"", "", "", ""}, {"", "", "", ""}},
            {-111, {"", "", "", ""}, {"", "", "", ""}},
            {INT_MIN, {"", "", "", ""}, {"", "", "", ""}},
    };
    test_left_and_right_const(str_col, cases);
}

static void test_substr_not_const(std::vector<std::tuple<std::string, int, int, std::string>>& cases) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(cases.begin(), cases.end(), gen);
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
    auto str_col = BinaryColumn::create();
    auto off_col = Int32Column::create();
    auto len_col = Int32Column::create();
    for (auto& c : cases) {
        str_col->append(Slice(std::get<0>(c)));
        off_col->append(std::get<1>(c));
        len_col->append(std::get<2>(c));
    }
    Columns columns{str_col, off_col, len_col};
    auto result = StringFunctions::substring(context.get(), columns).value();
    auto* binary_result = down_cast<const BinaryColumn*>(result.get());
    const auto size = cases.size();
    ASSERT_TRUE(binary_result != nullptr);
    ASSERT_EQ(binary_result->size(), size);
    for (auto i = 0; i < size; ++i) {
        ASSERT_EQ(binary_result->get_slice(i).to_string(), std::get<3>(cases[i]));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, substrNotConstASCIITest) {
    ColumnPtr str_col = BinaryColumn::create();
    std::string ascii_1_9 = "123456789";
    std::vector<std::tuple<std::string, int, int, std::string>> cases = {
            {"", 0, 1, ""},
            {"", 1, 1, ""},
            {"", -1, 1, ""},
            {"", 1, -1, ""},
            {"", INT_MAX, INT_MIN, ""},
            {"", INT_MIN, INT_MAX, ""},
            {"", INT_MAX, INT_MAX, ""},
            {"", INT_MIN, INT_MIN, ""},
            {"a", 0, 1, ""},
            {"a", 1, 1, "a"},
            {"a", 1, 2, "a"},
            {"a", 1, INT_MAX, "a"},
            {"a", 1, INT_MIN, ""},
            {"a", -1, 0, ""},
            {"a", -1, 1, "a"},
            {"a", -1, 2, "a"},
            {"a", -1, -1, ""},
            {"a", -1, INT_MIN, ""},
            {"a", -1, INT_MAX, "a"},
            {ascii_1_9, -1, INT_MIN, ""},
            {ascii_1_9, -1, -1, ""},
            {ascii_1_9, -1, 0, ""},
            {ascii_1_9, -1, 1, "9"},
            {ascii_1_9, -1, INT_MAX, "9"},
            {ascii_1_9, 0, INT_MIN, ""},
            {ascii_1_9, 0, -1, ""},
            {ascii_1_9, 0, 0, ""},
            {ascii_1_9, 0, 1, ""},
            {ascii_1_9, 0, INT_MAX, ""},
            {ascii_1_9, 1, INT_MIN, ""},
            {ascii_1_9, 1, -1, ""},
            {ascii_1_9, 1, 0, ""},
            {ascii_1_9, 1, 1, "1"},
            {ascii_1_9, 1, INT_MAX, ascii_1_9},
            {ascii_1_9, 5, 1, "5"},
            {ascii_1_9, 5, 5, "56789"},
            {ascii_1_9, 5, 6, "56789"},
            {ascii_1_9, 5, INT_MAX, "56789"},
            {ascii_1_9, -4, 1, "6"},
            {ascii_1_9, -4, 3, "678"},
            {ascii_1_9, -4, 4, "6789"},
            {ascii_1_9, -4, 5, "6789"},
            {ascii_1_9, -4, INT_MAX, "6789"},
            {ascii_1_9, -4, INT_MAX, "6789"},
            {ascii_1_9, -9, INT_MIN, ""},
            {ascii_1_9, -9, -1, ""},
            {ascii_1_9, -9, 0, ""},
            {ascii_1_9, -9, 1, "1"},
            {ascii_1_9, -9, 9, ascii_1_9},
            {ascii_1_9, -9, INT_MAX, ascii_1_9},
            {ascii_1_9, -10, INT_MIN, ""},
            {ascii_1_9, -10, -1, ""},
            {ascii_1_9, -10, 0, ""},
            {ascii_1_9, -10, 1, ""},
            {ascii_1_9, -10, 9, ""},
            {ascii_1_9, -10, INT_MAX, ""},
            {ascii_1_9, 9, INT_MIN, ""},
            {ascii_1_9, 9, -1, ""},
            {ascii_1_9, 9, 0, ""},
            {ascii_1_9, 9, 1, "9"},
            {ascii_1_9, 9, 9, "9"},
            {ascii_1_9, 9, INT_MAX, "9"},
            {ascii_1_9, 10, INT_MIN, ""},
            {ascii_1_9, 10, -1, ""},
            {ascii_1_9, 10, 0, ""},
            {ascii_1_9, 10, 1, ""},
            {ascii_1_9, 10, 9, ""},
            {ascii_1_9, 10, INT_MAX, ""},
    };
    test_substr_not_const(cases);
}

PARALLEL_TEST(VecStringFunctionsTest, substrNotConstUtf8Test) {
    ColumnPtr str_col = BinaryColumn::create();
    std::string zh_1_9 = "壹贰叁肆伍陆柒捌玖";
    std::vector<std::tuple<std::string, int, int, std::string>> cases = {
            {"", 0, 1, ""},
            {"", 1, 1, ""},
            {"", -1, 1, ""},
            {"", 1, -1, ""},
            {"", INT_MAX, INT_MIN, ""},
            {"", INT_MIN, INT_MAX, ""},
            {"", INT_MAX, INT_MAX, ""},
            {"", INT_MIN, INT_MIN, ""},
            {"a", 0, 1, ""},
            {"a", 1, 1, "a"},
            {"a", 1, 2, "a"},
            {"a", 1, INT_MAX, "a"},
            {"a", 1, INT_MIN, ""},
            {"a", -1, 0, ""},
            {"a", -1, 1, "a"},
            {"a", -1, 2, "a"},
            {"a", -1, -1, ""},
            {"a", -1, INT_MIN, ""},
            {"a", -1, INT_MAX, "a"},
            {zh_1_9, -1, INT_MIN, ""},
            {zh_1_9, -1, -1, ""},
            {zh_1_9, -1, 0, ""},
            {zh_1_9, -1, 1, "玖"},
            {zh_1_9, -1, INT_MAX, "玖"},
            {zh_1_9, 0, INT_MIN, ""},
            {zh_1_9, 0, -1, ""},
            {zh_1_9, 0, 0, ""},
            {zh_1_9, 0, 1, ""},
            {zh_1_9, 0, INT_MAX, ""},
            {zh_1_9, 1, INT_MIN, ""},
            {zh_1_9, 1, -1, ""},
            {zh_1_9, 1, 0, ""},
            {zh_1_9, 1, 1, "壹"},
            {zh_1_9, 1, INT_MAX, zh_1_9},
            {zh_1_9, 5, 1, "伍"},
            {zh_1_9, 5, 5, "伍陆柒捌玖"},
            {zh_1_9, 5, 6, "伍陆柒捌玖"},
            {zh_1_9, 5, INT_MAX, "伍陆柒捌玖"},
            {zh_1_9, -4, 1, "陆"},
            {zh_1_9, -4, 3, "陆柒捌"},
            {zh_1_9, -4, 4, "陆柒捌玖"},
            {zh_1_9, -4, 5, "陆柒捌玖"},
            {zh_1_9, -4, INT_MAX, "陆柒捌玖"},
            {zh_1_9, -4, INT_MAX, "陆柒捌玖"},
            {zh_1_9, -9, INT_MIN, ""},
            {zh_1_9, -9, -1, ""},
            {zh_1_9, -9, 0, ""},
            {zh_1_9, -9, 1, "壹"},
            {zh_1_9, -9, 9, zh_1_9},
            {zh_1_9, -9, INT_MAX, zh_1_9},
            {zh_1_9, -10, INT_MIN, ""},
            {zh_1_9, -10, -1, ""},
            {zh_1_9, -10, 0, ""},
            {zh_1_9, -10, 1, ""},
            {zh_1_9, -10, 9, ""},
            {zh_1_9, -10, INT_MAX, ""},
            {zh_1_9, 9, INT_MIN, ""},
            {zh_1_9, 9, -1, ""},
            {zh_1_9, 9, 0, ""},
            {zh_1_9, 9, 1, "玖"},
            {zh_1_9, 9, 9, "玖"},
            {zh_1_9, 9, INT_MAX, "玖"},
            {zh_1_9, 10, INT_MIN, ""},
            {zh_1_9, 10, -1, ""},
            {zh_1_9, 10, 0, ""},
            {zh_1_9, 10, 1, ""},
            {zh_1_9, 10, 9, ""},
            {zh_1_9, 10, INT_MAX, ""},
    };
    test_substr_not_const(cases);
}

PARALLEL_TEST(VecStringFunctionsTest, strcmpTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto lhs = BinaryColumn::create();
    auto rhs = BinaryColumn::create();

    lhs->append("");
    rhs->append("");

    lhs->append("");
    rhs->append("text1");

    lhs->append("text2");
    rhs->append("");

    lhs->append("text1");
    rhs->append("text1");

    lhs->append("text1");
    rhs->append("text2");

    lhs->append("text2");
    rhs->append("text1");

    columns.emplace_back(lhs);
    columns.emplace_back(rhs);

    ColumnPtr result = StringFunctions::strcmp(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    ASSERT_EQ(6, result->size());
    ASSERT_EQ(0, v->get_data()[0]);
    ASSERT_EQ(-1, v->get_data()[1]);
    ASSERT_EQ(1, v->get_data()[2]);
    ASSERT_EQ(0, v->get_data()[3]);
    ASSERT_EQ(-1, v->get_data()[4]);
    ASSERT_EQ(1, v->get_data()[5]);
}

PARALLEL_TEST(VecStringFunctionsTest, strposTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();

    std::vector<std::string> haystacks = {"abc", "abcabc", "", "hello", "hello world", "hello", ""};
    std::vector<std::string> needles = {"b", "abc", "something", "world", "", "xyz", "anything"};

    std::vector<int64_t> expected = {2, 1, 0, 0, 1, 0, 0};

    for (size_t i = 0; i < haystacks.size(); ++i) {
        haystack->append(haystacks[i]);
        needle->append(needles[i]);
    }

    columns.emplace_back(haystack);
    columns.emplace_back(needle);

    ColumnPtr result = StringFunctions::strpos(ctx.get(), columns).value();
    ASSERT_EQ(haystacks.size(), result->size());

    auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);

    for (size_t i = 0; i < haystacks.size(); ++i) {
        ASSERT_EQ(expected[i], v->get_data()[i]) << "Failed for input: " << haystacks[i] << ", " << needles[i];
    }
}

PARALLEL_TEST(VecStringFunctionsTest, strposInstanceTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Test positive instance
    {
        Columns columns;
        auto haystack = BinaryColumn::create();
        auto needle = BinaryColumn::create();
        auto instance = Int32Column::create();

        std::vector<std::string> haystacks = {"abcabc", "abcabc", "hello hello world", "hello hello world"};
        std::vector<std::string> needles = {"abc", "abc", "hello", "hello"};
        std::vector<int32_t> instances = {1, 2, 1, 2};

        std::vector<int64_t> expected = {1, 4, 1, 7};

        for (size_t i = 0; i < haystacks.size(); ++i) {
            haystack->append(haystacks[i]);
            needle->append(needles[i]);
            instance->append(instances[i]);
        }

        columns.emplace_back(haystack);
        columns.emplace_back(needle);
        columns.emplace_back(instance);

        ColumnPtr result = StringFunctions::strpos_instance(ctx.get(), columns).value();
        ASSERT_EQ(haystacks.size(), result->size());

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);

        for (size_t i = 0; i < haystacks.size(); ++i) {
            ASSERT_EQ(expected[i], v->get_data()[i])
                    << "Failed for input: " << haystacks[i] << ", " << needles[i] << ", " << instances[i];
        }
    }

    // Test negative instance (search from end)
    {
        Columns columns;
        auto haystack = BinaryColumn::create();
        auto needle = BinaryColumn::create();
        auto instance = Int32Column::create();

        std::vector<std::string> haystacks = {"abcabc", "abcabc", "hello hello world"};
        std::vector<std::string> needles = {"abc", "abc", "hello"};
        std::vector<int32_t> instances = {-1, -2, -1};

        std::vector<int64_t> expected = {4, 1, 7};

        for (size_t i = 0; i < haystacks.size(); ++i) {
            haystack->append(haystacks[i]);
            needle->append(needles[i]);
            instance->append(instances[i]);
        }

        columns.emplace_back(haystack);
        columns.emplace_back(needle);
        columns.emplace_back(instance);

        ColumnPtr result = StringFunctions::strpos_instance(ctx.get(), columns).value();
        ASSERT_EQ(haystacks.size(), result->size());

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);

        for (size_t i = 0; i < haystacks.size(); ++i) {
            ASSERT_EQ(expected[i], v->get_data()[i])
                    << "Failed for input: " << haystacks[i] << ", " << needles[i] << ", " << instances[i];
        }
    }

    // Test NULL values
    {
        Columns columns;
        auto haystack = BinaryColumn::create();
        auto needle = BinaryColumn::create();
        auto instance = Int32Column::create();
        auto nulls = NullColumn::create();

        haystack->append("test");
        needle->append("e");
        instance->append(1);
        nulls->append(0);

        haystack->append("test");
        needle->append("e");
        instance->append(1);
        nulls->append(1);

        auto haystack_nullable = NullableColumn::create(haystack, NullColumn::create(*nulls));
        auto needle_nullable = NullableColumn::create(needle->clone(), NullColumn::create(*nulls));
        auto instance_nullable = NullableColumn::create(instance->clone(), NullColumn::create(*nulls));

        columns.emplace_back(haystack_nullable);
        columns.emplace_back(needle_nullable);
        columns.emplace_back(instance_nullable);

        ColumnPtr result = StringFunctions::strpos_instance(ctx.get(), columns).value();
        ASSERT_EQ(2, result->size());
        ASSERT_TRUE(result->is_nullable());

        auto nullable_result = down_cast<const NullableColumn*>(result.get());
        ASSERT_FALSE(nullable_result->is_null(0));
        ASSERT_TRUE(nullable_result->is_null(1));

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(nullable_result->data_column());
        ASSERT_EQ(2, v->get_data()[0]);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllPatternZero) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrCryE", "hitCdeciCsionCdlist", "hitCdecCisiCondlCist", "12342356"};
    std::string res[] = {"['bCd']", "['bCdrr']", "['hitCdeci','sionCdlist']", "['hitCdec','isiCondl']", "[]"};
    int indexs[] = {0, 0, 0, 0, 0};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append("([[:lower:]]+)C([[:lower:]]+)");
        index->append(indexs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllConstPatternZero) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("([[:lower:]]+)C([[:lower:]]+)", 1);
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisiondlist"};
    int indexs[] = {0, 0, 0, 1};

    std::string res[] = {"['bCd']", "['bCdrrry']", "['hitdeciCsiondlist']", "['hitdec']"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        index->append(indexs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllConstZero) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("([[:lower:]]+)C([[:lower:]]+)", 5);
    auto index = ColumnHelper::create_const_column<TYPE_BIGINT>(0, 5);

    std::string strs[] = {"AbCdE", "AbCdrrCryE", "hitCdeciCsionCdlist", "hitCdecCisiCondlCist", "12342356"};
    std::string res[] = {"['bCd']", "['bCdrr']", "['hitCdeci','sionCdlist']", "['hitCdec','isiCondl']", "[]"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllNullableGroupPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto null = NullColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitCdecCisiCondlCist"};
    int indexs[] = {1, 2, 1, 0};

    std::string res[] = {"NULL", "['drrry']", "NULL", "['hitCdec','isiCondl']"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append("([[:lower:]]+)C([[:lower:]]+)");
        null->append(i % 2 == 0);
        index->append(indexs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(NullableColumn::create(index, null));

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrCryE", "hitCdeciCsionCdlist", "hitCdecCisiCondlCist", "12342356"};
    std::string res[] = {"['b']", "['b']", "['hit','sion']", "['hit','isi']", "[]"};
    int indexs[] = {1, 1, 1, 1, 1};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append("([[:lower:]]+)C([[:lower:]]+)");
        index->append(indexs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllNullablePattern1) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisiondlist"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"['b']", "['drrry']", "['hitdeci']", "['isiondlist']"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append("([[:lower:]]+)C([[:lower:]]+)");
        index->append(indexs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    auto result = StringFunctions::regexp_extract_all(context, columns).value();
    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllNullablePattern2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto null = NullColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisioedlise"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"NULL", "['drrry']", "NULL", "['td','sio','s']"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append(i < 2 ? "([[:lower:]]+)C([[:lower:]]+)" : "(i)(.*?)(e)");
        null->append(i % 2 == 0);
        index->append(indexs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(pattern, null));
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllOnlyNullPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    ColumnPtr pattern = ColumnHelper::create_const_null_column(1);
    auto index = Int64Column::create();

    int length = 4;

    for (int i = 0; i < length; ++i) {
        str->append("test" + std::to_string(i));
        index->append(1);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();
    for (int i = 0; i < length; ++i) {
        ASSERT_TRUE(result->is_null(i));
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllConstPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("([[:lower:]]+)C([[:lower:]]+)", 1);
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisiondlist"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"['b']", "['drrry']", "['hitdeci']", "['isiondlist']"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        index->append(indexs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllConst) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("([[:lower:]]+)C([[:lower:]]+)", 5);
    auto index = ColumnHelper::create_const_column<TYPE_BIGINT>(2, 5);

    std::string strs[] = {"AbCdE", "AbCdrrCryE", "hitCdeciCsionCdlist", "hitCdecCisiCondlCist", "12342356"};
    std::string res[] = {"['d']", "['drr']", "['deci','dlist']", "['dec','ondl']", "[]"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, crc32Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    str->append("starrocks");
    str->append("STARROCKS");
    columns.emplace_back(str);

    ASSERT_TRUE(StringFunctions::crc32(ctx.get(), columns).ok());
    ColumnPtr result = StringFunctions::crc32(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
    ASSERT_EQ(static_cast<uint32_t>(2312449062), v->get_data()[0]);
    ASSERT_EQ(static_cast<uint32_t>(3440849609), v->get_data()[1]);
}

PARALLEL_TEST(VecStringFunctionsTest, regexpSplitTest) {
    // const pattern, const max_split - default_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("[ABC]", 1);

        std::string strs[] = {"oneAtwoBthreeC", "1A2B3C", "AABBCC"};
        std::string res[] = {"['one','two','three','']", "['1','2','3','']", "['','','','','','','']"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
        }

        columns.emplace_back(str);
        columns.emplace_back(pattern);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const pattern, const max_split - customized_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto null = NullColumn::create();
        auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("[ABC]", 1);
        auto max_split = ColumnHelper::create_const_column<TYPE_INT>(2, 1);

        std::string strs[] = {"oneAtwoBthreeC", "1A2B3C", "AABBCC", "AABBCC"};
        std::string res[] = {"['one','twoBthreeC']", "['1','2B3C']", "['','ABBCC']", "NULL"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            null->append(i == 3 ? 1 : 0);
        }

        columns.emplace_back(NullableColumn::create(str, null));
        columns.emplace_back(pattern);
        columns.emplace_back(max_split);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const pattern - default_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("[ABC]", 1);

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC",
                              "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};

        std::string res[] = {"['one','two','three','']", "['one','two','three','']", "['one','two','three','']",
                             "['one','two','three','']", "['one','two','three','']", "['one','two','three','']"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
        }

        columns.emplace_back(str);
        columns.emplace_back(pattern);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const pattern - customized_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto null = NullColumn::create();
        auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("[ABC]", 1);
        auto max_split = Int32Column::create();

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC",
                              "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        int max_splits[] = {-1, 0, 1, 2, 3, 4, 5};

        std::string res[] = {"['one','two','three','']",
                             "['one','two','three','']",
                             "['oneAtwoBthreeC']",
                             "['one','twoBthreeC']",
                             "['one','two','threeC']",
                             "['one','two','three','']",
                             "NULL"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            null->append(i == 6 ? 1 : 0);
            max_split->append(max_splits[i]);
        }

        columns.emplace_back(NullableColumn::create(str, null));
        columns.emplace_back(pattern);
        columns.emplace_back(max_split);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const max_split - default_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = BinaryColumn::create();

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        std::string patterns[] = {"[nwe]", "[ne]", "[123]"};
        std::string res[] = {"['o','','At','oBthr','','C']", "['o','','AtwoBthr','','C']", "['oneAtwoBthreeC']"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            pattern->append(patterns[i]);
        }

        columns.emplace_back(str);
        columns.emplace_back(pattern);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const max_split - customized_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = BinaryColumn::create();
        auto null = NullColumn::create();
        auto max_split = ColumnHelper::create_const_column<TYPE_INT>(4, 1);

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        std::string patterns[] = {"[nwe]", "[ne]", "[123]", "[123]"};
        std::string res[] = {"['o','','At','oBthreeC']", "['o','','AtwoBthr','eC']", "['oneAtwoBthreeC']", "NULL"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            pattern->append(patterns[i]);
            null->append(i == 3 ? 1 : 0);
        }

        columns.emplace_back(str);
        columns.emplace_back(NullableColumn::create(pattern, null));
        columns.emplace_back(max_split);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // none const - default_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = BinaryColumn::create();

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        std::string patterns[] = {"[nwe]", "[ne]", "[123]"};

        std::string res[] = {"['o','','At','oBthr','','C']", "['o','','AtwoBthr','','C']", "['oneAtwoBthreeC']"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            pattern->append(patterns[i]);
        }

        columns.emplace_back(str);
        columns.emplace_back(pattern);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // none const - customized_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = BinaryColumn::create();
        auto null = NullColumn::create();
        auto max_split = Int32Column::create();

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        std::string patterns[] = {"[nwe]", "[ne]", "[123]", "[123]"};
        int max_splits[] = {1, 2, 3, 4};

        std::string res[] = {"['oneAtwoBthreeC']", "['o','eAtwoBthreeC']", "['oneAtwoBthreeC']", "NULL"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            pattern->append(patterns[i]);
            null->append(i == 3 ? 1 : 0);
            max_split->append(max_splits[i]);
        }

        columns.emplace_back(str);
        columns.emplace_back(NullableColumn::create(pattern, null));
        columns.emplace_back(max_split);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpCountTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    {
        auto str_col = BinaryColumn::create();
        str_col->append("abc123def456");
        str_col->append("test.com test.net test.org");
        str_col->append("a b  c   d");
        str_col->append("ababababab");
        str_col->append("");

        // Create nullable column for testing NULL input
        auto null_col = NullColumn::create();
        for (int i = 0; i < 4; ++i) {
            null_col->append(0);
        }
        null_col->append(1);
        auto nullable_str_col = NullableColumn::create(str_col, null_col);

        // Test with constant regex pattern number regex
        auto pattern_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("[0-9]", 5);

        Columns columns = {nullable_str_col, pattern_col};
        context->set_constant_columns(columns);

        ASSERT_TRUE(
                StringFunctions::regexp_count_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        auto result = StringFunctions::regexp_count(context, columns).value();
        ASSERT_TRUE(StringFunctions::regexp_close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

        ASSERT_EQ(result->size(), 5);
        ASSERT_EQ(result->get(0).get_int64(), 6);
        ASSERT_EQ(result->get(1).get_int64(), 0);
        ASSERT_EQ(result->get(2).get_int64(), 0);
        ASSERT_EQ(result->get(3).get_int64(), 0);
        ASSERT_TRUE(result->is_null(4));
    }

    // Dynamic regex pattern test
    {
        auto str_col = BinaryColumn::create();
        str_col->append("abc123def456");
        str_col->append("abc");
        str_col->append("ABC");

        auto pattern_col = BinaryColumn::create();
        pattern_col->append("[a-z]"); // checks lowercase
        pattern_col->append("[A-Z]"); // checks uppercase
        pattern_col->append("[0-9]"); // checks number

        Columns columns = {str_col, pattern_col};

        auto result = StringFunctions::regexp_count(context, columns).value();

        ASSERT_EQ(result->size(), 3);
        ASSERT_EQ(result->get(0).get_int64(), 6);
        ASSERT_EQ(result->get(1).get_int64(), 0);
        ASSERT_EQ(result->get(2).get_int64(), 0);
    }

    // Special characters test
    {
        auto str_col = BinaryColumn::create();
        str_col->append("a,b,c,d,e");
        str_col->append("a.b.c.d.e");

        auto pattern_col = BinaryColumn::create();
        pattern_col->append(",");   // checks comma
        pattern_col->append("\\."); // checks dot

        Columns columns = {str_col, pattern_col};

        auto result = StringFunctions::regexp_count(context, columns).value();

        ASSERT_EQ(result->size(), 2);
        ASSERT_EQ(result->get(0).get_int64(), 4);
        ASSERT_EQ(result->get(1).get_int64(), 4);
    }

    // Empty and invalid pattern test
    {
        auto str_col = BinaryColumn::create();
        str_col->append("abc");
        str_col->append("abc");
        str_col->append("abc");

        auto pattern_col = BinaryColumn::create();
        pattern_col->append("");
        pattern_col->append("(a");
        pattern_col->append("a");

        auto null_col = NullColumn::create();
        null_col->append(0);
        null_col->append(0);
        null_col->append(1);
        auto nullable_pattern = NullableColumn::create(pattern_col, null_col);

        Columns columns = {str_col, nullable_pattern};

        auto result = StringFunctions::regexp_count(context, columns).value();
        ASSERT_EQ(result->size(), 3);
        ASSERT_TRUE(result->is_null(0));
        ASSERT_TRUE(result->is_null(1));
        ASSERT_TRUE(result->is_null(2));
    }

    {
        auto str_col = BinaryColumn::create();
        str_col->append("ababababab");
        str_col->append("aaaaaaaaaa");
        str_col->append("abababa");

        auto pattern_col = BinaryColumn::create();
        pattern_col->append("ab");
        pattern_col->append("a");
        pattern_col->append("aba");

        Columns columns = {str_col, pattern_col};

        auto result = StringFunctions::regexp_count(context, columns).value();

        ASSERT_EQ(result->size(), 3);
        ASSERT_EQ(result->get(0).get_int64(), 5);
        ASSERT_EQ(result->get(1).get_int64(), 10);
        ASSERT_EQ(result->get(2).get_int64(), 2);
    }

    // Unicode characters test
    {
        auto str_col = BinaryColumn::create();
        str_col->append("AbCdExCeF");
        str_col->append("1a 2b 14m");

        auto pattern_col = BinaryColumn::create();
        pattern_col->append("C");
        pattern_col->append("\\d+");

        Columns columns = {str_col, pattern_col};

        auto result = StringFunctions::regexp_count(context, columns).value();

        ASSERT_EQ(result->size(), 2);
        ASSERT_EQ(result->get(0).get_int64(), 2);
        ASSERT_EQ(result->get(1).get_int64(), 3);
    }
}
} // namespace starrocks
