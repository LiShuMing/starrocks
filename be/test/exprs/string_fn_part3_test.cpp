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

PARALLEL_TEST(VecStringFunctionsTest, startsWithTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto prefix = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "TEST");
        prefix->append(std::to_string(j % 10) + "T");
    }

    columns.emplace_back(str);
    columns.emplace_back(prefix);

    ColumnPtr result = StringFunctions::starts_with(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_TRUE(v->get_data()[k]);
        } else {
            ASSERT_FALSE(v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, startsWithNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto prefix = BinaryColumn::create();
    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "TEST");
        if (j > 10) {
            prefix->append(std::to_string(j));
            null->append(false);
        } else {
            prefix->append(std::to_string(j));
            null->append(true);
        }
    }

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(prefix, null));

    ColumnPtr result = StringFunctions::starts_with(ctx.get(), columns).value();

    ASSERT_EQ(20, result->size());
    ASSERT_TRUE(result->is_nullable());
    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int k = 0; k < 20; ++k) {
        if (k > 10) {
            ASSERT_TRUE(v->get_data()[k]);
        } else {
            ASSERT_TRUE(result->is_null(k));
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, endsWithNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto suffix = BinaryColumn::create();
    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("TEST" + std::to_string(j));
        if (j > 10) {
            suffix->append(std::to_string(j));
            null->append(false);
        } else {
            suffix->append(std::to_string(j));
            null->append(true);
        }
    }

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(suffix, null));

    ColumnPtr result = StringFunctions::ends_with(ctx.get(), columns).value();

    ASSERT_EQ(20, result->size());
    ASSERT_TRUE(result->is_nullable());
    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int k = 0; k < 20; ++k) {
        if (k > 10) {
            ASSERT_TRUE(v->get_data()[k]);
        } else {
            ASSERT_TRUE(result->is_null(k));
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, appendTrailingCharIfAbsentTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto pad = BinaryColumn::create();

    str->append("qwer");
    pad->append("r");

    str->append("qwe");
    pad->append("r");

    str->append("");
    pad->append("r");

    columns.emplace_back(str);
    columns.emplace_back(pad);

    ColumnPtr result = StringFunctions::append_trailing_char_if_absent(ctx.get(), columns).value();
    ASSERT_EQ(3, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    ASSERT_EQ("qwer", v->get_data()[0].to_string());
    ASSERT_EQ("qwer", v->get_data()[1].to_string());
    ASSERT_EQ("", v->get_data()[2].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, appendTrailingCharIfAbsentNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto pad = BinaryColumn::create();

    str->append("qwer");
    pad->append("rw");

    str->append("qwe");
    pad->append("er");

    columns.emplace_back(str);
    columns.emplace_back(pad);

    ColumnPtr result = StringFunctions::append_trailing_char_if_absent(ctx.get(), columns).value();
    ASSERT_EQ(2, result->size());

    ASSERT_TRUE(result->is_nullable());
    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));
}

PARALLEL_TEST(VecStringFunctionsTest, appendTrailingCharIfAbsentUTF8Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto pad = BinaryColumn::create();

    str->append("中国");
    pad->append("a");

    str->append("北京");
    pad->append("b");

    columns.emplace_back(str);
    columns.emplace_back(pad);

    ColumnPtr result = StringFunctions::append_trailing_char_if_absent(ctx.get(), columns).value();
    ASSERT_EQ(2, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    ASSERT_EQ("中国a", v->get_data()[0].to_string());
    ASSERT_EQ("北京b", v->get_data()[1].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, appendTrailingCharIfAbsentUTF8NullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto pad = BinaryColumn::create();

    str->append("中国");
    pad->append("国");

    str->append("北京");
    pad->append("京");

    columns.emplace_back(str);
    columns.emplace_back(pad);

    ColumnPtr result = StringFunctions::append_trailing_char_if_absent(ctx.get(), columns).value();
    ASSERT_EQ(2, result->size());

    ASSERT_TRUE(result->is_nullable());
    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));
}

PARALLEL_TEST(VecStringFunctionsTest, lengthTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::length(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_EQ(1, v->get_data()[k]);
        } else {
            ASSERT_EQ(2, v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, lengthChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append("中文" + std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::length(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_EQ(7, v->get_data()[k]);
        } else {
            ASSERT_EQ(8, v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, utf8LengthTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::utf8_length(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_EQ(1, v->get_data()[k]);
        } else {
            ASSERT_EQ(2, v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, utf8LengthChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append("中文" + std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::utf8_length(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_EQ(3, v->get_data()[k]);
        } else {
            ASSERT_EQ(4, v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, upperTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    std::unique_ptr<RuntimeState> runtime_state(new RuntimeState());
    ctx->set_runtime_state(runtime_state.get());

    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append("abcd" + std::to_string(j));
    }

    columns.emplace_back(str);

    ASSERT_TRUE(StringFunctions::upper_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ColumnPtr result = StringFunctions::upper(ctx.get(), columns).value();
    ASSERT_TRUE(StringFunctions::upper_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ("ABCD" + std::to_string(k), v->get_data()[k].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, caseToggleTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    std::unique_ptr<RuntimeState> runtime_state(new RuntimeState());
    ctx->set_runtime_state(runtime_state.get());
    Columns columns;
    auto src = BinaryColumn::create();
    src->append("");
    src->append("a");
    src->append("1");
    src->append("abcd_efg_higk_lmn_opq_rst_uvw_xyz");
    src->append("ABCD_EFG_HIGK_LMN_OPQ_RST_UVW_XYZ");
    src->append("AbCd_EfG_HiGk_LmN_oPq_RsT_UvW_xYz");
    std::string s;
    s.resize(255);
    for (int i = 0; i < 255; ++i) {
        s[i] = (char)i;
    }
    src->append(s);
    src->append("三aBcD十eFg年HiGk众生LmN牛马oPq六十年RsT诸uVw佛XyZ龙象");
    src->append(
            "φημὶγὰρἐγὼεἶναιτὸABCD_EFG_HIGK_LMNδίκαιονοὐκἄλλοτιOPQRST_"
            "UVWἢτὸτοῦκρείττονοςσυμφέρονXYZ");
    columns.emplace_back(src);

    ASSERT_TRUE(StringFunctions::upper_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto upper_dst = StringFunctions::upper(ctx.get(), columns).value();
    ASSERT_TRUE(StringFunctions::upper_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(StringFunctions::lower_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto lower_dst = StringFunctions::lower(ctx.get(), columns).value();
    ASSERT_TRUE(StringFunctions::lower_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto binary_upper_dst = down_cast<BinaryColumn*>(upper_dst->as_mutable_ptr().get());
    auto binary_lower_dst = down_cast<BinaryColumn*>(lower_dst->as_mutable_ptr().get());
    ASSERT_TRUE(binary_upper_dst != nullptr);
    ASSERT_TRUE(binary_lower_dst != nullptr);
    auto size = src->size();
    ASSERT_EQ(binary_upper_dst->size(), size);
    ASSERT_EQ(binary_lower_dst->size(), size);
    for (auto i = 0; i < size; ++i) {
        Slice origin = src->get_slice(i);
        Slice uc = binary_upper_dst->get_slice(i);
        Slice lc = binary_lower_dst->get_slice(i);
        std::string uc1 = origin.to_string();
        std::string lc1 = origin.to_string();
        std::transform(uc1.begin(), uc1.end(), uc1.begin(), [](char c) -> char { return std::toupper(c); });
        std::transform(lc1.begin(), lc1.end(), lc1.begin(), [](char c) -> char { return std::tolower(c); });
        ASSERT_EQ(uc.to_string(), uc1);
        ASSERT_EQ(lc.to_string(), lc1);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, asciiTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();

    str->append("qwer");
    str->append("qwe");
    str->append("");

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::ascii(ctx.get(), columns).value();
    ASSERT_EQ(3, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    ASSERT_EQ(static_cast<int32_t>('q'), v->get_data()[0]);
    ASSERT_EQ(static_cast<int32_t>('q'), v->get_data()[1]);
    ASSERT_EQ(0, v->get_data()[2]);
}

PARALLEL_TEST(VecStringFunctionsTest, charTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = Int32Column::create();

    str->append(65);
    str->append(66);
    str->append(97);
    str->append(98);
    str->append(33);
    str->append(126);

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::get_char(ctx.get(), columns).value();
    ASSERT_EQ(6, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    ASSERT_EQ("A", v->get_data()[0].to_string());
    ASSERT_EQ("B", v->get_data()[1].to_string());
    ASSERT_EQ("a", v->get_data()[2].to_string());
    ASSERT_EQ("b", v->get_data()[3].to_string());
    ASSERT_EQ("!", v->get_data()[4].to_string());
    ASSERT_EQ("~", v->get_data()[5].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, inetAtonInvalidIPv4Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto input_column = BinaryColumn::create();
    input_column->append("999.999.999.999");
    input_column->append("abc.def.ghi.jkl");
    input_column->append("192.168.1.1.1");
    input_column->append("192.168.1");
    input_column->append("");
    columns.emplace_back(input_column);

    auto result = StringFunctions::inet_aton(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));
    ASSERT_TRUE(result->is_null(2));
    ASSERT_TRUE(result->is_null(3));
    ASSERT_TRUE(result->is_null(4));
}

PARALLEL_TEST(VecStringFunctionsTest, inetAtonValidIPv4Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto input_column = BinaryColumn::create();
    input_column->append("192.168.1.1");
    input_column->append("0.0.0.0");
    input_column->append("255.255.255.255");
    columns.emplace_back(input_column);

    auto result = StringFunctions::inet_aton(ctx.get(), columns).value();

    auto res_column = ColumnHelper::cast_to<TYPE_BIGINT>(result);
    ASSERT_EQ(3232235777, res_column->get_data()[0]);
    ASSERT_EQ(0, res_column->get_data()[1]);
    ASSERT_EQ(4294967295, res_column->get_data()[2]);
}

PARALLEL_TEST(VecStringFunctionsTest, instrTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("abcd" + std::to_string(j));
        sub->append(std::to_string(j));
    }

    columns.emplace_back(str);
    columns.emplace_back(sub);

    ColumnPtr result = StringFunctions::instr(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        ASSERT_EQ(5, v->get_data()[j]);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, instrChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("中文字符" + std::to_string(j));
        sub->append(std::to_string(j));
    }

    columns.emplace_back(str);
    columns.emplace_back(sub);

    ColumnPtr result = StringFunctions::instr(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        ASSERT_EQ(5, v->get_data()[j]);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, locateNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();
    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("abcd" + std::to_string(j));
        sub->append(std::to_string(j));
        null->append(j % 2);
    }

    columns.emplace_back(NullableColumn::create(sub, null));
    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::locate(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());
    ASSERT_TRUE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int j = 0; j < 20; ++j) {
        if (j % 2) {
            ASSERT_TRUE(result->is_null(j));
        } else {
            ASSERT_EQ(5, v->get_data()[j]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, locatePosTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();
    auto pos = Int32Column::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "abcd" + std::to_string(j));
        sub->append(std::to_string(j));
        pos->append(4);
    }

    columns.emplace_back(sub);
    columns.emplace_back(str);
    columns.emplace_back(pos);

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        if (j < 10) {
            ASSERT_EQ(6, v->get_data()[j]);
        } else {
            ASSERT_EQ(7, v->get_data()[j]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, locatePosChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();
    auto pos = Int32Column::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "中文字符" + std::to_string(j));
        sub->append(std::to_string(j));
        pos->append(4);
    }

    columns.emplace_back(sub);
    columns.emplace_back(str);
    columns.emplace_back(pos);

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        if (j < 10) {
            ASSERT_EQ(6, v->get_data()[j]);
        } else {
            ASSERT_EQ(7, v->get_data()[j]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, concatWsTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto step = BinaryColumn::create();
    auto str1 = BinaryColumn::create();
    auto str2 = BinaryColumn::create();
    auto str3 = BinaryColumn::create();

    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        step->append("|");
        str1->append("a");
        str2->append(std::to_string(j));
        str3->append("b");
        null->append(j % 2);
    }

    columns.emplace_back(step);
    columns.emplace_back(str1);
    columns.emplace_back(str2);
    columns.emplace_back(NullableColumn::create(str3, null));

    ColumnPtr result = StringFunctions::concat_ws(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < 20; ++j) {
        if (j % 2) {
            ASSERT_EQ("a|" + std::to_string(j), v->get_data()[j].to_string());
        } else {
            ASSERT_EQ("a|" + std::to_string(j) + "|b", v->get_data()[j].to_string());
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, concatWs1Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto step = BinaryColumn::create();
    auto str1 = BinaryColumn::create();
    auto str2 = BinaryColumn::create();
    auto str3 = BinaryColumn::create();

    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        step->append("-----");
        str1->append("a");
        str2->append(std::to_string(j));
        str3->append("b");
        null->append(j % 2);
    }

    columns.emplace_back(step);
    columns.emplace_back(str1);
    columns.emplace_back(str2);
    columns.emplace_back(NullableColumn::create(str3, null));

    ColumnPtr result = StringFunctions::concat_ws(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < 20; ++j) {
        if (j % 2) {
            ASSERT_EQ("a-----" + std::to_string(j), v->get_data()[j].to_string());
        } else {
            ASSERT_EQ("a-----" + std::to_string(j) + "-----b", v->get_data()[j].to_string());
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, findInSetTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto strlist = BinaryColumn::create();

    str->append("b");
    strlist->append("a,b,c");

    str->append("bc");
    strlist->append("ab,cd,bc");

    str->append("bc");
    strlist->append("abc,bcd,efg");

    str->append("abc,");
    strlist->append("abc,bcd,efg");

    str->append("");
    strlist->append("abc");

    str->append("");
    strlist->append(",abc");

    str->append("abc");
    strlist->append("abc");

    str->append("bc");
    strlist->append("abc");

    str->append("bc");
    strlist->append("abc");

    columns.emplace_back(str);
    columns.emplace_back(strlist);

    ColumnPtr result = StringFunctions::find_in_set(ctx.get(), columns).value();
    ASSERT_EQ(9, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    ASSERT_EQ(2, v->get_data()[0]);
    ASSERT_EQ(3, v->get_data()[1]);
    ASSERT_EQ(0, v->get_data()[2]);
    ASSERT_EQ(0, v->get_data()[3]);
    ASSERT_EQ(0, v->get_data()[4]);
    ASSERT_EQ(1, v->get_data()[5]);
    ASSERT_EQ(1, v->get_data()[6]);
    ASSERT_EQ(0, v->get_data()[7]);
    ASSERT_EQ(0, v->get_data()[8]);
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractNullablePattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto null = NullColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisiondlist"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"", "drrry", "", "td"};

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

    auto result = StringFunctions::regexp_extract(context, columns).value();

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        if (i % 2 == 0) {
            ASSERT_TRUE(result->is_null(i));
        } else {
            ASSERT_FALSE(result->is_null(i));
        }

        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractOnlyNullPattern) {
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

    auto result = StringFunctions::regexp_extract(context, columns).value();
    for (int i = 0; i < length; ++i) {
        ASSERT_TRUE(result->is_null(i));
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractConstPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("([[:lower:]]+)C([[:lower:]]+)", 1);
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisiondlist"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"b", "drrry", "hitdeci", "isiondlist"};

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

    auto result = StringFunctions::regexp_extract(context, columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtract) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCDdrrryE", "hitdecisiondlist", "hitdecisiondlist"};
    std::string ptns[] = {"([[:lower:]]+)C([[:lower:]]+)", "([[:lower:]]+)CD([[:lower:]]+)", "(i)(.*?)(e)",
                          "(i)(.*?)(s)"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"b", "drrry", "i", "tdeci"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append(ptns[i]);
        index->append(indexs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract(context, columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplaceNullablePattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto null = NullColumn::create();
    auto replace = BinaryColumn::create();

    std::string strs[] = {"a b c", "a sdfwe b c"};
    std::string replaces[] = {"-", "<\\1>"};

    std::string res[] = {"a-b-c", "a< >sdfwe< >b< >c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    pattern->append("( )");
    pattern->append("dsdfsf");
    null->append(0);
    null->append(1);

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(pattern, null));
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    ASSERT_EQ(res[0], v->get_data()[0].to_string());
    ASSERT_TRUE(result->is_null(1));

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplaceOnlyNullPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    ColumnPtr pattern = ColumnHelper::create_const_null_column(1);
    auto replace = BinaryColumn::create();

    std::string strs[] = {"a b c", "a sdfwe b c"};
    std::string replaces[] = {"-", "<\\1>"};

    std::string res[] = {"a-b-c", "a< >sdfwe< >b< >c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();

    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplaceConstPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = ColumnHelper::create_const_column<TYPE_VARCHAR>("( )", 1);
    auto replace = BinaryColumn::create();

    std::string strs[] = {"a b c", "a sdfwe b c"};
    std::string replaces[] = {"-", "<\\1>"};

    std::string res[] = {"a-b-c", "a< >sdfwe< >b< >c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    // Test Binary input data
    {
        FunctionContext::FunctionStateScope scope = FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL;
        std::unique_ptr<FunctionContext> ctx0(FunctionContext::create_test_context());
        int binary_size = 10;
        std::unique_ptr<char[]> binary_datas = std::make_unique<char[]>(binary_size);
        memset(binary_datas.get(), 0xff, binary_size);

        auto par0 = BinaryColumn::create();
        auto par1 = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(binary_datas.get(), binary_size), 1);

        ctx0->set_constant_columns({par0, par1});

        ASSERT_ERROR(StringFunctions::regexp_replace_prepare(ctx0.get(), scope));
        ASSERT_OK(StringFunctions::regexp_close(ctx0.get(), scope));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplace) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = BinaryColumn::create();
    auto replace = BinaryColumn::create();

    std::string strs[] = {"a b c", "a b c"};
    std::string ptns[] = {" ", "(b)"};
    std::string replaces[] = {"-", "<\\1>"};

    std::string res[] = {"a-b-c", "a <b> c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        ptn->append(ptns[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplaceWithEmptyPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1);
    auto replace = BinaryColumn::create();

    std::string strs[] = {"yyyy-mm-dd", "yyyy-mm-dd"};
    std::string replaces[] = {"CHINA", "CHINA"};

    std::string res[] = {"CHINAyCHINAyCHINAyCHINAyCHINA-CHINAmCHINAmCHINA-CHINAdCHINAdCHINA",
                         "CHINAyCHINAyCHINAyCHINAyCHINA-CHINAmCHINAmCHINA-CHINAdCHINAdCHINA"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replaceNullablePattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto null = NullColumn::create();
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"a u z", "a sdfwe b c", "a equals c"};
    const std::string replaces[] = {"Ü", " ", ""};

    const std::string res[] = {"a Ü z", "a sdfwe b c", "ac"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    pattern->append("u");
    pattern->append("dsdfsf");
    pattern->append(" equals ");
    null->append(0);
    null->append(1);
    null->append(0);

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(pattern, null));
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    const auto v =
            ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    EXPECT_EQ(res[0], v->get_data()[0].to_string());
    EXPECT_TRUE(result->is_null(1));
    EXPECT_EQ(res[2], v->get_data()[2].to_string());

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replaceOnlyNullPattern1) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    ColumnPtr pattern = ColumnHelper::create_const_null_column(1);
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"a b c", "a sdfwe b c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(strs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        EXPECT_TRUE(result->is_null(i));
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// Test replace when input is only_null column.
PARALLEL_TEST(VecStringFunctionsTest, replaceOnlyNullPattern2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    ColumnPtr str = ColumnHelper::create_const_null_column(2);
    ColumnPtr pattern = ColumnHelper::create_const_null_column(1);
    ColumnPtr replace = ColumnHelper::create_const_null_column(1);

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();

    EXPECT_EQ(result->size(), 2);
    EXPECT_TRUE(result->only_null());
    EXPECT_TRUE(result->is_constant());

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// Test replace when input is not only_null column but pattern/replace is only_null
PARALLEL_TEST(VecStringFunctionsTest, replaceOnlyNullPattern2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = ColumnHelper::create_const_column<TYPE_VARCHAR>("a b c", 2);
    ColumnPtr pattern = ColumnHelper::create_const_null_column(1);
    ColumnPtr replace = ColumnHelper::create_const_null_column(1);

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();

    EXPECT_EQ(result->size(), 2);
    EXPECT_TRUE(result->only_null());
    EXPECT_TRUE(result->is_constant());

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replaceConstPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = ColumnHelper::create_const_column<TYPE_VARCHAR>(" ", 1);
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"a b c", "a sdfwe b c"};
    const std::string replaces[] = {"-", "< > "};

    const std::string res[] = {"a-b-c", "a< > sdfwe< > b< > c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    const auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// Test replace when input is const column and pattern/replace is not const column
PARALLEL_TEST(VecStringFunctionsTest, replaceConstColumn1) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = ColumnHelper::create_const_column<TYPE_VARCHAR>("a b c", 2);
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>(" ", 1);
    auto replace = BinaryColumn::create();
    const std::string replaces[] = {"-", "+"};
    for (int i = 0; i < sizeof(replaces) / sizeof(replaces[0]); ++i) {
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    ASSERT_TRUE(!result->is_constant());
    ASSERT_EQ(result->size(), 2);

    const std::string res[] = {"a-b-c", "a+b+c"};
    const auto vv = ColumnHelper::as_column<BinaryColumn>(result);
    for (int i = 0; i < vv->size(); i++) {
        ASSERT_EQ(res[i], vv->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// Test replace when input/pattern/replace are all const columns
PARALLEL_TEST(VecStringFunctionsTest, replaceConstColumn2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = ColumnHelper::create_const_column<TYPE_VARCHAR>("a b c", 2);
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>(" ", 1);
    auto replace = ColumnHelper::create_const_column<TYPE_VARCHAR>("+", 1);

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    ASSERT_TRUE(result->is_constant());
    ASSERT_EQ(result->size(), 2);
    const auto v = ColumnHelper::as_column<ConstColumn>(result);
    const auto vv = ColumnHelper::as_column<BinaryColumn>(v->data_column());
    ASSERT_EQ("a+b+c", vv->get_data()[0].to_string());

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replace) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = BinaryColumn::create();
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"a b c", "a . c", "a b c", "abc?", "xyz"};
    const std::string ptns[] = {" ", ".", "^a", "abc?", "z$"};
    const std::string replaces[] = {"-", "*\\*", " ", "xyz", " "};

    const std::string res[] = {"a-b-c", "a *\\* c", "a b c", "xyz", "xyz"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        ptn->append(ptns[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    const auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replaceWithEmptyPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1);
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"yyyy-mm-dd", "*starrocks."};
    const std::string replaces[] = {"CHINA", "CHINA"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    const auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(strs[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

} // namespace starrocks
