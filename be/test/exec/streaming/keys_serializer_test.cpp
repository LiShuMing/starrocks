// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include "exec/vectorized/aggregate/agg_hash_map.h"

#include <gtest/gtest.h>

#include <any>

#include "column/column_helper.h"
#include "column/datum.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/aggregate/agg_hash_set.h"
#include "runtime/mem_pool.h"
#include "runtime/primitive_type.h"

TEST(KeysSerializerTest, Basic) {

}