// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::vectorized {
using SlotInfo = std::tuple<std::string, LogicalType, bool>;
using SlotInfoArray = std::vector<SlotInfo>;

class DescTblHelper {
public:
    static void generate_desc_tuple(const SlotInfoArray& slot_infos, TDescriptorTableBuilder* desc_tbl_builder) {
        TTupleDescriptorBuilder tuple_builder;
        for (auto& slot_info : slot_infos) {
            auto& name = std::get<0>(slot_info);
            auto type = TypeDescriptor::from_primtive_type(std::get<1>(slot_info));
            auto& is_nullable = std::get<2>(slot_info);
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(type)
                    .length(type.len)
                    .precision(type.precision)
                    .scale(type.scale)
                    .nullable(is_nullable);
            slot_desc_builder.column_name(name);
            tuple_builder.add_slot(slot_desc_builder.build());
        }
        tuple_builder.build(desc_tbl_builder);
    }

    static DescriptorTbl* generate_desc_tbl(RuntimeState* runtime_state, ObjectPool& obj_pool,
                                            const std::vector<SlotInfoArray>& slot_infos) {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        for (auto& slot_info : slot_infos) {
            generate_desc_tuple(slot_info, &desc_tbl_builder);
        }
        DescriptorTbl* desc_tbl = nullptr;
        DescriptorTbl::create(runtime_state, &obj_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                              config::vector_chunk_size);
        return desc_tbl;
    }
};
} // namespace starrocks::vectorized
