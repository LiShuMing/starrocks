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

namespace starrocks {

#include "runtime/agg_state_type_desc.h"

#include <memory>

#include "runtime/types.h"

// Create a new AggStateDesc from a thrift TTypeDesc.
static AggStateTypeDescPtr AggStateTypeDesc::from_thrift(const TAggStateTypeDesc& desc) {
    std::string agg_func_name = desc.agg_func_name;
    std::vector<TypeDescriptor> arg_types;
    for (auto& arg_type : desc.arg_types) {
        arg_types.emplace_back(TypeDescriptor::from_thrift(arg_type));
    }
    bool result_nullable = desc.result_nullable;
    int func_version = desc.func_version;
    return std::make_shared<AggStateTypeDesc>(agg_func_name, arg_types, result_nullable, func_version);
}

// Transform this AggStateDesc to a thrift TTypeDesc.
void AggStateTypeDesc::to_thrift(TTypeDesc* t) {
    t->__isset.agg_state_type = true;
    t->agg_state_type.agg_func_name = _func_name;
    t->agg_state_type.result_nullable = _is_result_nullable;
    t->agg_state_type.func_version = _func_version;
    for (auto& arg_type : _arg_types) {
        t->agg_state_type.arg_types.push_back(arg_type.to_thrift());
    }
}

} // namespace starrocks