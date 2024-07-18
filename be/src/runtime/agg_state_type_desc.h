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

namespace starrocks {

#include <memory>

#include "gen_cpp/Types_types.h" // for TPrimitiveType
#include "gen_cpp/types.pb.h"    // for PTypeDesc

class TypeDescriptor;

class AggStateTypeDesc;
using AggStateTypeDescPtr = std::shared_ptr<AggStateTypeDesc>;

class AggStateTypeDesc {
public:
    AggStateTypeDesc(std::string func_name, std::vector<TypeDescriptor> arg_types, bool is_result_nullable,
                     int func_version)
            : _func_name(std::move(func_name)),
              _arg_types(std::move(arg_types)),
              _is_result_nullable(is_result_nullable),
              _func_version(func_version) {}

    std::string get_func_name() const { return _func_name; }
    std::vector<TypeDescriptor> get_arg_types() const { return _arg_types; }
    bool is_result_nullable() const { return _is_result_nullable; }
    int get_func_version() const { return _func_version; }

    // Create a new AggStateDesc from a thrift TTypeDesc.
    static AggStateTypeDescPtr from_thrift(const TAggStateTypeDesc& desc);
    // Transform this AggStateDesc to a thrift TTypeDesc.
    void to_thrift(TTypeDesc* t);

private:
    // nested aggregate function name
    std::string _func_name;
    // nested aggregate function return type
    // TypeDescriptor _return_type;
    // nested aggregate function argument types
    std::vector<TypeDescriptor> _arg_types;
    // nested aggregate function input is nullable
    bool _is_result_nullable;
    // nested aggregate function version
    int _func_version;
};

} // namespace starrocks