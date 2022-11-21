// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <type_traits>

#include "column/column.h"
#include "column/type_traits.h"

namespace starrocks::vectorized {

// TODO: Support detail agg state reusable between different agg stats.
// TODO: How to handle count=0's key-value?
template <LogicalType PT>
struct StreamDetailState {
    using T = RunTimeCppType<PT>;
    StreamDetailState() = default;
    ~StreamDetailState() = default;

    void update_rows(const T& value, int64_t num_rows) {
        auto iter = _detail_state.find(value);
        if constexpr (std::is_same_v<int64_t, T>) {
            std::cout << "before iter->second:" << iter->second << ", value:" << value << ", num_rows:" << num_rows
                      << std::endl;
        }
        if (iter != _detail_state.end()) {
            iter->second += num_rows;
        } else {
            iter->second = num_rows;
        }
        if constexpr (std::is_same_v<int64_t, T>) {
            std::cout << "after iter->second:" << iter->second << ", value:" << value << ", num_rows:" << num_rows
                      << std::endl;
        }
    }
    const std::map<T, int64_t>& detail_state() const { return _detail_state; }
    const bool is_sync() const { return _is_sync; }
    void mark_sync(bool sync) { this->_is_sync = sync; }

    // Keep tracts with all details for a specific group by key to
    // be used for detail aggregation retracts.
    std::map<T, int64_t> _detail_state;
    // Mark whether sync all detail data when generating output results,
    // when _is_sync=true, use all key-values in _detail_state to update
    // intermediate state, and generate the final results.
    bool _is_sync{false};
};

} // namespace starrocks::vectorized
