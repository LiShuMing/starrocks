// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_STATETABLE_H
#define STARROCKS_STATETABLE_H

#include "common/status.h"

namespace starrocks::vectorized {

class StateTable {
    Status flush();
};

}
#endif //STARROCKS_STATETABLE_H
