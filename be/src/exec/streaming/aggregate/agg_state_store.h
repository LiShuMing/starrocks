// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_AGG_STATE_STORE_H
#define STARROCKS_AGG_STATE_STORE_H

enum AggStateStoreKind {
    ResultTable,
    DetailTable,
    IntermediateTable
};

class ResultTableState {

};

class AggStateStore {
public:
    AggStateStoreKind agg_store_kind() const { return _kind; }

private:
    AggStateStoreKind _kind;
};

#endif //STARROCKS_AGG_STATE_STORE_H
