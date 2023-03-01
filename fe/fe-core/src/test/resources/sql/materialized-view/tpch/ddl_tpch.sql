-- ref: https://github.com/dragansah/tpch-dbgen/blob/master/tpch-alter.sql
CREATE TABLE region ( R_REGIONKEY  INTEGER NOT NULL,
                      R_NAME       CHAR(25) NOT NULL,
                      R_COMMENT    VARCHAR(152),
                      PAD char(1) NOT NULL)
    ENGINE=OLAP
DUPLICATE KEY(`r_regionkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "R_REGIONKEY",
    "storage_format" = "DEFAULT"
);


CREATE TABLE part  ( P_PARTKEY     INTEGER NOT NULL,
                     P_NAME        VARCHAR(55) NOT NULL,
                     P_MFGR        CHAR(25) NOT NULL,
                     P_BRAND       CHAR(10) NOT NULL,
                     P_TYPE        VARCHAR(25) NOT NULL,
                     P_SIZE        INTEGER NOT NULL,
                     P_CONTAINER   CHAR(10) NOT NULL,
                     P_RETAILPRICE DOUBLE NOT NULL,
                     P_COMMENT     VARCHAR(23) NOT NULL,
                     PAD CHAR(1) NOT NULL)
    ENGINE=OLAP
DUPLICATE KEY(`P_PARTKEY`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`P_PARTKEY`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "P_PARTKEY",
    "storage_format" = "default"
);

CREATE TABLE `nation` (
                          `N_NATIONKEY` int(11) NOT NULL COMMENT "",
                          `N_NAME` char(25) NOT NULL COMMENT "",
                          `N_REGIONKEY` int(11) NOT NULL COMMENT "",
                          `N_COMMENT` varchar(152) NULL COMMENT "",
                          `PAD` char(1) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`N_NATIONKEY`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "N_NATIONKEY",
    "foreign_key_constraints" = "(N_REGIONKEY) REFERENCES region(R_REGIONKEY)",
    "storage_format" = "DEFAULT"
);


CREATE TABLE customer ( C_CUSTKEY     INTEGER NOT NULL,
                        C_NAME        VARCHAR(25) NOT NULL,
                        C_ADDRESS     VARCHAR(40) NOT NULL,
                        C_NATIONKEY   INTEGER NOT NULL,
                        C_PHONE       CHAR(15) NOT NULL,
                        C_ACCTBAL     double   NOT NULL,
                        C_MKTSEGMENT  CHAR(10) NOT NULL,
                        C_COMMENT     VARCHAR(117) NOT NULL,
                        PAD char(1) NOT NULL)
    ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "C_CUSTKEY",
    "foreign_key_constraints" = "(C_NATIONKEY) REFERENCES nation(N_NATIONKEY)",
    "storage_format" = "DEFAULT"
);

CREATE TABLE orders  ( O_ORDERKEY       INTEGER NOT NULL,
                       O_CUSTKEY        INTEGER NOT NULL,
                       O_ORDERSTATUS    CHAR(1) NOT NULL,
                       O_TOTALPRICE     double NOT NULL,
                       O_ORDERDATE      DATE NOT NULL,
                       O_ORDERPRIORITY  CHAR(15) NOT NULL,
                       O_CLERK          CHAR(15) NOT NULL,
                       O_SHIPPRIORITY   INTEGER NOT NULL,
                       O_COMMENT        VARCHAR(79) NOT NULL,
                       PAD char(1) NOT NULL)
    ENGINE=OLAP
DUPLICATE KEY(`o_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "O_ORDERKEY",
    "foreign_key_constraints" = "(O_CUSTKEY) REFERENCES customer(C_CUSTKEY)",
    "storage_format" = "DEFAULT"
);

CREATE TABLE supplier ( S_SUPPKEY     INTEGER NOT NULL,
                        S_NAME        CHAR(25) NOT NULL,
                        S_ADDRESS     VARCHAR(40) NOT NULL,
                        S_NATIONKEY   INTEGER NOT NULL,
                        S_PHONE       CHAR(15) NOT NULL,
                        S_ACCTBAL     double NOT NULL,
                        S_COMMENT     VARCHAR(101) NOT NULL,
                        PAD char(1) NOT NULL)
    ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "S_SUPPKEY",
    "foreign_key_constraints" = "(S_NATIONKEY) REFERENCES nation(N_NATIONKEY)",
    "storage_format" = "DEFAULT"
);

CREATE TABLE partsupp ( PS_PARTKEY     INTEGER NOT NULL,
                        PS_SUPPKEY     INTEGER NOT NULL,
                        PS_AVAILQTY    INTEGER NOT NULL,
                        PS_SUPPLYCOST  double  NOT NULL,
                        PS_COMMENT     VARCHAR(199) NOT NULL,
                        PAD char(1) NOT NULL)
    ENGINE=OLAP
DUPLICATE KEY(`ps_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "unique_constraints" = "PS_PARTKEY,PS_SUPPKEY",
    "foreign_key_constraints" = "(PS_PARTKEY) REFERENCES part(P_PARTKEY);(PS_SUPPKEY) REFERENCES supplier(S_SUPPKEY)",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);

CREATE TABLE lineitem ( L_ORDERKEY    INTEGER NOT NULL,
                        L_PARTKEY     INTEGER NOT NULL,
                        L_SUPPKEY     INTEGER NOT NULL,
                        L_LINENUMBER  INTEGER NOT NULL,
                        L_QUANTITY    DOUBLE NOT NULL,
                        L_EXTENDEDPRICE  DOUBLE NOT NULL,
                        L_DISCOUNT    DOUBLE NOT NULL,
                        L_TAX         DOUBLE NOT NULL,
                        L_RETURNFLAG  CHAR(1) NOT NULL,
                        L_LINESTATUS  CHAR(1) NOT NULL,
                        L_SHIPDATE    DATE NOT NULL,
                        L_COMMITDATE  DATE NOT NULL,
                        L_RECEIPTDATE DATE NOT NULL,
                        L_SHIPINSTRUCT CHAR(25) NOT NULL,
                        L_SHIPMODE     CHAR(10) NOT NULL,
                        L_COMMENT      VARCHAR(44) NOT NULL,
                        PAD CHAR(1) NOT NULL)
    ENGINE=OLAP
DUPLICATE KEY(`L_ORDERKEY`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "L_ORDERKEY;L_PARTKEY;L_SUPPKEY",
    "foreign_key_constraints" = "(L_ORDERKEY) REFERENCES orders(O_ORDERKEY);(L_PARTKEY,L_SUPPKEY) REFERENCES partsupp(PS_PARTKEY, PS_SUPPKEY)",
    "storage_format" = "default"
);