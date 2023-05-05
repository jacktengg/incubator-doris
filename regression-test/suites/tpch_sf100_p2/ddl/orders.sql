CREATE TABLE IF NOT EXISTS orders  (
  O_ORDERKEY       INTEGER NOT NULL,
  O_CUSTKEY        INTEGER NOT NULL,
  O_ORDERSTATUS    CHAR(1) NOT NULL,
  O_TOTALPRICE     DECIMALV3(15,2) NOT NULL,
  O_ORDERDATE      DATE NOT NULL,
  O_ORDERPRIORITY  CHAR(15) NOT NULL,  
  O_CLERK          CHAR(15) NOT NULL, 
  O_SHIPPRIORITY   INTEGER NOT NULL,
  O_COMMENT        VARCHAR(79) NOT NULL
)
DUPLICATE KEY(O_ORDERKEY, O_CUSTKEY)
DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 32
PROPERTIES (
  "replication_num" = "1"
)

