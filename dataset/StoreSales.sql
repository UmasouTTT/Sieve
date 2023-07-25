hadoop fs -mkdir -p /tpcds/sf100/
create schema if not exists hive.tpcds_100;
use hive.tpcds_100;
CREATE TABLE hive.tpcds_1000.store_sales WITH (external_location='hdfs://node4:8020/tpcds/sf100/store_sales', format = 'parquet') AS SELECT * FROM tpcds.sf100.store_sales;