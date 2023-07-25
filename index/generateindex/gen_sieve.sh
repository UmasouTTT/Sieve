nohup python3 -u gen_sieve.py --wltype tpcds --table catalog_sales --column cs_sold_date_sk  > tpcds-cs_sold_date_sk-sieve.log 2>&1 &
nohup python3 -u gen_sieve.py --wltype tpcds --table catalog_sales --column cs_bill_cdemo_sk  > tpcds-cs_bill_cdemo_sk-sieve.log 2>&1 &
nohup python3 -u gen_sieve.py --wltype tpcds --table catalog_sales --column cs_bill_addr_sk  > tpcds-cs_bill_addr_sk-sieve.log 2>&1 &
wait
nohup python3 -u gen_sieve.py --wltype tpcds --table catalog_sales --column cs_ship_cdemo_sk  > tpcds-cs_ship_cdemo_sk-sieve.log 2>&1 &
nohup python3 -u gen_sieve.py --wltype tpcds --table catalog_sales --column cs_ship_customer_sk  > tpcds-cs_ship_customer_sk-sieve.log 2>&1 &
nohup python3 -u gen_sieve.py --wltype tpcds --table catalog_sales --column cs_ship_addr_sk  > tpcds-cs_ship_addr_sk-sieve.log 2>&1 &
wait
nohup python3 -u gen_sieve.py --wltype tpcds --table store_sales --column ss_customer_sk > tpcds-ss_customer_sk-other.log 2>&1 &
nohup python3 -u gen_sieve.py --wltype tpcds --table store_sales --column ss_cdemo_sk > tpcds-ss_cdemo_sk-other.log 2>&1 &
nohup python3 -u gen_sieve.py --wltype tpcds --table store_sales --column ss_addr_sk > tpcds-ss_addr_sk-other.log 2>&1 &
