nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_order_number --attribute_percent 0.0015 > tpcds-cs_order_number-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_ticket_number --attribute_percent 0.0015 > tpcds-ss_ticket_number-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table web_sales --column ws_order_number --attribute_percent 0.0025 > tpcds-ws_order_number-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpch --table lineitem --column orderkey --attribute_percent 0.00029 > tpch-lineitem-orderkey-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpch --table partsupp --column partkey --attribute_percent 0.0005 > tpch-partsupp-partkey-gre.log 2>&1 &
wait
# tpcds cs gre
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_sold_date_sk --attribute_percent 0.0015 > tpcds-cs_sold_date_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_sold_time_sk --attribute_percent 0.0015 > tpcds-cs_sold_time_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_ship_date_sk --attribute_percent 0.0015 > tpcds-cs_ship_date_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_bill_customer_sk --attribute_percent 0.0015 > tpcds-cs_bill_customer_sk-gre.log 2>&1 &
wait
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_bill_cdemo_sk --attribute_percent 0.0015 > tpcds-cs_bill_cdemo_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_bill_hdemo_sk --attribute_percent 0.0015 > tpcds-cs_bill_hdemo_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_bill_addr_sk --attribute_percent 0.0015 > tpcds-cs_bill_addr_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_ship_customer_sk --attribute_percent 0.0015 > tpcds-cs_ship_customer_sk-gre.log 2>&1 &
wait
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_ship_cdemo_sk --attribute_percent 0.0015 > tpcds-cs_ship_cdemo_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_ship_hdemo_sk --attribute_percent 0.0015 > tpcds-cs_ship_hdemo_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_ship_addr_sk --attribute_percent 0.0015 > tpcds-cs_ship_addr_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_call_center_sk --attribute_percent 0.0015 > tpcds-cs_call_center_sk-gre.log 2>&1 &
wait
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_catalog_page_sk --attribute_percent 0.0015 > tpcds-cs_catalog_page_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_ship_mode_sk --attribute_percent 0.0015 > tpcds-cs_ship_mode_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_warehouse_sk --attribute_percent 0.0015 > tpcds-cs_warehouse_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_item_sk --attribute_percent 0.0015 > tpcds-cs_item_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table catalog_sales --column cs_promo_sk --attribute_percent 0.0015 > tpcds-cs_promo_sk-gre.log 2>&1 &
wait
# tpcds ss gre
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_sold_date_sk --attribute_percent 0.0015 > tpcds-ss_sold_date_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_sold_time_sk --attribute_percent 0.0015 > tpcds-ss_sold_time_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_item_sk --attribute_percent 0.0015 > tpcds-ss_item_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_customer_sk --attribute_percent 0.0015 > tpcds-ss_customer_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_cdemo_sk --attribute_percent 0.0015 > tpcds-ss_cdemo_sk-gre.log 2>&1 &
wait
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_hdemo_sk --attribute_percent 0.0015 > tpcds-ss_hdemo_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_addr_sk --attribute_percent 0.0015 > tpcds-ss_addr_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_store_sk --attribute_percent 0.0015 > tpcds-ss_store_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_promo_sk --attribute_percent 0.0015 > tpcds-ss_promo_sk-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpcds --table store_sales --column ss_quantity --attribute_percent 0.0015 > tpcds-ss_quantity-gre.log 2>&1 &
# tpch lineitem gre
nohup python3 -u gen_gre.py --wltype tpch --table lineitem --column partkey --attribute_percent 0.00029 > tpch-lineitem-partkey-gre.log 2>&1 &
nohup python3 -u gen_gre.py --wltype tpch --table lineitem --column suppkey --attribute_percent 0.00029 > tpch-lineitem-suppkey-gre.log 2>&1 &
