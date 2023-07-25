nohup python3 -u gen_hippo.py --wltype tpcds --table catalog_sales --column cs_order_number > tpcds-cs_order_number-hippo.log 2>&1 &
wait
nohup python3 -u gen_hippo.py --wltype tpcds --table store_sales --column ss_ticket_number > tpcds-ss_ticket_number-hippo.log 2>&1 &
wait
nohup python3 -u gen_hippo.py --wltype tpcds --table web_sales --column ws_order_number > tpcds-ws_order_number-hippo.log 2>&1 &
wait
nohup python3 -u gen_hippo.py --wltype tpch --table lineitem --column orderkey > tpch-lineitem-orderkey-hippo.log 2>&1 &
wait
nohup python3 -u gen_hippo.py --wltype tpch --table partsupp --column partkey > tpch-partsupp-partkey-hippo.log 2>&1 &
wait