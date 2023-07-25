nohup python3 -u gen_workload_2txt.py --sf 100 --wltype tpcds --table catalog_sales --column cs_order_number > cs_order_number_workload.txt 2>&1 &
wait
nohup python3 -u gen_workload_2txt.py --sf 100 --wltype tpcds --table store_sales --column ss_ticket_number > ss_ticket_number_workload.txt 2>&1 &
wait
nohup python3 -u gen_workload_2txt.py --sf 100 --wltype tpch --table lineitem --column orderkey > lineitem_orderkey_workload.txt 2>&1 &
wait
nohup python3 -u gen_workload_2txt.py --sf 100 --wltype tpch --table partsupp --column partkey > partsupp_partkey_workload.txt 2>&1 &
wait
nohup python3 -u gen_workload_2txt.py --wltype wiki  --column pagecount > pagecount_workload.txt 2>&1 &
wait
nohup python3 -u gen_workload_2txt.py --wltype openstreet > openstreet_workload.txt 2>&1 &