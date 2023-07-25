nohup python3 -u gen_finger.py --sf 1000 --wltype tpcds --table store_sales --column ss_ticket_number > ss_1tb_finger_gen.log 2>&1 &
wait
nohup python3 -u gen_finger.py --sf 1000 --wltype tpcds --table catalog_sales --column cs_order_number > cs_1tb_finger_gen.log 2>&1 &
wait
nohup python3 -u gen_finger.py --sf 1000 --wltype tpch --table lineitem --column orderkey > lo_1tb_finger_gen.log 2>&1 &
wait
nohup python3 -u gen_finger.py --sf 1000 --wltype tpch --table partsupp --column partkey > pp_1tb_finger_gen.log 2>&1 &
wait
nohup python3 -u gen_minmax.py --sf 1000 --wltype tpcds --table catalog_sales --column cs_order_number > cs_1tb_minmax_gen.log 2>&1 &
wait
nohup python3 -u gen_minmax.py --sf 1000 --wltype tpch --table lineitem --column orderkey > lo_1tb_minmax_gen.log 2>&1 &
wait
nohup python3 -u gen_minmax.py --sf 1000 --wltype tpch --table partsupp --column partkey > pp_1tb_minmax_gen.log 2>&1 &
wait
nohup python3 -u gen_sieve.py --sf 1000 --wltype tpcds --table catalog_sales --column cs_order_number > cs_1tb_sieve_gen.log 2>&1 &
wait
nohup python3 -u gen_sieve.py --sf 1000 --wltype tpch --table lineitem --column orderkey > lo_1tb_sieve_gen.log 2>&1 &
wait
nohup python3 -u gen_sieve.py --sf 1000 --wltype tpch --table partsupp --column partkey > pp_1tb_sieve_gen.log 2>&1 &
wait
nohup python3 -u gen_reversed.py --sf 1000 --wltype tpcds --table catalog_sales --column cs_order_number > cs_1tb_opt_gen.log 2>&1 &
wait
nohup python3 -u gen_reversed.py --sf 1000 --wltype tpch --table lineitem --column orderkey > lo_1tb_opt_gen.log 2>&1 &
wait
nohup python3 -u gen_reversed.py --sf 1000 --wltype tpch --table partsupp --column partkey > pp_1tb_opt_gen.log 2>&1 &