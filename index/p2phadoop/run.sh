#nohup python3 -u autorun_cs_order_number.py > autorun_cs_order_number.log 2>&1 &
#wait
#nohup python3 -u autorun_lineitem_orderkey.py > autorun_lineitem_orderkey.log 2>&1 &
#wait

#nohup python3 -u autorun_ss_ticket_number.py > autorun_ss_ticket_number.log 2>&1 &
#wait
#nohup python3 -u autorun_ws_order_number.py > autorun_ws_order_number.log 2>&1 &
#wait
nohup python3 -u autorun_partsupp_partkey.py > autorun_partsupp_partkey.log 2>&1 &
wait
nohup python3 -u auto_wiki_pagecount.py > auto_wiki_pagecount.log 2>&1 &
wait
nohup python3 -u autorun_orders_orderkey.py > autorun_orders_orderkey.log 2>&1 &
wait