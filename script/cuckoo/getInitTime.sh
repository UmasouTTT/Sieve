dir="catalog_sales/"
column="cs_order_number"
logpath="cs_order_number_workload.txt"
ls $dir | while read line
do
    file=${dir}${line}
    echo "$file start"
    sed -i "31c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "378c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "280c\  std::ifstream file(\"/proj/dst-PG0/search_workload/$logpath\");" lookup_benchmark.cc
    bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark -- --input_csv_path="$file" --columns_to_test="$column" > cslog/$line.log
    echo "$file end"
done

dir="store_sales/"
column="ss_ticket_number"
logpath="ss_ticket_number_workload.txt"
ls $dir | while read line
do
    file=${dir}${line}
    echo "$file start"
    sed -i "31c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "378c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "280c\  std::ifstream file(\"/proj/dst-PG0/search_workload/$logpath\");" lookup_benchmark.cc
    bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark -- --input_csv_path="$file" --columns_to_test="$column" > sslog/$line.log
    echo "$file end"
done


dir="lineitem/"
column="orderkey"
logpath="lineitem_orderkey_workload.txt"
ls $dir | while read line
do
    file=${dir}${line}
    echo "$file start"
    sed -i "31c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "378c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "280c\  std::ifstream file(\"/proj/dst-PG0/search_workload/$logpath\");" lookup_benchmark.cc
    bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark -- --input_csv_path="$file" --columns_to_test="$column" > lineitemlog/$line.log
    echo "$file end"
done


dir="openstreet/"
column="lon"
logpath="openstreet_workload.txt"
ls $dir | while read line
do
    file=${dir}${line}
    echo "$file start"
    sed -i "31c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "378c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "280c\  std::ifstream file(\"/proj/dst-PG0/search_workload/$logpath\");" lookup_benchmark.cc
    bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark -- --input_csv_path="$file" --columns_to_test="$column" > openstreetlog/$line.log
    echo "$file end"
done

dir="wikipedia/"
column="pagecount"
logpath="pagecount_workloadsub.txt"
ls $dir | while read line
do
    file=${dir}${line}
    echo "$file start"
    sed -i "31c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "378c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "280c\  std::ifstream file(\"/proj/dst-PG0/search_workload/$logpath\");" lookup_benchmark.cc
    bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark -- --input_csv_path="$file" --columns_to_test="$column" > wikilog/$line.log
    echo "$file end"
done

dir="store_sales/"
column="ss_ticket_number"
logpath="ss_ticket_number_1tb_workloadsub.txt"
ls $dir | while read line
do
    file=${dir}${line}
    echo "$file start"
    sed -i "31c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "378c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "280c\  std::ifstream file(\"/proj/dst-PG0/search_workload/$logpath\");" lookup_benchmark.cc
    bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark -- --input_csv_path="$file" --columns_to_test="$column" > ss1tblog/$line.log
    echo "$file end"
done