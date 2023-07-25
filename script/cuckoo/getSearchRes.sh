cat /proj/dst-PG0/search_workload/partsupp_partkey_workload.txt | while read line; do
    if [[ $line == *"selectivity"* ]]; then
        echo $line
    else
        prefix=$(echo $line | awk '{print $1}')
        if [[ $prefix == "range:" ]]; then
            nums=$(echo $line | awk -F ':' '{print $2}')
            start=$(echo $nums | awk -F ',' '{print $1}')
            end=$(echo $nums | awk -F ',' '{print $2}')
            echo "range: $start, $end"
        elif [[ $prefix == "point:" ]]; then
            num=$(echo $line | awk -F ':' '{print $2}')
            echo "point: $num"
        fi
    fi
done

dir="./partsupp/"
column="partkey"
cur_sec=`date '+%s'`
echo "before dir cons, sec is $cur_sec"
ls $dir | while read line
do
    cur_sec=`date '+%s'`
    file=${dir}${line}
    # replace 26th row of BUILD.bazel
    sed -i "31c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    sed -i "378c\        \"$file\"" /mydata/cuckoo-index/BUILD.bazel
    bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark -- --input_csv_path="$file" --columns_to_test="$column"
    down_sec=`date '+%s'`
    echo "$file con time is $((down_sec-cur_sec))"
done
cur_sec=`date '+%s'`
echo "after dir cons, sec is $cur_sec"