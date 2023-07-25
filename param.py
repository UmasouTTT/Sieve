import argparse

parser = argparse.ArgumentParser(description='IndexOnLake')

# -- learned index --
parser.add_argument('--segment_error', type=int, default=100,
                    help='error for Fiting index')
parser.add_argument('--sieve_gap_percent', type=float, default=0.01,
                    help="sieve gap percent")
parser.add_argument('--file_type', type=str, default="Parquet",
                    help='type of file')

parser.add_argument('--min_selectivity', type=float, default=0.0001,
                    help='min selectivity')

parser.add_argument('--with_middle_layer', type=bool, default=True)

parser.add_argument('--make_middle_layer_type', type=str, default="dynamic",
                    help="static, dynamic")

parser.add_argument('--sub_segment_num', type=int, default=-1,
                    help="sub_segment_num")

parser.add_argument('--attribute_percent', type=float, default=0.002,
                    help="static, dynamic")

parser.add_argument('--largegapth', type=int, default=10,
                    help='largegapth')

parser.add_argument('--partition_num', type=int, default=1000,
                    help="the easy way to control sieve size")

parser.add_argument('--block_density_threshold', type=float, default=0.3,
                    help="threshold for regenerate data")

parser.add_argument('--rebuildtime', type=int, default=100,
                    help="a single file rebuild sieve time/s")

# invertIndex
parser.add_argument('--split_upper_threshold', type=int, default=100,
                    help='split upper limit')
parser.add_argument('--split_lower_threshold', type=int, default=2,
                    help='split lower limit')
parser.add_argument('--jaccard_upper_threshold', type=int, default=0.9,
                    help='jaccard upper limit')

# gaplist
parser.add_argument('--num_of_gap_lists', type=int, default=10,
                    help='num of gaplists')
parser.add_argument('--int_interval', type=int, default=1,
                    help='smallest gap for int values')

# finger prints
parser.add_argument('--num_of_intervals', type=int, default=1000,
                    help='num of intervals')

# GRT
parser.add_argument('--num_of_sub_ranges', type=int, default=1000,
                    help='num of GRT ranges')
parser.add_argument('--num_of_gre_gap', type=int, default=15,
                    help='num of GRT ranges')

# Bloom
parser.add_argument('--bloom_init_capacity', type=int, default=5000,
                    help='num of bloom_init_capacity')

parser.add_argument('--bloom_error_rate', type=float, default=0.05,
                    help='error rate of bloom')

# Two Birds
parser.add_argument('--bucket_num', type=int, default=10000,
                    help='num of buckets')
parser.add_argument('--partial_histogram_density', type=float, default=0.005,
                    help='num of buckets')
# workload
parser.add_argument('--sf', type=str, default="100",
                    help='scale factor')
parser.add_argument('--wltype', type=str, default="tpch",
                    help='workload type')
parser.add_argument('--table', type=str, default="part",
                    help='table')
parser.add_argument('--column', type=str, default="partkey",
                    help='column')
parser.add_argument('--singleFile', type=bool, default=False,
                    help='singleFile')

# insert
parser.add_argument('--isinsert', type=bool, default=False,
                    help='isinsert')

#dump dir
parser.add_argument('--isdump', type=bool, default=False,
                    help='isdump')
parser.add_argument('--learnedIndexDir', type=str, default="./indexdata/learned/",
                    help='learnedIndexDir')
parser.add_argument('--greIndexDir', type=str, default="./indexdata/gre/",
                    help='greIndexDir')
parser.add_argument('--grtIndexDir', type=str, default="./indexdata/grt/",
                    help='grtIndexDir')
parser.add_argument('--fingerprintIndexDir', type=str, default="./indexdata/fingerprint/",
                    help='fingerprintIndexDir')
parser.add_argument('--gaplistIndexDir', type=str, default="./indexdata/gaplist/",
                    help='gaplistIndexDir')
parser.add_argument('--minmaxIndexDir', type=str, default="./indexdata/minmax/",
                    help='minmaxIndexDir')
parser.add_argument('--reversedIndexDir', type=str, default="./indexdata/reversed/",
                    help='reversedIndexDir')
parser.add_argument('--fittreeDir', type=str, default="./indexdata/fittree/",
                    help='fittreeDir')
parser.add_argument('--fitmapDir', type=str, default="./indexdata/fitmap/",
                    help='fitmapDir')
parser.add_argument('--hippoDir', type=str, default="./indexdata/hippo/",
                    help='hippodir')

parser.add_argument('--selectivity', type=float, default=0.00001,
                    help='selectivity-0.001%,0.01%,0.1%,1%')

# p2p
parser.add_argument('--range_s', type=int, default=10, help='range_s')
parser.add_argument('--range_e', type=int, default=10, help='range_e')
parser.add_argument('--isFirstRun', type=bool, default=False, help='isFirstRun')

# Server
parser.add_argument("--host", type=str, default="127.0.0.1",
                    help='host of index')
parser.add_argument("--port", type=int, default="8888",
                    help='port of index')

args = parser.parse_args()
