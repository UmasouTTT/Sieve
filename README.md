# Sieve: A Learned Data-Skipping Index for Data Analytics

## Introduction
Sieve is a learning-enhanced data-skipping index designed to effectively filter out irrelevant data blocks for modern data analytics services. By capturing patterns in the block distribution with piece-wise linear functions, Sieve is able to trade off storage consumption and data filter efficiency.




## Dataset
### Prepared Data
The experiment dataset can be obtained from the remote dataset of cloudlab.

Cloudlab urn: "urn:publicid:IDN+clemson.cloudlab.us:ccjs-pg0+ltdataset+datasetinclem".

For more details about Cloudlab urn, see http://docs.cloudlab.us/advanced-storage.html#%28part._remote-storage%29
|Dataset|Dir|
|-----|-----|
|StoreSales|/mydata/bigdata/data/StoreSales/|
|Wikipedia|/mydata/bigdata/data/Wikipedia/|
|Maps|/mydata/bigdata/data/Maps/|

### Generate Data
1. StoreSales: https://trino.io/docs/current/connector/tpcds.html
2. Wikipedia: https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-07/
3. Maps: https://github.com/awslabs/open-data-docs/tree/main/docs/osm-pds
   
- Data preprocessing(pulling raw data, transforming) can be referred to scripts in ./Sieve/dataset.

Place prepared/generated datasets under path: ./Sieve/dataset/StoreSales, ./Sieve/dataset/Wikipedia, ./Sieve/dataset/Maps.


## Setup

### Base OS
Ubuntu 18.04
### Software Requirements
python3.8
### Installing Packages
    ```
    // for common
    sudo apt update
    sudo apt upgrade
    apt install --upgrade pip

    // for data processing
    pip install pyarrow==8.0.0
    pip install datasketch
    pip install pybloom_live
    
    // for cuckoo index
    sudo apt-get install pkg-config zip g++ zlib1g-dev unzip python
    wget https://github.com/bazelbuild/bazel/releases/download/4.2.3/bazel-4.2.3-installer-linux-x86_64.sh
    chmod +x bazel-4.2.3-installer-linux-x86_64.sh
	./bazel-4.2.3-installer-linux-x86_64.sh --user
    export PATH="$PATH:$HOME/bin"
    // cuckoo-index source code
    cd ./Sieve
    git clone https://github.com/LiuJiazhen1999/cuckoo-index -b change
    
    ```
	
## Running All Experiments

### Initialization overhead & Overall Performance
python index/simulator/index_compare/indexcompare_prelog.py.


### Impact of data insertion
python index/simulator/insert/insertTest.py.

### Block Size Scalability
python index/simulator/blk_scale/diff_blksize.py.

### Worst Case Data
1. Dense data: python index/simulator/worsecasedata/dense_worst_case.py
2. Sparse data: python index/simulator/worsecasedata/sparse_worst_case.py


