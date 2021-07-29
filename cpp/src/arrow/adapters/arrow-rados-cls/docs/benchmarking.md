<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Benchmarking SkyhookDM

1. Download the required scripts and make them executable.

```bash
# download the ceph deploy script
wget https://raw.githubusercontent.com/uccross/skyhookdm-arrow/arrow-master/cpp/src/arrow/adapters/arrow-rados-cls/scripts/deploy_ceph.sh && chmod +x deploy_ceph.sh

# download the skyhook deploy script
wget https://raw.githubusercontent.com/uccross/skyhookdm-arrow/arrow-master/cpp/src/arrow/adapters/arrow-rados-cls/scripts/deploy_skyhook.sh && chmod +x deploy_skyhook.sh

# download the data deploy script
wget https://raw.githubusercontent.com/uccross/skyhookdm-arrow/arrow-master/cpp/src/arrow/adapters/arrow-rados-cls/scripts/deploy_data.sh && chmod +x deploy_data.sh
```

2. Execute deploy_ceph script to deploy a Ceph cluster on a set of nodes and to mount CephFS on the client/admin node. On the client node, execute:

```bash
./deploy_ceph.sh mon1,mon2,mon3 osd1,osd2,osd3 mds1 mgr1 /dev/sdb 3
```
where mon1, mon2, osd1, etc. are the internal hostnames of the nodes.

3. Build and install the SkyhookDM CLS plugins and PyArrow (with Skyhook extensions):

```bash
./deploy_skyhook.sh osd1,osd2,osd3
```
This will build the CLS plugins as shared libraries and deploy them to the OSD nodes.

4. Download a sample dataset from [this](https://github.com/jayjeetc/datasets) repository:

```bash
apt update
apt install git-lfs
git clone https://github.com/jayjeetc/datasets
cd datasets/
git lfs pull
cd ..
```

5. Create and write a sample dataset to the CephFS mount by replicating the 16MB Parquet file downloaded in the previous step:

```bash
./deploy_data.sh [source file] [destination dir] [no. of copies] [stripe unit]
```

For example,

```bash
./deploy_data.sh datasets/16MB.parquet /mnt/cephfs/dataset 1500 16777216
```

This will write 1500 of ~16MB Parquet files to /mnt/cephfs/dataset using a CephFS stripe size of 16MB. If your files are not already of size less than 16MB, you can split them up using the `SplittedParquetWriter` API as shown in the code snippet given below. 

```python
import os
import sys

from pyarrow.rados import SplittedParquetWriter

if __name__ == "__main__":
    source_dir = str(sys.argv[1])
    destination_dir = str(sys.argv[2])
    chunksize = int(sys.argv[3])

    files = os.listdir(source_dir)
    for file in files:
        path = os.path.join(source_dir, file)
        writer = SplittedParquetWriter(path, destination_dir, chunksize)
        writer.write()
```

6. Optionally, you can also deploy Prometheus and Grafana for monitoring the cluster by following [this](https://github.com/JayjeetAtGithub/prometheus-on-baremetal#readme) guide.

7. Run the benchmark script given below to get some initial benchmarks for SkyhookDM performance while using different row selectivities.

```python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import sys
import time
import numpy as np
import pandas
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
import json


def drop_caches():
    os.system('sync')
    os.system('echo 3 > /proc/sys/vm/drop_caches')
    os.system('sync')


def do_scan(fragment, filter_):
    fragment.to_table(filter=filter_, use_threads=False)


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("usage: ./bench.py <format(pq/rpq)> <iterations> <dataset> <workers> <file>")
        sys.exit(0)

    fmt = str(sys.argv[1])
    iterations = int(sys.argv[2])
    directory = str(sys.argv[3])
    workers = int(sys.argv[4])
    resultfile = str(sys.argv[5])

    if fmt == "rpq":
        format_ = ds.SkyhookFileFormat("pq")
    elif fmt == "ripc":
        format_ = ds.SkyhookFileFormat("ipc")
    elif fmt == "pq":
        format_ = "parquet"
    elif fmt == "ipc":
        format_ = "ipc"

    selectivity = ["100", "99", "90", "75", "50", "25", "10", "1"]
    data = dict()
    for per in selectivity:
        data[per] = list()
        for i in range(iterations):
            drop_caches()
            dataset_ = ds.dataset(directory, format=format_)
            start = time.time()

            if per == "100":
                filter_ = None
            if per == "99":
                filter_ = (ds.field("total_amount") > -200)
            if per == "90":
                filter_ = (ds.field("total_amount") > 4)
            if per == "75":
                filter_ = (ds.field("total_amount") > 9)
            if per == "50":
                filter_ = (ds.field("total_amount") > 11)
            if per == "25":
                filter_ = (ds.field("total_amount") > 19)
            if per == "10":
                filter_ = (ds.field("total_amount") > 27)
            if per == "1":
                filter_ = (ds.field("total_amount") > 69)

            with ThreadPoolExecutor(max_workers=workers) as executor:
                    for fragment in dataset_.get_fragments(filter=filter_):
                            future = executor.submit(do_scan, fragment, filter_)

            end = time.time()
            data[per].append(end-start)
            print(end-start)

            with open(resultfile, 'w') as fp:
                json.dump(data, fp)

```

```bash
python3 bench.py [format(pq/rpq)] [iterations] [file:///path/to/dataset] [workers] [result file]
```

For example,
```bash
python3 bench.py rpq 10 file:///mnt/cephfs/dataset 16 result.json
```
