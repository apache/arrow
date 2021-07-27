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

    if fmt == "sky":
        format_ = "skyhook"
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
