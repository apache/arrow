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

import pyarrow
import pyarrow.dataset as ds
import pyarrow.parquet as pq

def test_discovery():
    format_ = ds.RadosParquetFileFormat(b"/etc/ceph/ceph.conf")
    dataset = ds.dataset("file:///mnt/cephfs/nyc/", format=format_)
    assert len(dataset.files) == 8

    print(dataset.to_table(
        columns=['DOLocationID', 'total_amount', 'fare_amount'], 
        filter=( ds.field('total_amount') > 200 )).to_pandas()
    )

def test_parition_pruning():
    format_ = ds.RadosParquetFileFormat(b"/etc/ceph/ceph.conf")
    dataset = ds.dataset(
        "file:///mnt/cephfs/nyc/", 
        format=format_, 
        partitioning=["payment_type", "VendorID"], 
        partition_base_dir="/mnt/cephfs/nyc"
    )
    table = dataset.to_table(columns=["VendorID", "payment_type", "fare_amount"], filter=(ds.field("payment_type") > 2))
    print(table.to_pandas())

if __name__ == "__main__":
    test_discovery()
    test_parition_pruning()
