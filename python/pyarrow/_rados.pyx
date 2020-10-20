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

# cython: language_level = 3

from pyarrow._dataset cimport FileFormat
from pyarrow.lib cimport *
from pyarrow.lib import frombytes, tobytes

cdef class RadosParquetFileFormat(FileFormat):
    cdef:
        CRadosParquetFileFormat* rados_parquet_format

    def __init__(
        self,
        ceph_config_path="/etc/ceph/ceph.conf",
        data_pool="cephfs_data",
        user_name="client.admin",
        cluster_name="ceph"
    ):
        self.init(shared_ptr[CFileFormat](
            new CRadosParquetFileFormat(
                tobytes(ceph_config_path),
                tobytes(data_pool),
                tobytes(user_name),
                tobytes(cluster_name)
            )
        ))

    cdef void init(self, const shared_ptr[CFileFormat]& sp):
        FileFormat.init(self, sp)
        self.rados_parquet_format = <CRadosParquetFileFormat*> sp.get()
