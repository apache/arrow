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

from pyarrow._dataset cimport Dataset
from pyarrow.lib cimport *
from pyarrow.lib import frombytes, tobytes
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow.lib cimport _Weakrefable

cdef extern from "arrow/dataset/dataset_rados.h" \
        namespace "arrow::dataset" nogil:
    cdef cppclass CRadosDatasetFactoryOptions \
            "arrow::dataset::RadosDatasetFactoryOptions":
        vector[c_string] objects_
        c_string pool_name_
        c_string user_name_
        c_string cluster_name_
        c_string ceph_config_path_
        uint64_t flags_
        c_string cls_name_

    cdef cppclass CRadosDataset \
            "arrow::dataset::RadosDataset"(CDataset):
        @staticmethod
        CResult[shared_ptr[CDataset]] Make "Make"(
            CRadosDatasetFactoryOptions factory_option
        )

        @staticmethod
        CStatus Write "Write"(
            vector[shared_ptr[CRecordBatch]] batches,
            CRadosDatasetFactoryOptions factory_option,
            c_string object_id
        )

cdef class RadosDatasetFactoryOptions(_Weakrefable):
    cdef:
        CRadosDatasetFactoryOptions rados_factory_options

    cdef inline CRadosDatasetFactoryOptions unwrap(self):
        return self.rados_factory_options

cdef class RadosDataset(Dataset):
    cdef:
        CRadosDataset* rados_dataset

    cdef void init(self, const shared_ptr[CDataset]& sp)
