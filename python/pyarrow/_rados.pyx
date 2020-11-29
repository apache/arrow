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
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow.lib cimport _Weakrefable

cdef class RadosDatasetFactoryOptions(_Weakrefable):
    __slots__ = ()

    def __init__(self,
                 pool_name='test-pool',
                 ceph_config_path='/etc/ceph/ceph.conf',
                 objects=[],
                 user_name='client.admin',
                 cluster_name='ceph',
                 flags=0,
                 cls_name='arrow'):
        self.rados_factory_options.pool_name_ = tobytes(pool_name)
        self.rados_factory_options.ceph_config_path_ = tobytes(
            ceph_config_path)
        self.rados_factory_options.objects_ = [tobytes(s) for s in objects]
        self.rados_factory_options.user_name_ = tobytes(user_name)
        self.rados_factory_options.cluster_name_ = tobytes(cluster_name)
        self.rados_factory_options.flags_ = flags
        self.rados_factory_options.cls_name_ = tobytes(cls_name)

    @property
    def ceph_config_path(self):
        return frombytes(self.rados_factory_options.ceph_config_path_)

    @property
    def objects(self):
        return [
            frombytes(path) for path in self.rados_factory_options.objects_
        ]

    @property
    def pool_name(self):
        return frombytes(self.rados_factory_options.pool_name_)

    @property
    def user_name(self):
        return frombytes(self.rados_factory_options.user_name_)

    @property
    def cluster_name(self):
        return frombytes(self.rados_factory_options.cluster_name_)

    @property
    def flags(self):
        return self.rados_factory_options.flags_

    @property
    def cls_name(self):
        return frombytes(self.rados_factory_options.cls_name_)


cdef class RadosDataset(Dataset):
    def __init__(self, RadosDatasetFactoryOptions rados_factory_options=None):
        cdef:
            CRadosDatasetFactoryOptions c_rados_factory_options

        if rados_factory_options is None:
            rados_factory_options = RadosDatasetFactoryOptions()

        c_rados_factory_options = rados_factory_options.unwrap()

        self.init(GetResultValue(CRadosDataset.Make(c_rados_factory_options)))

    cdef void init(self, const shared_ptr[CDataset]& sp):
        Dataset.init(self, sp)
        self.rados_dataset = <CRadosDataset*> sp.get()
