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
import pytest
import urllib
import pathlib
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


skip = False
try:
    import rados as librados
    import pyarrow.rados as rados
except Exception:
    skip = True


def datadir():
    return pathlib.Path(__file__).parent / 'data'


def write_test_object():
    filename = datadir() / 'parquet' / 'v0.7.1.parquet'
    table = pq.read_table(filename)
    with open('v0.7.1.arrow', 'wb') as f:
        writer = pa.ipc.RecordBatchStreamWriter(f, table.schema)
        writer.write(table)
        writer.close()

    for i in range(0, 4):
        os.system('rados -p test-pool put myobj.{} v0.7.1.arrow'.format(i))


@pytest.mark.rados
def test_rados_dataset():
    if skip:
        return
    cluster = librados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    cluster.create_pool('test-pool')

    write_test_object()

    dataset = ds.dataset(
        source="rados:///etc/ceph/ceph.conf?cluster=ceph \
                &pool=test-pool&ids={}".format(
            urllib.parse.quote(
                str(['myobj.0', 'myobj.1', 'myobj.2', 'myobj.3']), safe='')
        )
    )

    dataset.to_table()

    cluster.delete_pool('test-pool')


@pytest.mark.rados
def test_rados_url():
    if skip:
        return
    conf = '/etc/ceph/ceph.conf'
    cluster = 'ceph'
    pool = 'test-pool'
    ids = ['obj.0', 'obj.1', 'obj.2', 'obj.3']

    generated_url = rados.generate_uri(
        ceph_config_path=conf, cluster=cluster, pool=pool, objects=ids)

    rados_factory_options = rados.parse_uri(generated_url)
    assert rados_factory_options.ceph_config_path == conf
    assert rados_factory_options.cluster_name == cluster
    assert rados_factory_options.pool_name == pool
    assert rados_factory_options.objects == ids
