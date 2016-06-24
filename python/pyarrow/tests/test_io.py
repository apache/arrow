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

from io import BytesIO
from os.path import join as pjoin
import os
import random

import pytest

import pyarrow.io as io

#----------------------------------------------------------------------
# HDFS tests


def hdfs_test_client():
    host = os.environ.get('ARROW_HDFS_TEST_HOST', 'localhost')
    user = os.environ['ARROW_HDFS_TEST_USER']
    try:
        port = int(os.environ.get('ARROW_HDFS_TEST_PORT', 20500))
    except ValueError:
        raise ValueError('Env variable ARROW_HDFS_TEST_PORT was not '
                         'an integer')

    return io.HdfsClient.connect(host, port, user)


libhdfs = pytest.mark.skipif(not io.have_libhdfs(),
                             reason='No libhdfs available on system')


HDFS_TMP_PATH = '/tmp/pyarrow-test-{0}'.format(random.randint(0, 1000))

@pytest.fixture(scope='session')
def hdfs(request):
    fixture = hdfs_test_client()
    def teardown():
        fixture.delete(HDFS_TMP_PATH, recursive=True)
        fixture.close()
    request.addfinalizer(teardown)
    return fixture


@libhdfs
def test_hdfs_close():
    client = hdfs_test_client()
    assert client.is_open
    client.close()
    assert not client.is_open

    with pytest.raises(Exception):
        client.ls('/')


@libhdfs
def test_hdfs_mkdir(hdfs):
    path = pjoin(HDFS_TMP_PATH, 'test-dir/test-dir')
    parent_path = pjoin(HDFS_TMP_PATH, 'test-dir')

    hdfs.mkdir(path)
    assert hdfs.exists(path)

    hdfs.delete(parent_path, recursive=True)
    assert not hdfs.exists(path)


@libhdfs
def test_hdfs_ls(hdfs):
    base_path = pjoin(HDFS_TMP_PATH, 'ls-test')
    hdfs.mkdir(base_path)

    dir_path = pjoin(base_path, 'a-dir')
    f1_path = pjoin(base_path, 'a-file-1')

    hdfs.mkdir(dir_path)

    f = hdfs.open(f1_path, 'wb')
    f.write('a' * 10)

    contents = sorted(hdfs.ls(base_path, False))
    assert contents == [dir_path, f1_path]


@libhdfs
def test_hdfs_download_upload(hdfs):
    base_path = pjoin(HDFS_TMP_PATH, 'upload-test')

    data = b'foobarbaz'
    buf = BytesIO(data)
    buf.seek(0)

    hdfs.upload(base_path, buf)

    out_buf = BytesIO()
    hdfs.download(base_path, out_buf)
    out_buf.seek(0)
    assert out_buf.getvalue() == data


@libhdfs
def test_hdfs_file_context_manager(hdfs):
    path = pjoin(HDFS_TMP_PATH, 'ctx-manager')

    data = b'foo'
    with hdfs.open(path, 'wb') as f:
        f.write(data)

    with hdfs.open(path, 'rb') as f:
        assert f.size() == 3
        result = f.read(10)
        assert result == data
