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
import unittest

import pytest

import pyarrow.io as io

# ----------------------------------------------------------------------
# HDFS tests


def hdfs_test_client(driver='libhdfs'):
    host = os.environ.get('ARROW_HDFS_TEST_HOST', 'localhost')
    user = os.environ['ARROW_HDFS_TEST_USER']
    try:
        port = int(os.environ.get('ARROW_HDFS_TEST_PORT', 20500))
    except ValueError:
        raise ValueError('Env variable ARROW_HDFS_TEST_PORT was not '
                         'an integer')

    return io.HdfsClient(host, port, user, driver=driver)


class HdfsTestCases(object):

    def _make_test_file(self, hdfs, test_name, test_path, test_data):
        base_path = pjoin(self.tmp_path, test_name)
        hdfs.mkdir(base_path)

        full_path = pjoin(base_path, test_path)

        with hdfs.open(full_path, 'wb') as f:
            f.write(test_data)

        return full_path

    @classmethod
    def setUpClass(cls):
        cls.check_driver()
        cls.hdfs = hdfs_test_client(cls.DRIVER)
        cls.tmp_path = '/tmp/pyarrow-test-{0}'.format(random.randint(0, 1000))
        cls.hdfs.mkdir(cls.tmp_path)

    @classmethod
    def tearDownClass(cls):
        cls.hdfs.delete(cls.tmp_path, recursive=True)
        cls.hdfs.close()

    def test_hdfs_close(self):
        client = hdfs_test_client()
        assert client.is_open
        client.close()
        assert not client.is_open

        with pytest.raises(Exception):
            client.ls('/')

    def test_hdfs_mkdir(self):
        path = pjoin(self.tmp_path, 'test-dir/test-dir')
        parent_path = pjoin(self.tmp_path, 'test-dir')

        self.hdfs.mkdir(path)
        assert self.hdfs.exists(path)

        self.hdfs.delete(parent_path, recursive=True)
        assert not self.hdfs.exists(path)

    def test_hdfs_ls(self):
        base_path = pjoin(self.tmp_path, 'ls-test')
        self.hdfs.mkdir(base_path)

        dir_path = pjoin(base_path, 'a-dir')
        f1_path = pjoin(base_path, 'a-file-1')

        self.hdfs.mkdir(dir_path)

        f = self.hdfs.open(f1_path, 'wb')
        f.write('a' * 10)

        contents = sorted(self.hdfs.ls(base_path, False))
        assert contents == [dir_path, f1_path]

    def test_hdfs_download_upload(self):
        base_path = pjoin(self.tmp_path, 'upload-test')

        data = b'foobarbaz'
        buf = BytesIO(data)
        buf.seek(0)

        self.hdfs.upload(base_path, buf)

        out_buf = BytesIO()
        self.hdfs.download(base_path, out_buf)
        out_buf.seek(0)
        assert out_buf.getvalue() == data

    def test_hdfs_file_context_manager(self):
        path = pjoin(self.tmp_path, 'ctx-manager')

        data = b'foo'
        with self.hdfs.open(path, 'wb') as f:
            f.write(data)

        with self.hdfs.open(path, 'rb') as f:
            assert f.size() == 3
            result = f.read(10)
            assert result == data

    def test_hdfs_read_whole_file(self):
        path = pjoin(self.tmp_path, 'read-whole-file')

        data = b'foo' * 1000
        with self.hdfs.open(path, 'wb') as f:
            f.write(data)

        with self.hdfs.open(path, 'rb') as f:
            result = f.read()

        assert result == data


class TestLibHdfs(HdfsTestCases, unittest.TestCase):

    DRIVER = 'libhdfs'

    @classmethod
    def check_driver(cls):
        if not io.have_libhdfs():
            pytest.skip('No libhdfs available on system')

    def test_hdfs_orphaned_file(self):
        hdfs = hdfs_test_client()
        file_path = self._make_test_file(hdfs, 'orphaned_file_test', 'fname',
                                         'foobarbaz')

        f = hdfs.open(file_path)
        hdfs = None
        f = None  # noqa


class TestLibHdfs3(HdfsTestCases, unittest.TestCase):

    DRIVER = 'libhdfs3'

    @classmethod
    def check_driver(cls):
        if not io.have_libhdfs3():
            pytest.skip('No libhdfs3 available on system')
