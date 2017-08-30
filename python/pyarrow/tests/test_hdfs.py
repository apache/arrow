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

import numpy as np
import pandas.util.testing as pdt
import pytest

from pyarrow.compat import guid
import pyarrow as pa

import pyarrow.tests.test_parquet as test_parquet

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

    return pa.hdfs.connect(host, port, user, driver=driver)


@pytest.mark.hdfs
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

    def test_cat(self):
        path = pjoin(self.tmp_path, 'cat-test')

        data = b'foobarbaz'
        with self.hdfs.open(path, 'wb') as f:
            f.write(data)

        contents = self.hdfs.cat(path)
        assert contents == data

    def test_capacity_space(self):
        capacity = self.hdfs.get_capacity()
        space_used = self.hdfs.get_space_used()
        disk_free = self.hdfs.df()

        assert capacity > 0
        assert capacity > space_used
        assert disk_free == (capacity - space_used)

    def test_close(self):
        client = hdfs_test_client()
        assert client.is_open
        client.close()
        assert not client.is_open

        with pytest.raises(Exception):
            client.ls('/')

    def test_mkdir(self):
        path = pjoin(self.tmp_path, 'test-dir/test-dir')
        parent_path = pjoin(self.tmp_path, 'test-dir')

        self.hdfs.mkdir(path)
        assert self.hdfs.exists(path)

        self.hdfs.delete(parent_path, recursive=True)
        assert not self.hdfs.exists(path)

    def test_mv_rename(self):
        path = pjoin(self.tmp_path, 'mv-test')
        new_path = pjoin(self.tmp_path, 'mv-new-test')

        data = b'foobarbaz'
        with self.hdfs.open(path, 'wb') as f:
            f.write(data)

        assert self.hdfs.exists(path)
        self.hdfs.mv(path, new_path)
        assert not self.hdfs.exists(path)
        assert self.hdfs.exists(new_path)

        assert self.hdfs.cat(new_path) == data

        self.hdfs.rename(new_path, path)
        assert self.hdfs.cat(path) == data

    def test_info(self):
        path = pjoin(self.tmp_path, 'info-base')
        file_path = pjoin(path, 'ex')
        self.hdfs.mkdir(path)

        data = b'foobarbaz'
        with self.hdfs.open(file_path, 'wb') as f:
            f.write(data)

        path_info = self.hdfs.info(path)
        file_path_info = self.hdfs.info(file_path)

        assert path_info['kind'] == 'directory'

        assert file_path_info['kind'] == 'file'
        assert file_path_info['size'] == len(data)

    def test_disk_usage(self):
        path = pjoin(self.tmp_path, 'disk-usage-base')
        p1 = pjoin(path, 'p1')
        p2 = pjoin(path, 'p2')

        subdir = pjoin(path, 'subdir')
        p3 = pjoin(subdir, 'p3')

        if self.hdfs.exists(path):
            self.hdfs.delete(path, True)

        self.hdfs.mkdir(path)
        self.hdfs.mkdir(subdir)

        data = b'foobarbaz'

        for file_path in [p1, p2, p3]:
            with self.hdfs.open(file_path, 'wb') as f:
                f.write(data)

        assert self.hdfs.disk_usage(path) == len(data) * 3

    def test_ls(self):
        base_path = pjoin(self.tmp_path, 'ls-test')
        self.hdfs.mkdir(base_path)

        dir_path = pjoin(base_path, 'a-dir')
        f1_path = pjoin(base_path, 'a-file-1')

        self.hdfs.mkdir(dir_path)

        f = self.hdfs.open(f1_path, 'wb')
        f.write('a' * 10)

        contents = sorted(self.hdfs.ls(base_path, False))
        assert contents == [dir_path, f1_path]

    def test_chmod_chown(self):
        path = pjoin(self.tmp_path, 'chmod-test')
        with self.hdfs.open(path, 'wb') as f:
            f.write(b'a' * 10)

    def test_download_upload(self):
        base_path = pjoin(self.tmp_path, 'upload-test')

        data = b'foobarbaz'
        buf = BytesIO(data)
        buf.seek(0)

        self.hdfs.upload(base_path, buf)

        out_buf = BytesIO()
        self.hdfs.download(base_path, out_buf)
        out_buf.seek(0)
        assert out_buf.getvalue() == data

    def test_file_context_manager(self):
        path = pjoin(self.tmp_path, 'ctx-manager')

        data = b'foo'
        with self.hdfs.open(path, 'wb') as f:
            f.write(data)

        with self.hdfs.open(path, 'rb') as f:
            assert f.size() == 3
            result = f.read(10)
            assert result == data

    def test_read_whole_file(self):
        path = pjoin(self.tmp_path, 'read-whole-file')

        data = b'foo' * 1000
        with self.hdfs.open(path, 'wb') as f:
            f.write(data)

        with self.hdfs.open(path, 'rb') as f:
            result = f.read()

        assert result == data

    @test_parquet.parquet
    def test_read_multiple_parquet_files(self):
        import pyarrow.parquet as pq

        nfiles = 10
        size = 5

        tmpdir = pjoin(self.tmp_path, 'multi-parquet-' + guid())

        self.hdfs.mkdir(tmpdir)

        test_data = []
        paths = []
        for i in range(nfiles):
            df = test_parquet._test_dataframe(size, seed=i)

            df['index'] = np.arange(i * size, (i + 1) * size)

            # Hack so that we don't have a dtype cast in v1 files
            df['uint32'] = df['uint32'].astype(np.int64)

            path = pjoin(tmpdir, '{0}.parquet'.format(i))

            table = pa.Table.from_pandas(df, preserve_index=False)
            with self.hdfs.open(path, 'wb') as f:
                pq.write_table(table, f)

            test_data.append(table)
            paths.append(path)

        result = self.hdfs.read_parquet(tmpdir)
        expected = pa.concat_tables(test_data)

        pdt.assert_frame_equal(result.to_pandas()
                               .sort_values(by='index').reset_index(drop=True),
                               expected.to_pandas())

    @test_parquet.parquet
    def test_read_common_metadata_files(self):
        tmpdir = pjoin(self.tmp_path, 'common-metadata-' + guid())
        self.hdfs.mkdir(tmpdir)
        test_parquet._test_read_common_metadata_files(self.hdfs, tmpdir)


class TestLibHdfs(HdfsTestCases, unittest.TestCase):

    DRIVER = 'libhdfs'

    @classmethod
    def check_driver(cls):
        if not pa.have_libhdfs():
            pytest.fail('No libhdfs available on system')

    def test_orphaned_file(self):
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
        if not pa.have_libhdfs3():
            pytest.fail('No libhdfs3 available on system')
