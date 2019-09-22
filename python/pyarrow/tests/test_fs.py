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

import io
import calendar
from datetime import datetime
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib  # py2 compat

import pytest

import pyarrow as pa
from pyarrow import ArrowIOError
from pyarrow.tests.test_io import gzip_compress, gzip_decompress
from pyarrow.fs import (FileType, Selector, FileSystem, LocalFileSystem,
                        SubTreeFileSystem)


class FileSystemWrapper:

    # Whether the filesystem may "implicitly" create intermediate directories
    have_implicit_directories = False
    # Whether the filesystem may allow writing a file "over" a directory
    allow_write_file_over_dir = False
    # Whether the filesystem allows moving a directory
    allow_move_dir = True
    # Whether the filesystem allows appending to a file
    allow_append_to_file = False
    # Whether the filesystem supports directory modification times
    have_directory_mtimes = True

    @property
    def impl(self):
        return self._impl

    @impl.setter
    def impl(self, impl):
        self._impl = impl

    def pathpair(self, p):
        raise NotImplementedError()

    def mkdir(self, p):
        raise NotImplementedError()

    def touch(self, p):
        raise NotImplementedError()

    def exists(self, p):
        raise NotImplementedError()

    def mtime(self, p):
        raise NotImplementedError()

    def write_bytes(self, p, data):
        raise NotImplementedError()

    def read_bytes(self, p):
        raise NotImplementedError()


class LocalWrapper(FileSystemWrapper):

    allow_append_to_file = True

    def __init__(self, tempdir):
        self.impl = LocalFileSystem()
        self.tempdir = tempdir

    def pathpair(self, p):
        path_for_wrapper = str(self.tempdir / p)
        path_for_impl = '/'.join([self.tempdir.as_posix(), p])
        return (path_for_wrapper, path_for_impl)

    def mkdir(self, p):
        return pathlib.Path(p).mkdir(parents=True)

    def touch(self, p):
        return pathlib.Path(p).touch()

    def exists(self, p):
        return pathlib.Path(p).exists()

    def mtime(self, p):
        path = pathlib.Path(p)
        mtime = path.stat().st_mtime
        return datetime.utcfromtimestamp(mtime)

    def write_bytes(self, p, data):
        return pathlib.Path(p).write_bytes(data)

    def read_bytes(self, p):
        return pathlib.Path(p).read_bytes()


class SubTreeLocalWrapper(LocalWrapper):

    def __init__(self, tempdir, prefix='local/prefix'):
        prefix_absolute = tempdir / prefix
        prefix_absolute.mkdir(parents=True)

        self.impl = SubTreeFileSystem(
            prefix_absolute.as_posix(),
            LocalFileSystem()
        )
        self.prefix = prefix
        self.tempdir = tempdir

    def pathpair(self, p):
        path_for_wrapper = str(self.tempdir / self.prefix / p)
        path_for_impl = p
        return (path_for_wrapper, path_for_impl)


class S3Wrapper(FileSystemWrapper):

    allow_move_dir = False

    def __init__(self, minio_client, bucket='test-bucket', **kwargs):
        from pyarrow.fs import S3FileSystem
        self.impl = S3FileSystem(**kwargs)
        self.client = minio_client
        self.bucket = bucket

    def pathpair(self, p):
        path_for_wrapper = p
        path_for_impl = '/'.join([self.bucket, p])
        return (path_for_wrapper, path_for_impl)

    def touch(self, p):
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=p.rstrip('/'),
            data=io.BytesIO(b''),
            length=0
        )

    def mkdir(self, p):
        if not p.endswith('/'):
            p += '/'
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=p,
            data=io.BytesIO(b''),
            length=0
        )

    def exists(self, p):
        from minio.error import NoSuchKey, NoSuchBucket
        try:
            self.client.get_object(
                bucket_name=self.bucket,
                object_name=p
            )
        except (NoSuchBucket, NoSuchKey):
            return False
        else:
            return True

    def mtime(self, p):
        stat = self.client.stat_object(
            bucket_name=self.bucket,
            object_name=p
        )
        ts = calendar.timegm(stat.last_modified)
        return datetime.utcfromtimestamp(ts)

    def write_bytes(self, p, data):
        assert not p.endswith('/')
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=p,
            data=io.BytesIO(data),
            length=len(data)
        )

    def read_bytes(self, p):
        assert not p.endswith('/')
        data = self.client.get_object(
            bucket_name=self.bucket,
            object_name=p
        )
        return data.read()


class SubTreeS3Wrapper(S3Wrapper):

    def __init__(self, minio_client, bucket='test-bucket', prefix='s3/prefix',
                 **kwargs):
        from pyarrow.fs import S3FileSystem
        self.impl = SubTreeFileSystem(
            '/'.join([bucket, prefix]),
            S3FileSystem(**kwargs)
        )
        self.client = minio_client
        self.bucket = bucket
        self.prefix = prefix

    def pathpair(self, p):
        path_for_wrapper = '/'.join([self.prefix, p])
        path_for_impl = p
        return (path_for_wrapper, path_for_impl)


@pytest.fixture(params=[
    LocalWrapper,
    SubTreeLocalWrapper
])
def localfs(request, tempdir):
    return request.param(tempdir)


@pytest.fixture(params=[
    S3Wrapper,
    SubTreeS3Wrapper
])
def s3fs(request, minio_server, minio_client, minio_bucket):
    from pyarrow.fs import initialize_s3
    initialize_s3()

    address, access_key, secret_key = minio_server
    return request.param(
        minio_client=minio_client,
        bucket=minio_bucket,
        endpoint_override=address,
        access_key=access_key,
        secret_key=secret_key,
        scheme='http'
    )


@pytest.fixture(params=[
    pytest.lazy_fixture('localfs'),
    pytest.lazy_fixture('s3fs'),
])
def fs(request):
    return request.param


def test_cannot_instantiate_base_filesystem():
    with pytest.raises(TypeError):
        FileSystem()


def test_non_path_like_input_raises(fs):
    class Path:
        pass

    invalid_paths = [1, 1.1, Path(), tuple(), {}, [], lambda: 1,
                     pathlib.Path()]
    for path in invalid_paths:
        with pytest.raises(TypeError):
            fs.impl.create_dir(path)


def test_get_target_stats(fs):
    _aaa, aaa = fs.pathpair('a/aa/aaa/')
    _bb, bb = fs.pathpair('a/bb')
    _c, c = fs.pathpair('c.txt')

    fs.mkdir(_aaa)
    fs.touch(_bb)
    fs.write_bytes(_c, b'test')

    def mtime_almost_equal(a, b):
        # arrow's filesystem implementation truncates mtime to microsends
        # resolution whereas pathlib rounds
        diff = (a - b).total_seconds()
        return abs(diff) <= 10**-6

    aaa_stat, bb_stat, c_stat = fs.impl.get_target_stats([aaa, bb, c])

    assert aaa_stat.path == aaa
    assert 'aaa' in repr(aaa_stat)
    assert aaa_stat.extension == ''
    assert mtime_almost_equal(aaa_stat.mtime, fs.mtime(_aaa))
    # type is inconsistent base_name has a trailing slas for 'aaa' and 'aaa/'
    # assert aaa_stat.base_name == 'aaa'
    # assert aaa_stat.type == FileType.Directory
    # assert aaa_stat is None

    assert bb_stat.path == str(bb)
    assert bb_stat.base_name == 'bb'
    assert bb_stat.extension == ''
    assert bb_stat.type == FileType.File
    assert bb_stat.size == 0
    assert mtime_almost_equal(bb_stat.mtime, fs.mtime(_bb))

    assert c_stat.path == str(c)
    assert c_stat.base_name == 'c.txt'
    assert c_stat.extension == 'txt'
    assert c_stat.type == FileType.File
    assert c_stat.size == 4
    assert mtime_almost_equal(c_stat.mtime, fs.mtime(_c))


@pytest.mark.skip()
def test_get_target_stats_with_selector(fs, tempdir, testpath):
    base_dir = testpath('.')
    base_dir_ = tempdir

    selector = Selector(base_dir, allow_non_existent=False, recursive=True)
    assert selector.base_dir == str(base_dir)

    (tempdir / 'test_file').touch()
    (tempdir / 'test_directory').mkdir()

    stats = fs.get_target_stats(selector)
    expected = list(base_dir_.iterdir())
    assert len(stats) == len(expected)

    for st in stats:
        p = base_dir_ / st.path
        if p.is_dir():
            assert st.type == FileType.Directory
        if p.is_file():
            assert st.type == FileType.File


def test_create_dir(fs):
    _d, d = fs.pathpair('test-directory/')
    assert not fs.exists(_d)
    fs.impl.create_dir(d)
    assert fs.exists(_d)

    # recursive
    _r, r = fs.pathpair('deeply/nested/directory/')
    assert not fs.exists(_r)
    with pytest.raises(ArrowIOError):
        fs.impl.create_dir(r, recursive=False)
    fs.impl.create_dir(r)
    assert fs.exists(_r)


def test_delete_dir(fs):
    _d, d = fs.pathpair('directory/')
    _nd, nd = fs.pathpair('directory/nested/')
    fs.mkdir(_nd)

    assert fs.exists(_nd)
    fs.impl.delete_dir(nd)
    assert not fs.exists(_nd)

    assert fs.exists(_d)
    fs.impl.delete_dir(d)
    assert not fs.exists(_d)


def test_copy_file(fs):
    _s, s = fs.pathpair('test-copy-source-file')
    _t, t = fs.pathpair('test-copy-target-file')
    fs.touch(_s)

    assert not fs.exists(_t)
    fs.impl.copy_file(s, t)
    assert fs.exists(_s)
    assert fs.exists(_t)


def test_move_directory(fs):
    # move directory (doesn't work with S3)
    _s, s = fs.pathpair('source-dir/')
    _t, t = fs.pathpair('target-dir/')
    fs.mkdir(_s)

    if fs.allow_move_dir:
        assert fs.exists(_s)
        assert not fs.exists(_t)
        fs.impl.move(s, t)
        assert not fs.exists(_s)
        assert fs.exists(_t)
    else:
        with pytest.raises(pa.ArrowIOError):
            fs.impl.move(s, t)


def test_move_file(fs):
    _s, s = fs.pathpair('test-move-source-file')
    _t, t = fs.pathpair('test-move-target-file')
    fs.touch(_s)

    assert fs.exists(_s)
    assert not fs.exists(_t)
    fs.impl.move(s, t)
    assert not fs.exists(_s)
    assert fs.exists(_t)


def test_delete_file(fs):
    _p, p = fs.pathpair('test-delete-target-file')
    fs.touch(_p)

    assert fs.exists(_p)
    fs.impl.delete_file(p)
    assert not fs.exists(_p)

    _p, p = fs.pathpair('test-delete-nested')
    fs.mkdir(_p)

    _p, p = fs.pathpair('test-delete-nested/target-file')
    fs.touch(_p)

    assert fs.exists(_p)
    fs.impl.delete_file(p)
    assert not fs.exists(_p)


def identity(v):
    return v


@pytest.mark.parametrize(
    ('compression', 'buffer_size', 'compressor'),
    [
        (None, None, identity),
        (None, 64, identity),
        ('gzip', None, gzip_compress),
        ('gzip', 256, gzip_compress),
    ]
)
def test_open_input_stream(fs, compression, buffer_size, compressor):
    _p, p = fs.pathpair('open-input-stream')

    data = b'some data for reading' * 1024
    fs.write_bytes(_p, compressor(data))

    with fs.impl.open_input_stream(p, compression, buffer_size) as f:
        result = f.read(len(data))

    assert result == data


def test_open_input_file(fs):
    _p, p = fs.pathpair('open-input-file')

    data = b'some data' * 1024
    fs.write_bytes(_p, data)

    read_from = len(b'some data') * 512
    with fs.impl.open_input_file(p) as f:
        f.seek(read_from)
        result = f.read()

    assert result == data[read_from:]


@pytest.mark.parametrize(
    ('compression', 'buffer_size', 'decompressor'),
    [
        (None, None, identity),
        (None, 64, identity),
        ('gzip', None, gzip_decompress),
        ('gzip', 256, gzip_decompress),
    ]
)
def test_open_output_stream(fs, compression, buffer_size, decompressor):
    _p, p = fs.pathpair('open-output-stream')

    data = b'some data for writing' * 1024
    with fs.impl.open_output_stream(p, compression, buffer_size) as f:
        f.write(data)

    with fs.impl.open_input_stream(p, compression, buffer_size) as f:
        assert f.read(len(data)) == data

    result = decompressor(fs.read_bytes(_p))
    assert result == data


@pytest.mark.parametrize(
    ('compression', 'buffer_size', 'compressor', 'decompressor'),
    [
        (None, None, identity, identity),
        (None, 64, identity, identity),
        ('gzip', None, gzip_compress, gzip_decompress),
        ('gzip', 256, gzip_compress, gzip_decompress),
    ]
)
def test_open_append_stream(fs, compression, buffer_size, compressor,
                            decompressor):
    _p, p = fs.pathpair('open-append-stream')

    data = compressor(b'already existing')
    fs.write_bytes(_p, data)

    if fs.allow_append_to_file:
        with fs.impl.open_append_stream(p, compression, buffer_size) as f:
            f.write(b'\nnewly added')
        result = decompressor(fs.read_bytes(_p))
        assert result == b'already existing\nnewly added'
    else:
        with pytest.raises(pa.ArrowNotImplementedError):
            fs.impl.open_append_stream(p, compression, buffer_size)
