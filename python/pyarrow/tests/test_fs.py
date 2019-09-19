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
import os
import subprocess
import tempfile
from datetime import datetime
try:

    import pathlib
except ImportError:
    import pathlib2 as pathlib  # py2 compat

import pytest

from pyarrow import ArrowIOError
from pyarrow.tests.test_io import gzip_compress, gzip_decompress
from pyarrow.fs import (FileType, Selector, FileSystem, LocalFileSystem,
                        SubTreeFileSystem)


class S3Path:

    def __init__(self, path, minio_client):
        self.client = minio_client
        self.path = path

    @property
    def bucket_name(self):
        return self.path.split('/', 1)[0]

    @property
    def object_name(self):
        return self.path.split('/', 1)[1]

    @property
    def parent(self):
        parent, _ = self.path.rsplit('/', 1)
        return S3Path(parent, minio_client=self.client)

    def as_posix(self):
        return self.path  # always posix

    def touch(self):
        object_name = self.object_name.rstrip('/')
        self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=io.BytesIO(b''),
            length=0
        )

    def mkdir(self, parents=True):
        object_name = self.object_name
        if not object_name.endswith('/'):
            object_name += '/'
        self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=io.BytesIO(b''),
            length=0
        )

    def exists(self):
        from minio.error import ResponseError, NoSuchKey, NoSuchBucket

        try:
            self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=self.object_name
            )
        except (NoSuchBucket, NoSuchKey):
            return False
        else:
            return True

    def write_bytes(self, content):
        assert not self.object_name.endswith('/')
        self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=self.object_name,
            data=io.BytesIO(content),
            length=len(content)
        )

    def read_bytes(self):
        assert not self.object_name.endswith('/')
        data = self.client.get_object(
            bucket_name=self.bucket_name,
            object_name=self.object_name
        )
        return data.read()


@pytest.fixture(scope='module')
@pytest.mark.s3
def minio_server():
    host, port = 'localhost', 9000
    access_key, secret_key = 'arrow', 'apachearrow'

    address = '{}:{}'.format(host, port)
    env = os.environ.copy()
    env.update({
        'MINIO_ACCESS_KEY': access_key,
        'MINIO_SECRET_KEY': secret_key
    })

    try:
        with tempfile.TemporaryDirectory() as tempdir:
            args = ['minio', '--compat', 'server', '--address', address,
                    tempdir]
            with subprocess.Popen(args, env=env) as proc:
                yield address, access_key, secret_key
                proc.terminate()
    except FileNotFoundError:
        pytest.skip('Minio executable cannot be located')


@pytest.fixture(scope='module')
def minio_client(minio_server):
    from minio import Minio
    address, access_key, secret_key = minio_server
    client = Minio(address, access_key=access_key, secret_key=secret_key,
                   secure=False)
    client.make_bucket('bucket')
    return client


@pytest.fixture
def localfs(tempdir):
    def local_paths(p):
        path = tempdir / p
        return (path.as_posix(), path)
    return (LocalFileSystem(), local_paths)


@pytest.fixture
def s3fs(minio_server, minio_client):
    from pyarrow.fs import S3FileSystem, initialize_s3

    def s3_paths(p):
        path = S3Path('bucket/{}'.format(p), minio_client=minio_client)
        return (path.as_posix(), path)

    initialize_s3()
    address, access_key, secret_key = minio_server
    fs = S3FileSystem(access_key=access_key, secret_key=secret_key,
                      endpoint_override=address, scheme='http')

    return (fs, s3_paths)


@pytest.fixture(params=[
    pytest.lazy_fixture('localfs'),
    pytest.lazy_fixture('s3fs')
])
def subtreefs(request):
    fs = SubTreeFileSystem('', request.param[0])
    return (fs, lambda p: p)


@pytest.fixture(params=[
    pytest.lazy_fixture('localfs'),
    pytest.lazy_fixture('s3fs'),
    # pytest.lazy_fixture('subtreefs'),
])
def filesystem(request):
    return request.param


@pytest.fixture
def fs(request, filesystem):
    return filesystem[0]


@pytest.fixture
def paths(request, filesystem):
    return filesystem[1]


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
            fs.create_dir(path)


@pytest.mark.skip()
def test_get_target_stats(fs, tempdir, testpath):
    aaa, aaa_ = testpath('a/aa/aaa'), tempdir / 'a' / 'aa' / 'aaa'
    bb, bb_ = testpath('a/bb'), tempdir / 'a' / 'bb'
    c, c_ = testpath('c.txt'), tempdir / 'c.txt'

    aaa_.mkdir(parents=True)
    bb_.touch()
    c_.write_bytes(b'test')

    def mtime_almost_equal(fs_dt, pathlib_ts):
        # arrow's filesystem implementation truncates mtime to microsends
        # resolution whereas pathlib rounds
        pathlib_dt = datetime.utcfromtimestamp(pathlib_ts)
        difference = (fs_dt - pathlib_dt).total_seconds()
        return abs(difference) <= 10**-6

    aaa_stat, bb_stat, c_stat = fs.get_target_stats([aaa, bb, c])

    assert aaa_stat.path == aaa
    assert 'aaa' in repr(aaa_stat)
    assert aaa_stat.base_name == 'aaa'
    assert aaa_stat.extension == ''
    assert aaa_stat.type == FileType.Directory
    assert mtime_almost_equal(aaa_stat.mtime, aaa_.stat().st_mtime)
    with pytest.raises(ValueError):
        aaa_stat.size

    assert bb_stat.path == str(bb)
    assert bb_stat.base_name == 'bb'
    assert bb_stat.extension == ''
    assert bb_stat.type == FileType.File
    assert bb_stat.size == 0
    assert mtime_almost_equal(bb_stat.mtime, bb_.stat().st_mtime)

    assert c_stat.path == str(c)
    assert c_stat.base_name == 'c.txt'
    assert c_stat.extension == 'txt'
    assert c_stat.type == FileType.File
    assert c_stat.size == 4
    assert mtime_almost_equal(c_stat.mtime, c_.stat().st_mtime)


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


def test_create_dir(fs, paths):
    directory, directory_ = paths('test-directory/')
    assert not directory_.exists()
    fs.create_dir(directory)
    from pyarrow.fs import S3FileSystem
    assert directory_.exists()

    # recursive
    directory, directory_ = paths('deeply/nested/directory/')
    assert not directory_.exists()
    with pytest.raises(ArrowIOError):
        fs.create_dir(directory, recursive=False)
    fs.create_dir(directory)
    assert directory_.exists()


def test_delete_dir(fs, paths):
    folder, folder_ = paths('directory/')
    nested, nested_ = paths('nested/directory/')

    folder_.mkdir()
    nested_.mkdir(parents=True)

    assert folder_.exists()
    fs.delete_dir(folder)
    assert not folder_.exists()

    assert nested_.exists()
    fs.delete_dir(nested)
    assert not nested_.exists()


def test_copy_file(fs, paths):
    # copy file
    source, source_ = paths('test-copy-source-file')
    source_.touch()
    target, target_ = paths('test-copy-target-file')

    assert not target_.exists()
    fs.copy_file(source, target)
    assert source_.exists()
    assert target_.exists()


def test_move(fs, paths):
    # # move directory (doesn't work with S3)
    # source, source_ = paths('source-dir/')
    # source_.mkdir()
    # target, target_ = paths('target-dir/')
    # assert source_.exists()
    # assert not target_.exists()

    # fs.move(source, target)
    # assert not source_.exists()
    # assert target_.exists()

    # move file
    source, source_ = paths('test-move-source-file')
    source_.touch()
    target, target_ = paths('test-move-target-file')
    assert source_.exists()
    assert not target_.exists()

    fs.move(source, target)
    assert not source_.exists()
    assert target_.exists()


def test_delete_file(fs, paths):
    target, target_ = paths('test-delete-target-file')
    target_.touch()
    assert target_.exists()
    fs.delete_file(target)
    assert not target_.exists()

    nested, nested_ = paths('test-delete-nested/target-file')
    nested_.parent.mkdir()
    nested_.touch()
    assert nested_.exists()
    fs.delete_file(nested)
    assert not nested_.exists()


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
def test_open_input_stream(fs, paths, compression, buffer_size, compressor):
    file, file_ = paths('open-input-stream')

    data = b'some data for reading' * 1024
    file_.write_bytes(compressor(data))

    with fs.open_input_stream(file, compression, buffer_size) as f:
        result = f.read(len(data))

    assert result == data


def test_open_input_file(fs, paths):
    file, file_ = paths('open-input-file')
    data = b'some data' * 1024
    file_.write_bytes(data)

    read_from = len(b'some data') * 512
    with fs.open_input_file(file) as f:
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
def test_open_output_stream(fs, paths, compression, buffer_size, decompressor):
    file, file_ = paths('open-output-stream-1')

    data = b'some data for writing' * 1024
    with fs.open_output_stream(file, compression, buffer_size) as f:
        f.write(data)

    with fs.open_input_stream(file, compression, buffer_size) as f:
        assert f.read(len(data)) == data

    assert decompressor(file_.read_bytes()) == data


@pytest.mark.parametrize(
    ('compression', 'buffer_size', 'compressor', 'decompressor'),
    [
        (None, None, identity, identity),
        (None, 64, identity, identity),
        ('gzip', None, gzip_compress, gzip_decompress),
        ('gzip', 256, gzip_compress, gzip_decompress),
    ]
)
def test_open_append_stream(fs, paths, compression, buffer_size, compressor,
                            decompressor):
    file, file_ = paths('open-append-stream')
    file_.write_bytes(compressor(b'already existing'))

    with fs.open_append_stream(file, compression, buffer_size) as f:
        f.write(b'\nnewly added')

    assert decompressor(file_.read_bytes()) == b'already existing\nnewly added'