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

from datetime import datetime
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib  # py2 compat

import pytest

import pyarrow as pa
from pyarrow.tests.test_io import (gzip_compress, gzip_decompress,
                                   assert_file_not_found)
from pyarrow.fs import (FileType, FileSelector, FileSystem, LocalFileSystem,
                        LocalFileSystemOptions, SubTreeFileSystem,
                        _MockFileSystem)


@pytest.fixture
def localfs(request, tempdir):
    return dict(
        fs=LocalFileSystem(),
        pathfn=lambda p: (tempdir / p).as_posix(),
        allow_copy_file=True,
        allow_move_dir=True,
        allow_append_to_file=True,
    )


@pytest.fixture
def mockfs(request):
    return dict(
        fs=_MockFileSystem(),
        pathfn=lambda p: p,
        allow_copy_file=True,
        allow_move_dir=True,
        allow_append_to_file=True,
    )


@pytest.fixture
def localfs_with_mmap(request, tempdir):
    return dict(
        fs=LocalFileSystem(use_mmap=True),
        pathfn=lambda p: (tempdir / p).as_posix(),
        allow_copy_file=True,
        allow_move_dir=True,
        allow_append_to_file=True,
    )


@pytest.fixture
def subtree_localfs(request, tempdir, localfs):
    prefix = 'subtree/prefix/'
    (tempdir / prefix).mkdir(parents=True)
    return dict(
        fs=SubTreeFileSystem(prefix, localfs['fs']),
        pathfn=prefix.__add__,
        allow_copy_file=True,
        allow_move_dir=True,
        allow_append_to_file=True,
    )


@pytest.fixture
def s3fs(request, minio_server):
    request.config.pyarrow.requires('s3')
    from pyarrow.fs import S3Options, S3FileSystem

    address, access_key, secret_key = minio_server
    bucket = 'pyarrow-filesystem/'
    options = S3Options(
        endpoint_override=address,
        access_key=access_key,
        secret_key=secret_key,
        scheme='http'
    )
    fs = S3FileSystem(options)
    fs.create_dir(bucket)

    return dict(
        fs=fs,
        pathfn=bucket.__add__,
        allow_copy_file=True,
        allow_move_dir=False,
        allow_append_to_file=False,
    )


@pytest.fixture
def subtree_s3fs(request, s3fs):
    prefix = 'pyarrow-filesystem/prefix/'
    return dict(
        fs=SubTreeFileSystem(prefix, s3fs['fs']),
        pathfn=prefix.__add__,
        allow_copy_file=True,
        allow_move_dir=False,
        allow_append_to_file=False,
    )


@pytest.fixture
def hdfs(request, hdfs_server):
    request.config.pyarrow.requires('hdfs')
    if not pa.have_libhdfs():
        pytest.skip('Cannot locate libhdfs')

    from pyarrow.fs import HdfsOptions, HadoopFileSystem

    host, port, user = hdfs_server
    options = HdfsOptions(endpoint=(host, port), user=user)

    fs = HadoopFileSystem(options)

    return dict(
        fs=fs,
        pathfn=lambda p: p,
        allow_copy_file=False,
        allow_move_dir=True,
        allow_append_to_file=True,
    )


@pytest.fixture(params=[
    pytest.param(
        pytest.lazy_fixture('localfs'),
        id='LocalFileSystem()'
    ),
    pytest.param(
        pytest.lazy_fixture('localfs_with_mmap'),
        id='LocalFileSystem(use_mmap=True)'
    ),
    pytest.param(
        pytest.lazy_fixture('subtree_localfs'),
        id='SubTreeFileSystem(LocalFileSystem())'
    ),
    pytest.param(
        pytest.lazy_fixture('s3fs'),
        id='S3FileSystem'
    ),
    pytest.param(
        pytest.lazy_fixture('hdfs'),
        id='HadoopFileSystem'
    ),
    pytest.param(
        pytest.lazy_fixture('mockfs'),
        id='_MockFileSystem()'
    ),
])
def filesystem_config(request):
    return request.param


@pytest.fixture
def fs(request, filesystem_config):
    return filesystem_config['fs']


@pytest.fixture
def pathfn(request, filesystem_config):
    return filesystem_config['pathfn']


@pytest.fixture
def allow_move_dir(request, filesystem_config):
    return filesystem_config['allow_move_dir']


@pytest.fixture
def allow_copy_file(request, filesystem_config):
    return filesystem_config['allow_copy_file']


@pytest.fixture
def allow_append_to_file(request, filesystem_config):
    return filesystem_config['allow_append_to_file']


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


def test_get_target_stats(fs, pathfn):
    aaa = pathfn('a/aa/aaa/')
    bb = pathfn('a/bb')
    c = pathfn('c.txt')
    zzz = pathfn('zzz')

    fs.create_dir(aaa)
    with fs.open_output_stream(bb):
        pass  # touch
    with fs.open_output_stream(c) as fp:
        fp.write(b'test')

    aaa_stat, bb_stat, c_stat, zzz_stat = \
        fs.get_target_stats([aaa, bb, c, zzz])

    assert aaa_stat.path == aaa
    assert 'aaa' in repr(aaa_stat)
    assert aaa_stat.extension == ''
    assert 'FileType.Directory' in repr(aaa_stat)
    assert isinstance(aaa_stat.mtime, datetime)

    assert bb_stat.path == str(bb)
    assert bb_stat.base_name == 'bb'
    assert bb_stat.extension == ''
    assert bb_stat.type == FileType.File
    assert 'FileType.File' in repr(bb_stat)
    assert bb_stat.size == 0
    assert isinstance(bb_stat.mtime, datetime)

    assert c_stat.path == str(c)
    assert c_stat.base_name == 'c.txt'
    assert c_stat.extension == 'txt'
    assert c_stat.type == FileType.File
    assert 'FileType.File' in repr(c_stat)
    assert c_stat.size == 4
    assert isinstance(c_stat.mtime, datetime)

    assert zzz_stat.path == str(zzz)
    assert zzz_stat.base_name == 'zzz'
    assert zzz_stat.extension == ''
    assert zzz_stat.type == FileType.NonExistent
    assert 'FileType.NonExistent' in repr(zzz_stat)
    assert isinstance(c_stat.mtime, datetime)


def test_get_target_stats_with_selector(fs, pathfn):
    base_dir = pathfn('selector-dir/')
    file_a = pathfn('selector-dir/test_file_a')
    file_b = pathfn('selector-dir/test_file_b')
    dir_a = pathfn('selector-dir/test_dir_a')

    try:
        fs.create_dir(base_dir)
        with fs.open_output_stream(file_a):
            pass
        with fs.open_output_stream(file_b):
            pass
        fs.create_dir(dir_a)

        selector = FileSelector(base_dir, allow_non_existent=False,
                                recursive=True)
        assert selector.base_dir == base_dir

        stats = fs.get_target_stats(selector)
        assert len(stats) == 3

        for st in stats:
            if st.path.endswith(file_a):
                assert st.type == FileType.File
            elif st.path.endswith(file_b):
                assert st.type == FileType.File
            elif st.path.endswith(dir_a):
                assert st.type == FileType.Directory
            else:
                raise ValueError('unexpected path {}'.format(st.path))
    finally:
        fs.delete_file(file_a)
        fs.delete_file(file_b)
        fs.delete_dir(dir_a)
        fs.delete_dir(base_dir)


def test_create_dir(fs, pathfn):
    d = pathfn('test-directory/')

    with pytest.raises(pa.ArrowIOError):
        fs.delete_dir(d)

    fs.create_dir(d)
    fs.delete_dir(d)

    d = pathfn('deeply/nested/test-directory/')
    fs.create_dir(d, recursive=True)
    fs.delete_dir(d)


def test_delete_dir(fs, pathfn):
    d = pathfn('directory/')
    nd = pathfn('directory/nested/')

    fs.create_dir(nd)
    fs.delete_dir(nd)
    fs.delete_dir(d)
    with pytest.raises(pa.ArrowIOError):
        fs.delete_dir(d)


def test_copy_file(fs, pathfn, allow_copy_file):
    s = pathfn('test-copy-source-file')
    t = pathfn('test-copy-target-file')

    with fs.open_output_stream(s):
        pass

    if allow_copy_file:
        fs.copy_file(s, t)
        fs.delete_file(s)
        fs.delete_file(t)
    else:
        with pytest.raises(pa.ArrowNotImplementedError):
            fs.copy_file(s, t)


def test_move_directory(fs, pathfn, allow_move_dir):
    # move directory (doesn't work with S3)
    s = pathfn('source-dir/')
    t = pathfn('target-dir/')

    fs.create_dir(s)

    if allow_move_dir:
        fs.move(s, t)
        with pytest.raises(pa.ArrowIOError):
            fs.delete_dir(s)
        fs.delete_dir(t)
    else:
        with pytest.raises(pa.ArrowIOError):
            fs.move(s, t)


def test_move_file(fs, pathfn):
    s = pathfn('test-move-source-file')
    t = pathfn('test-move-target-file')

    with fs.open_output_stream(s):
        pass

    fs.move(s, t)
    with pytest.raises(pa.ArrowIOError):
        fs.delete_file(s)
    fs.delete_file(t)


def test_delete_file(fs, pathfn):
    p = pathfn('test-delete-target-file')
    with fs.open_output_stream(p):
        pass

    fs.delete_file(p)
    with pytest.raises(pa.ArrowIOError):
        fs.delete_file(p)

    d = pathfn('test-delete-nested')
    fs.create_dir(d)
    f = pathfn('test-delete-nested/target-file')
    with fs.open_output_stream(f) as s:
        s.write(b'data')

    fs.delete_dir(d)


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
def test_open_input_stream(fs, pathfn, compression, buffer_size, compressor):
    p = pathfn('open-input-stream')

    data = b'some data for reading\n' * 512
    with fs.open_output_stream(p) as s:
        s.write(compressor(data))

    with fs.open_input_stream(p, compression, buffer_size) as s:
        result = s.read()

    assert result == data


def test_open_input_file(fs, pathfn):
    p = pathfn('open-input-file')

    data = b'some data' * 1024
    with fs.open_output_stream(p) as s:
        s.write(data)

    read_from = len(b'some data') * 512
    with fs.open_input_file(p) as f:
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
def test_open_output_stream(fs, pathfn, compression, buffer_size,
                            decompressor):
    p = pathfn('open-output-stream')

    data = b'some data for writing' * 1024
    with fs.open_output_stream(p, compression, buffer_size) as f:
        f.write(data)

    with fs.open_input_stream(p, compression, buffer_size) as f:
        assert f.read(len(data)) == data


@pytest.mark.parametrize(
    ('compression', 'buffer_size', 'compressor', 'decompressor'),
    [
        (None, None, identity, identity),
        (None, 64, identity, identity),
        ('gzip', None, gzip_compress, gzip_decompress),
        ('gzip', 256, gzip_compress, gzip_decompress),
    ]
)
def test_open_append_stream(fs, pathfn, compression, buffer_size, compressor,
                            decompressor, allow_append_to_file):
    p = pathfn('open-append-stream')

    initial = compressor(b'already existing')
    with fs.open_output_stream(p) as s:
        s.write(initial)

    if allow_append_to_file:
        with fs.open_append_stream(p, compression=compression,
                                   buffer_size=buffer_size) as f:
            f.write(b'\nnewly added')

        with fs.open_input_stream(p) as f:
            result = f.read()

        result = decompressor(result)
        assert result == b'already existing\nnewly added'
    else:
        with pytest.raises(pa.ArrowNotImplementedError):
            fs.open_append_stream(p, compression=compression,
                                  buffer_size=buffer_size)


def test_localfs_options():
    options = LocalFileSystemOptions()
    assert options.use_mmap is False
    options.use_mmap = True
    assert options.use_mmap is True

    with pytest.raises(AttributeError):
        options.xxx = True

    options = LocalFileSystemOptions(use_mmap=True)
    assert options.use_mmap is True

    # LocalFileSystem instantiation
    LocalFileSystem(LocalFileSystemOptions(use_mmap=True))
    LocalFileSystem(use_mmap=False)

    with pytest.raises(AttributeError):
        LocalFileSystem(xxx=False)


def test_localfs_errors(localfs):
    # Local filesystem errors should raise the right Python exceptions
    # (e.g. FileNotFoundError)
    fs = localfs['fs']
    with assert_file_not_found():
        fs.open_input_stream('/non/existent/file')
    with assert_file_not_found():
        fs.open_output_stream('/non/existent/file')
    with assert_file_not_found():
        fs.create_dir('/non/existent/dir', recursive=False)
    with assert_file_not_found():
        fs.delete_dir('/non/existent/dir')
    with assert_file_not_found():
        fs.delete_file('/non/existent/dir')
    with assert_file_not_found():
        fs.move('/non/existent', '/xxx')
    with assert_file_not_found():
        fs.copy_file('/non/existent', '/xxx')


@pytest.mark.s3
def test_s3_options(minio_server):
    from pyarrow.fs import S3Options

    options = S3Options()

    assert options.region == 'us-east-1'
    options.region = 'us-west-1'
    assert options.region == 'us-west-1'

    assert options.scheme == 'https'
    options.scheme = 'http'
    assert options.scheme == 'http'

    assert options.endpoint_override == ''
    options.endpoint_override = 'localhost:8999'
    assert options.endpoint_override == 'localhost:8999'

    with pytest.raises(ValueError):
        S3Options(access_key='access')
    with pytest.raises(ValueError):
        S3Options(secret_key='secret')

    address, access_key, secret_key = minio_server
    options = S3Options(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=address,
        scheme='http'
    )
    assert options.scheme == 'http'
    assert options.endpoint_override == address


@pytest.mark.hdfs
def test_hdfs_options(hdfs_server):
    from pyarrow.fs import HdfsOptions, HadoopFileSystem
    if not pa.have_libhdfs():
        pytest.skip('Cannot locate libhdfs')

    options = HdfsOptions()
    assert options.endpoint == ('', 0)
    options.endpoint = ('localhost', 8080)
    assert options.endpoint == ('localhost', 8080)
    with pytest.raises(TypeError):
        options.endpoint = 'localhost:8000'

    assert options.driver == 'libhdfs'
    options.driver = 'libhdfs3'
    assert options.driver == 'libhdfs3'
    with pytest.raises(ValueError):
        options.driver = 'unknown'

    assert options.replication == 3
    options.replication = 2
    assert options.replication == 2

    assert options.user == ''
    options.user = 'libhdfs'
    assert options.user == 'libhdfs'

    assert options.default_block_size == 0
    options.default_block_size = 128*1024**2
    assert options.default_block_size == 128*1024**2

    assert options.buffer_size == 0
    options.buffer_size = 64*1024
    assert options.buffer_size == 64*1024

    options = HdfsOptions.from_uri('hdfs://localhost:8080/?user=test')
    assert options.endpoint == ('hdfs://localhost', 8080)
    assert options.user == 'test'

    host, port, user = hdfs_server
    uri = "hdfs://{}:{}/?user={}".format(host, port, user)
    fs = HadoopFileSystem(uri)
    assert fs.get_target_stats(FileSelector('/'))


@pytest.mark.parametrize(('uri', 'expected_klass', 'expected_path'), [
    # leading slashes are removed intentionally, becuase MockFileSystem doesn't
    # have a distinction between relative and absolute paths
    ('mock:', _MockFileSystem, ''),
    ('mock:foo/bar', _MockFileSystem, 'foo/bar'),
    ('mock:/foo/bar', _MockFileSystem, 'foo/bar'),
    ('mock:///foo/bar', _MockFileSystem, 'foo/bar'),
    ('file:', LocalFileSystem, ''),
    ('file:foo/bar', LocalFileSystem, 'foo/bar'),
    ('file:/foo/bar', LocalFileSystem, '/foo/bar'),
    ('file:///foo/bar', LocalFileSystem, '/foo/bar'),
    ('', LocalFileSystem, ''),
    ('foo/bar', LocalFileSystem, 'foo/bar'),
    ('/foo/bar', LocalFileSystem, '/foo/bar'),
])
def test_filesystem_from_uri(uri, expected_klass, expected_path):
    fs, path = FileSystem.from_uri(uri)
    assert isinstance(fs, expected_klass)
    assert path == expected_path
