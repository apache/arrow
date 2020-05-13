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

from datetime import datetime, timezone, timedelta
import gzip
import pathlib
import pickle
import sys

import pytest

import pyarrow as pa
from pyarrow.tests.test_io import assert_file_not_found
from pyarrow.fs import (FileType, FileSelector, FileSystem, LocalFileSystem,
                        SubTreeFileSystem,
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
    return dict(
        fs=SubTreeFileSystem(str(tempdir), localfs['fs']),
        pathfn=lambda p: p,
        allow_copy_file=True,
        allow_move_dir=True,
        allow_append_to_file=True,
    )


@pytest.fixture
def s3fs(request, s3_connection, s3_server):
    request.config.pyarrow.requires('s3')
    from pyarrow.fs import S3FileSystem

    host, port, access_key, secret_key = s3_connection
    bucket = 'pyarrow-filesystem/'

    fs = S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override='{}:{}'.format(host, port),
        scheme='http'
    )
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
def hdfs(request, hdfs_connection):
    request.config.pyarrow.requires('hdfs')
    if not pa.have_libhdfs():
        pytest.skip('Cannot locate libhdfs')

    from pyarrow.fs import HadoopFileSystem

    host, port, user = hdfs_connection
    fs = HadoopFileSystem(host, port=port, user=user)

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


def test_filesystem_equals():
    fs0 = LocalFileSystem()
    fs1 = LocalFileSystem()
    fs2 = _MockFileSystem()

    assert fs0.equals(fs0)
    assert fs0.equals(fs1)
    with pytest.raises(TypeError):
        fs0.equals('string')
    assert fs0 == fs0 == fs1
    assert fs0 != 4

    assert fs2 == fs2
    assert fs2 != _MockFileSystem()

    assert SubTreeFileSystem('/base', fs0) == SubTreeFileSystem('/base', fs0)
    assert SubTreeFileSystem('/base', fs0) != SubTreeFileSystem('/base', fs2)
    assert SubTreeFileSystem('/base', fs0) != SubTreeFileSystem('/other', fs0)


def test_subtree_filesystem():
    localfs = LocalFileSystem()

    subfs = SubTreeFileSystem('/base', localfs)
    assert subfs.base_path == '/base/'
    assert subfs.base_fs == localfs

    subfs = SubTreeFileSystem('/another/base/', LocalFileSystem())
    assert subfs.base_path == '/another/base/'
    assert subfs.base_fs == localfs


def test_filesystem_pickling(fs):
    if isinstance(fs, _MockFileSystem):
        pytest.xfail(reason='MockFileSystem is not serializable')

    serialized = pickle.dumps(fs)
    restored = pickle.loads(serialized)
    assert isinstance(restored, FileSystem)
    assert restored.equals(fs)


def test_filesystem_is_functional_after_pickling(fs, pathfn):
    if isinstance(fs, _MockFileSystem):
        pytest.xfail(reason='MockFileSystem is not serializable')

    aaa = pathfn('a/aa/aaa/')
    bb = pathfn('a/bb')
    c = pathfn('c.txt')

    fs.create_dir(aaa)
    with fs.open_output_stream(bb):
        pass  # touch
    with fs.open_output_stream(c) as fp:
        fp.write(b'test')

    restored = pickle.loads(pickle.dumps(fs))
    aaa_info, bb_info, c_info = restored.get_file_info([aaa, bb, c])
    assert aaa_info.type == FileType.Directory
    assert bb_info.type == FileType.File
    assert c_info.type == FileType.File


def test_non_path_like_input_raises(fs):
    class Path:
        pass

    invalid_paths = [1, 1.1, Path(), tuple(), {}, [], lambda: 1,
                     pathlib.Path()]
    for path in invalid_paths:
        with pytest.raises(TypeError):
            fs.create_dir(path)


def check_mtime(file_info):
    assert isinstance(file_info.mtime, datetime)
    assert isinstance(file_info.mtime_ns, int)
    if file_info.mtime_ns >= 0:
        assert file_info.mtime_ns == pytest.approx(
            file_info.mtime.timestamp() * 1e9)
        # It's an aware UTC datetime
        tzinfo = file_info.mtime.tzinfo
        assert tzinfo is not None
        assert tzinfo.utcoffset(None) == timedelta(0)


def test_get_file_info(fs, pathfn):
    aaa = pathfn('a/aa/aaa/')
    bb = pathfn('a/bb')
    c = pathfn('c.txt')
    zzz = pathfn('zzz')

    fs.create_dir(aaa)
    with fs.open_output_stream(bb):
        pass  # touch
    with fs.open_output_stream(c) as fp:
        fp.write(b'test')

    aaa_info, bb_info, c_info, zzz_info = fs.get_file_info([aaa, bb, c, zzz])

    assert aaa_info.path == aaa
    assert 'aaa' in repr(aaa_info)
    assert aaa_info.extension == ''
    assert 'FileType.Directory' in repr(aaa_info)
    check_mtime(aaa_info)

    assert bb_info.path == str(bb)
    assert bb_info.base_name == 'bb'
    assert bb_info.extension == ''
    assert bb_info.type == FileType.File
    assert 'FileType.File' in repr(bb_info)
    assert bb_info.size == 0
    check_mtime(bb_info)

    assert c_info.path == str(c)
    assert c_info.base_name == 'c.txt'
    assert c_info.extension == 'txt'
    assert c_info.type == FileType.File
    assert 'FileType.File' in repr(c_info)
    assert c_info.size == 4
    check_mtime(c_info)

    assert zzz_info.path == str(zzz)
    assert zzz_info.base_name == 'zzz'
    assert zzz_info.extension == ''
    assert zzz_info.type == FileType.NotFound
    assert 'FileType.NotFound' in repr(zzz_info)
    check_mtime(zzz_info)


def test_get_file_info_with_selector(fs, pathfn):
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

        selector = FileSelector(base_dir, allow_not_found=False,
                                recursive=True)
        assert selector.base_dir == base_dir

        infos = fs.get_file_info(selector)
        assert len(infos) == 3

        for info in infos:
            if info.path.endswith(file_a):
                assert info.type == FileType.File
            elif info.path.endswith(file_b):
                assert info.type == FileType.File
            elif info.path.endswith(dir_a):
                assert info.type == FileType.Directory
            else:
                raise ValueError('unexpected path {}'.format(info.path))
            check_mtime(info)
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
        ('gzip', None, gzip.compress),
        ('gzip', 256, gzip.compress),
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
        ('gzip', None, gzip.decompress),
        ('gzip', 256, gzip.decompress),
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
        ('gzip', None, gzip.compress, gzip.decompress),
        ('gzip', 256, gzip.compress, gzip.decompress),
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
    # LocalFileSystem instantiation
    LocalFileSystem(use_mmap=False)

    with pytest.raises(TypeError):
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


def test_localfs_file_info(localfs):
    fs = localfs['fs']

    file_path = pathlib.Path(__file__)
    dir_path = file_path.parent
    [file_info, dir_info] = fs.get_file_info([file_path.as_posix(),
                                              dir_path.as_posix()])
    assert file_info.size == file_path.stat().st_size
    assert file_info.mtime_ns == file_path.stat().st_mtime_ns
    check_mtime(file_info)
    assert dir_info.mtime_ns == dir_path.stat().st_mtime_ns
    check_mtime(dir_info)


def test_mockfs_mtime_roundtrip(mockfs):
    dt = datetime.fromtimestamp(1568799826, timezone.utc)
    fs = _MockFileSystem(dt)

    with fs.open_output_stream('foo'):
        pass
    [info] = fs.get_file_info(['foo'])
    assert info.mtime == dt


@pytest.mark.s3
def test_s3_options():
    from pyarrow.fs import S3FileSystem

    fs = S3FileSystem(access_key='access', secret_key='secret',
                      region='us-east-1', scheme='https',
                      endpoint_override='localhost:8999')
    assert isinstance(fs, S3FileSystem)
    assert pickle.loads(pickle.dumps(fs)) == fs

    with pytest.raises(ValueError):
        S3FileSystem(access_key='access')
    with pytest.raises(ValueError):
        S3FileSystem(secret_key='secret')


@pytest.mark.hdfs
def test_hdfs_options(hdfs_connection):
    from pyarrow.fs import HadoopFileSystem
    if not pa.have_libhdfs():
        pytest.skip('Cannot locate libhdfs')

    host, port, user = hdfs_connection

    replication = 2
    buffer_size = 64*1024
    default_block_size = 128*1024**2
    uri = ('hdfs://{}:{}/?user={}&replication={}&buffer_size={}'
           '&default_block_size={}')

    hdfs1 = HadoopFileSystem(host, port, user='libhdfs',
                             replication=replication, buffer_size=buffer_size,
                             default_block_size=default_block_size)
    hdfs2 = HadoopFileSystem.from_uri(uri.format(
        host, port, 'libhdfs', replication, buffer_size, default_block_size
    ))
    hdfs3 = HadoopFileSystem.from_uri(uri.format(
        host, port, 'me', replication, buffer_size, default_block_size
    ))
    hdfs4 = HadoopFileSystem.from_uri(uri.format(
        host, port, 'me', replication + 1, buffer_size, default_block_size
    ))
    hdfs5 = HadoopFileSystem(host, port)
    hdfs6 = HadoopFileSystem.from_uri('hdfs://{}:{}'.format(host, port))
    hdfs7 = HadoopFileSystem(host, port, user='localuser')
    hdfs8 = HadoopFileSystem(host, port, user='localuser',
                             kerb_ticket="cache_path")
    hdfs9 = HadoopFileSystem(host, port, user='localuser',
                             kerb_ticket=pathlib.Path("cache_path"))
    hdfs10 = HadoopFileSystem(host, port, user='localuser',
                              kerb_ticket="cache_path2")

    assert hdfs1 == hdfs2
    assert hdfs5 == hdfs6
    assert hdfs6 != hdfs7
    assert hdfs2 != hdfs3
    assert hdfs3 != hdfs4
    assert hdfs7 != hdfs5
    assert hdfs2 != hdfs3
    assert hdfs3 != hdfs4
    assert hdfs7 != hdfs8
    assert hdfs8 == hdfs9
    assert hdfs10 != hdfs9

    with pytest.raises(TypeError):
        HadoopFileSystem()
    with pytest.raises(TypeError):
        HadoopFileSystem.from_uri(3)

    for fs in [hdfs1, hdfs2, hdfs3, hdfs4, hdfs5, hdfs6, hdfs7, hdfs8,
               hdfs9, hdfs10]:
        assert pickle.loads(pickle.dumps(fs)) == fs

    host, port, user = hdfs_connection

    hdfs = HadoopFileSystem(host, port, user=user)
    assert hdfs.get_file_info(FileSelector('/'))

    hdfs = HadoopFileSystem.from_uri(
        "hdfs://{}:{}/?user={}".format(host, port, user)
    )
    assert hdfs.get_file_info(FileSelector('/'))


@pytest.mark.parametrize(('uri', 'expected_klass', 'expected_path'), [
    # leading slashes are removed intentionally, because MockFileSystem doesn't
    # have a distinction between relative and absolute paths
    ('mock:', _MockFileSystem, ''),
    ('mock:foo/bar', _MockFileSystem, 'foo/bar'),
    ('mock:/foo/bar', _MockFileSystem, 'foo/bar'),
    ('mock:///foo/bar', _MockFileSystem, 'foo/bar'),
    ('file:/', LocalFileSystem, '/'),
    ('file:///', LocalFileSystem, '/'),
    ('file:/foo/bar', LocalFileSystem, '/foo/bar'),
    ('file:///foo/bar', LocalFileSystem, '/foo/bar'),
    ('/', LocalFileSystem, '/'),
    ('/foo/bar', LocalFileSystem, '/foo/bar'),
])
def test_filesystem_from_uri(uri, expected_klass, expected_path):
    fs, path = FileSystem.from_uri(uri)
    assert isinstance(fs, expected_klass)
    assert path == expected_path


@pytest.mark.skipif(
    sys.version_info < (3, 6),
    reason="python 3.5 Path.resolve() checks that the path exists"
)
@pytest.mark.parametrize(
    'path',
    ['', '/', 'foo/bar', '/foo/bar', __file__]
)
def test_filesystem_from_path_object(path):
    p = pathlib.Path(path)
    fs, path = FileSystem.from_uri(p)
    assert isinstance(fs, LocalFileSystem)
    assert path == p.resolve().absolute().as_posix()


@pytest.mark.s3
def test_filesystem_from_uri_s3(s3_connection, s3_server):
    from pyarrow.fs import S3FileSystem

    host, port, access_key, secret_key = s3_connection

    uri = "s3://{}:{}@mybucket/foo/bar?scheme=http&endpoint_override={}:{}" \
        .format(access_key, secret_key, host, port)

    fs, path = FileSystem.from_uri(uri)
    assert isinstance(fs, S3FileSystem)
    assert path == "mybucket/foo/bar"

    fs.create_dir(path)
    [info] = fs.get_file_info([path])
    assert info.path == path
    assert info.type == FileType.Directory
