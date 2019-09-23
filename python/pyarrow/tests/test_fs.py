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

from pyarrow import ArrowIOError
from pyarrow.fs import (FileType, Selector, FileSystem, LocalFileSystem,
                        SubTreeFileSystem)
from pyarrow.tests.test_io import gzip_compress, gzip_decompress


@pytest.fixture(params=[
    pytest.param(
        lambda tmp: LocalFileSystem(),
        id='LocalFileSystem'
    ),
    pytest.param(
        lambda tmp: SubTreeFileSystem(tmp, LocalFileSystem()),
        id='SubTreeFileSystem(LocalFileSystem)'
    )
])
def fs(request, tempdir):
    return request.param(tempdir.as_posix())


@pytest.fixture
def testpath(request, fs, tempdir):
    # we always use the tempdir for reading and writing test artifacts, but
    # if the filesystem is wrapped in a SubTreeFileSystem then we don't need
    # to prepend the path with the tempdir, we also test the API with both
    # pathlib.Path objects and plain python strings
    def convert(path):
        if isinstance(fs, SubTreeFileSystem):
            path = pathlib.Path(path)
        else:
            path = tempdir / path
        return path.as_posix()
    return convert


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


def test_create_dir(fs, tempdir, testpath):
    directory = testpath('directory')
    directory_ = tempdir / 'directory'
    assert not directory_.exists()
    fs.create_dir(directory)
    assert directory_.exists()

    # recursive
    directory = testpath('deeply/nested/directory')
    directory_ = tempdir / 'deeply' / 'nested' / 'directory'
    assert not directory_.exists()
    with pytest.raises(ArrowIOError):
        fs.create_dir(directory, recursive=False)
    fs.create_dir(directory)
    assert directory_.exists()


def test_delete_dir(fs, tempdir, testpath):
    folder = testpath('directory')
    nested = testpath('nested/directory')
    folder_ = tempdir / 'directory'
    nested_ = tempdir / 'nested' / 'directory'

    folder_.mkdir()
    nested_.mkdir(parents=True)

    assert folder_.exists()
    fs.delete_dir(folder)
    assert not folder_.exists()

    assert nested_.exists()
    fs.delete_dir(nested)
    assert not nested_.exists()


def test_copy_file(fs, tempdir, testpath):
    # copy file
    source = testpath('source-file')
    source_ = tempdir / 'source-file'
    source_.touch()
    target = testpath('target-file')
    target_ = tempdir / 'target-file'
    assert not target_.exists()
    fs.copy_file(source, target)
    assert source_.exists()
    assert target_.exists()


def test_move(fs, tempdir, testpath):
    # move directory
    source = testpath('source-dir')
    source_ = tempdir / 'source-dir'
    source_.mkdir()
    target = testpath('target-dir')
    target_ = tempdir / 'target-dir'
    assert not target_.exists()
    fs.move(source, target)
    assert not source_.exists()
    assert target_.exists()

    # move file
    source = testpath('source-file')
    source_ = tempdir / 'source-file'
    source_.touch()
    target = testpath('target-file')
    target_ = tempdir / 'target-file'
    assert not target_.exists()
    fs.move(source, target)
    assert not source_.exists()
    assert target_.exists()


def test_delete_file(fs, tempdir, testpath):
    target = testpath('target-file')
    target_ = tempdir / 'target-file'
    target_.touch()
    assert target_.exists()
    fs.delete_file(target)
    assert not target_.exists()

    nested = testpath('nested/target-file')
    nested_ = tempdir / 'nested/target-file'
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
def test_open_input_stream(fs, tempdir, testpath, compression, buffer_size,
                           compressor):
    file = testpath('abc')
    file_ = tempdir / 'abc'
    data = b'some data' * 1024
    file_.write_bytes(compressor(data))

    with fs.open_input_stream(file, compression, buffer_size) as f:
        result = f.read()

    assert result == data


def test_open_input_file(fs, tempdir, testpath):
    file = testpath('abc')
    file_ = tempdir / 'abc'
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
def test_open_output_stream(fs, tempdir, testpath, compression, buffer_size,
                            decompressor):
    file = testpath('abc')
    file_ = tempdir / 'abc'

    data = b'some data' * 1024
    with fs.open_output_stream(file, compression, buffer_size) as f:
        f.write(data)

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
def test_open_append_stream(fs, tempdir, testpath, compression, buffer_size,
                            compressor, decompressor):
    file = testpath('abc')
    file_ = tempdir / 'abc'
    file_.write_bytes(compressor(b'already existing'))

    with fs.open_append_stream(file, compression, buffer_size) as f:
        f.write(b'\nnewly added')

    assert decompressor(file_.read_bytes()) == b'already existing\nnewly added'
