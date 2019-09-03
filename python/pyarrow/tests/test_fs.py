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

from pathlib import Path

import pytest

from pyarrow import ArrowIOError
from pyarrow.fs import (
    FileType,
    FileStats,
    LocalFileSystem,
    SubTreeFileSystem,
    Selector
)
from pyarrow.tests.test_io import gzip_compress, gzip_decompress


@pytest.fixture(params=[
    pytest.param(
        lambda tmp: LocalFileSystem(),
        id='LocalFileSystem'
    ),
    pytest.param(
        lambda tmp: SubTreeFileSystem(tmp, LocalFileSystem()),
        id='SubTreeFileSystem(LocalFileSystem)'
    ),
    # s3 follows
    # pytest.param(
    #     lambda tmp: SubTreeFileSystem(tmp, LocalFileSystem()),
    #     name='SubTreeFileSystem[LocalFileSystem]'
    # ),
])
def fs(request, tempdir):
    return request.param(tempdir)


@pytest.fixture(params=[
    pytest.param(Path, id='Path'),
    pytest.param(str, id='str')
])
def testpath(request, fs, tempdir):
    # we always use the tempdir for rewding and writing test artifacts, but
    # if the filesystem is wrapped in a SubTreeFileSystem then we don't need
    # to prepend the path with the tempdir, we also test the API with both
    # pathlib.Path objects and plain python strings
    if isinstance(fs, SubTreeFileSystem):
        return lambda path: request.param(path)
    else:
        return lambda path: request.param(tempdir / path)


def test_stat_with_paths(fs, tempdir, testpath):
    (tempdir / 'a' / 'aa' / 'aaa').mkdir(parents=True)
    (tempdir / 'a' / 'bb').touch()
    (tempdir / 'c.txt').touch()

    aaa_path = testpath('a/aa/aaa')
    bb_path = testpath('a/bb')
    c_path = testpath('c.txt')

    aaa, bb, c = fs.stat([aaa_path, bb_path, c_path])

    assert aaa.path == Path(aaa_path)
    assert aaa.base_name == 'aaa'
    assert aaa.extension == ''
    assert aaa.type == FileType.Directory

    assert bb.path == Path(bb_path)
    assert bb.base_name == 'bb'
    assert bb.extension == ''
    assert bb.type == FileType.File

    assert c.path == Path(c_path)
    assert c.base_name == 'c.txt'
    assert c.extension == 'txt'
    assert c.type == FileType.File

    selector = Selector(testpath(''), allow_non_existent=False, recursive=True)
    nodes = fs.stat(selector)
    assert len(nodes) == 5
    assert len(list(n for n in nodes if n.type == FileType.File)) == 2
    assert len(list(n for n in nodes if n.type == FileType.Directory)) == 3


def test_mkdir(fs, tempdir, testpath):
    directory = testpath('directory')
    directory_ = tempdir / 'directory'
    assert not directory_.exists()
    fs.mkdir(directory)
    assert directory_.exists()

    # recursive
    directory = testpath('deeply/nested/directory')
    directory_ = tempdir / 'deeply' / 'nested' / 'directory'
    assert not directory_.exists()
    with pytest.raises(ArrowIOError):
        fs.mkdir(directory, recursive=False)
    fs.mkdir(directory)
    assert directory_.exists()


def test_rmdir(fs, tempdir, testpath):
    folder = testpath('directory')
    nested = testpath('nested/directory')
    folder_ = tempdir / 'directory'
    nested_ = tempdir / 'nested' / 'directory'

    folder_.mkdir()
    nested_.mkdir(parents=True)

    assert folder_.exists()
    fs.rmdir(folder)
    assert not folder_.exists()

    assert nested_.exists()
    fs.rmdir(nested)
    assert not nested_.exists()


def test_cp(fs, tempdir, testpath):
    # copy file
    source = testpath('source-file')
    source_ = tempdir / 'source-file'
    source_.touch()
    target = testpath('target-file')
    target_ = tempdir / 'target-file'
    assert not target_.exists()
    fs.cp(source, target)
    assert source_.exists()
    assert target_.exists()


def test_mv(fs, tempdir, testpath):
    # move directory
    source = testpath('source-dir')
    source_ = tempdir / 'source-dir'
    source_.mkdir()
    target = testpath('target-dir')
    target_ = tempdir / 'target-dir'
    assert not target_.exists()
    fs.mv(source, target)
    assert not source_.exists()
    assert target_.exists()

    # move file
    source = testpath('source-file')
    source_ = tempdir / 'source-file'
    source_.touch()
    target = testpath('target-file')
    target_ = tempdir / 'target-file'
    assert not target_.exists()
    fs.mv(source, target)
    assert not source_.exists()
    assert target_.exists()


def test_rm(fs, tempdir, testpath):
    target = testpath('target-file')
    target_ = tempdir / 'target-file'
    target_.touch()
    assert target_.exists()
    fs.rm(target)
    assert not target_.exists()

    nested = testpath('nested/target-file')
    nested_ = tempdir / 'nested/target-file'
    nested_.parent.mkdir()
    nested_.touch()
    assert nested_.exists()
    fs.rm(nested)
    assert not nested_.exists()


@pytest.mark.parametrize(
    ('compression', 'buffer_size', 'compressor'),
    [
        (None, None, lambda s: s),
        (None, 64, lambda s: s),
        ('gzip', None, gzip_compress),
        ('gzip', 256, gzip_compress),
    ]
)
def test_input_stream(fs, tempdir, testpath, compression, buffer_size,
                      compressor):
    file = testpath('abc')
    file_ = tempdir / 'abc'
    data = b'some data' * 1024
    file_.write_bytes(compressor(data))

    with fs.input_stream(file, compression, buffer_size) as f:
        result = f.read()

    assert result == data


def test_input_file(fs, tempdir, testpath):
    file = testpath('abc')
    file_ = tempdir / 'abc'
    data = b'some data' * 1024
    file_.write_bytes(data)

    read_from = len(b'some data') * 512
    with fs.input_file(file) as f:
        f.seek(read_from)
        result = f.read()

    assert result == data[read_from:]


@pytest.mark.parametrize(
    ('compression', 'buffer_size', 'decompressor'),
    [
        (None, None, lambda s: s),
        (None, 64, lambda s: s),
        ('gzip', None, gzip_decompress),
        ('gzip', 256, gzip_decompress),
    ]
)
def test_output_stream(fs, tempdir, testpath, compression, buffer_size,
                       decompressor):
    file = testpath('abc')
    file_ = tempdir / 'abc'

    data = b'some data' * 1024
    with fs.output_stream(file, compression, buffer_size) as f:
        f.write(data)

    assert decompressor(file_.read_bytes()) == data


@pytest.mark.parametrize(
    ('compression', 'buffer_size', 'compressor', 'decompressor'),
    [
        (None, None, lambda s: s, lambda s: s),
        (None, 64, lambda s: s, lambda s: s),
        ('gzip', None, gzip_compress, gzip_decompress),
        ('gzip', 256, gzip_compress, gzip_decompress),
    ]
)
def test_append_stream(fs, tempdir, testpath, compression, buffer_size,
                       compressor, decompressor):
    file = testpath('abc')
    file_ = tempdir / 'abc'
    file_.write_bytes(compressor(b'already existing'))

    with fs.append_stream(file, compression, buffer_size) as f:
        f.write(b'\nnewly added')

    assert decompressor(file_.read_bytes()) == b'already existing\nnewly added'
