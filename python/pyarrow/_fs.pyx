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

# cython: language_level = 3

from pathlib import Path

from pyarrow.compat import frombytes, tobytes
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow_fs cimport *
from pyarrow.lib import _detect_compression
from pyarrow.lib cimport (
    check_status,
    NativeFile,
    BufferedOutputStream,
    BufferedInputStream,
    CompressedInputStream,
    CompressedOutputStream
)


cdef inline c_string _to_string(p):
    # supports types: byte, str, pathlib.Path
    return tobytes(str(p))


cpdef enum FileType:
    NonExistent
    Uknown
    File
    Directory


cdef class TimePoint:
    pass


cdef class FileStats:

    cdef CFileStats stats

    def __init__(self):
        raise TypeError('dont initialize me')

    @staticmethod
    cdef FileStats wrap(CFileStats stats):
        cdef FileStats self = FileStats.__new__(FileStats)
        self.stats = stats
        return self

    @property
    def type(self):
        cdef CFileType ctype = self.stats.type()
        if ctype == CFileType_NonExistent:
            return FileType.NonExistent
        elif ctype == CFileType_Unknown:
            return FileType.Uknown
        elif ctype == CFileType_File:
            return FileType.File
        elif ctype == CFileType_Directory:
            return FileType.Directory
        else:
            raise ValueError('Unhandled FileType {}'.format(type))

    @property
    def path(self):
        return Path(frombytes(self.stats.path()))

    @property
    def base_name(self):
        return frombytes(self.stats.base_name())

    @property
    def size(self):
        return self.stats.size()

    @property
    def extension(self):
        return frombytes(self.stats.extension())

    # @property
    # def mtime(self):
    #     return self.stats.mtime()


cdef class Selector:

    cdef CSelector selector

    def __init__(self, base_dir='', bint allow_non_existent=False,
                 bint recursive=False):
        self.base_dir = base_dir
        self.recursive = recursive
        self.allow_non_existent = allow_non_existent

    @property
    def base_dir(self):
        return Path(self.selector.base_dir)

    @base_dir.setter
    def base_dir(self, base_dir):
        self.selector.base_dir = _to_string(base_dir)

    @property
    def allow_non_existent(self):
        return self.selector.allow_non_existent

    @allow_non_existent.setter
    def allow_non_existent(self, bint allow_non_existent):
        self.selector.allow_non_existent = allow_non_existent

    @property
    def recursive(self):
        return self.selector.recursive

    @recursive.setter
    def recursive(self, bint recursive):
        self.selector.recursive = recursive


cdef class FileSystem:

    cdef:
        shared_ptr[CFileSystem] wrapped
        CFileSystem* fs

    def __init__(self):
        raise TypeError('dont initialize me')

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        self.wrapped = wrapped
        self.fs = wrapped.get()

    def stat(self, paths_or_selector):
        cdef:
            vector[CFileStats] stats
            vector[c_string] paths
            CSelector selector

        if isinstance(paths_or_selector, Selector):
            selector = (<Selector>paths_or_selector).selector
            check_status(self.fs.GetTargetStats(selector, &stats))
        elif isinstance(paths_or_selector, (list, tuple)):
            paths = [_to_string(s) for s in paths_or_selector]
            check_status(self.fs.GetTargetStats(paths, &stats))
        else:
            raise TypeError('Must pass either paths or a Selector')

        return [FileStats.wrap(stat) for stat in stats]

    def mkdir(self, path, bint recursive=True):
        check_status(self.fs.CreateDir(_to_string(path), recursive=recursive))

    def rmdir(self, path):
        check_status(self.fs.DeleteDir(_to_string(path)))

    def mv(self, src, dest):
        check_status(self.fs.Move(_to_string(src), _to_string(dest)))

    def cp(self, src, dest):
        check_status(self.fs.CopyFile(_to_string(src), _to_string(dest)))

    def rm(self, path):
        check_status(self.fs.DeleteFile(_to_string(path)))

    def _wrap_input_stream(self, stream, path, compression, buffer_size):
        if buffer_size is not None and buffer_size != 0:
            stream = BufferedInputStream(stream, buffer_size)
        if compression == 'detect':
            compression = _detect_compression(path)
        if compression is not None:
            stream = CompressedInputStream(stream, compression)
        return stream

    def _wrap_output_stream(self, stream, path, compression, buffer_size):
        if buffer_size is not None and buffer_size != 0:
            stream = BufferedOutputStream(stream, buffer_size)
        if compression == 'detect':
            compression = _detect_compression(path)
        if compression is not None:
            stream = CompressedOutputStream(stream, compression)
        return stream

    def input_file(self, path):
        cdef:
            c_string pathstr = _to_string(path)
            NativeFile stream = NativeFile()
            shared_ptr[CRandomAccessFile] in_handle

        with nogil:
            check_status(self.fs.OpenInputFile(pathstr, &in_handle))

        stream.set_random_access_file(in_handle)
        stream.is_readable = True
        return stream

    def input_stream(self, path, compression='detect', buffer_size=None):
        cdef:
            c_string pathstr = _to_string(path)
            NativeFile stream = NativeFile()
            shared_ptr[CInputStream] in_handle

        with nogil:
            check_status(self.fs.OpenInputStream(pathstr, &in_handle))

        stream.set_input_stream(in_handle)
        stream.is_readable = True

        return self._wrap_input_stream(
            stream, path=path, compression=compression, buffer_size=buffer_size
        )

    def output_stream(self, path, compression='detect', buffer_size=None):
        cdef:
            c_string pathstr = _to_string(path)
            NativeFile stream = NativeFile()
            shared_ptr[COutputStream] out_handle

        with nogil:
            check_status(self.fs.OpenOutputStream(pathstr, &out_handle))

        stream.set_output_stream(out_handle)
        stream.is_writable = True

        return self._wrap_output_stream(
            stream, path=path, compression=compression, buffer_size=buffer_size
        )

    def append_stream(self, path, compression='detect', buffer_size=None):
        cdef:
            c_string pathstr = _to_string(path)
            NativeFile stream = NativeFile()
            shared_ptr[COutputStream] out_handle

        with nogil:
            check_status(self.fs.OpenAppendStream(pathstr, &out_handle))

        stream.set_output_stream(out_handle)
        stream.is_writable = True

        return self._wrap_output_stream(
            stream, path=path, compression=compression, buffer_size=buffer_size
        )

    # CStatus OpenInputFile(const c_string& path,
    #                       shared_ptr[RandomAccessFile]* out)


cdef class LocalFileSystem(FileSystem):

    cdef:
        CLocalFileSystem* localfs

    def __init__(self):
        cdef shared_ptr[CLocalFileSystem] wrapped
        wrapped = make_shared[CLocalFileSystem]()
        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.localfs = <CLocalFileSystem*> wrapped.get()


cdef class SubTreeFileSystem(FileSystem):

    cdef:
        CSubTreeFileSystem* subtreefs

    def __init__(self, base_path, FileSystem base_fs):
        cdef:
            c_string pathstr
            shared_ptr[CSubTreeFileSystem] wrapped

        pathstr = tobytes(str(base_path))
        wrapped = make_shared[CSubTreeFileSystem](pathstr, base_fs.wrapped)

        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.subtreefs = <CSubTreeFileSystem*> wrapped.get()
