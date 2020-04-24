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

from cpython.datetime cimport datetime, PyDateTime_DateTime

from pyarrow.compat import frombytes, tobytes
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport PyDateTime_to_TimePoint
from pyarrow.lib import _detect_compression
from pyarrow.lib cimport *
from pyarrow.util import _stringify_path

from datetime import timezone
import pathlib


cdef inline c_string _path_as_bytes(path) except *:
    # handle only abstract paths, not bound to any filesystem like pathlib is,
    # so we only accept plain strings
    if not isinstance(path, (bytes, str)):
        raise TypeError('Path must be a string')
    # tobytes always uses utf-8, which is more or less ok, at least on Windows
    # since the C++ side then decodes from utf-8. On Unix, os.fsencode may be
    # better.
    return tobytes(path)


def _normalize_path(FileSystem filesystem, path):
    """
    Normalize path for the given filesystem.

    The default implementation of this method is a no-op, but subclasses
    may allow normalizing irregular path forms (such as Windows local paths).
    """
    cdef c_string c_path = _path_as_bytes(path)
    cdef c_string c_path_normalized

    c_path_normalized = GetResultValue(filesystem.fs.NormalizePath(c_path))
    return frombytes(c_path_normalized)


cdef class FileInfo:
    """
    FileSystem entry info.
    """

    def __init__(self):
        raise TypeError("FileInfo cannot be instantiated directly, use "
                        "FileSystem.get_file_info method instead.")

    @staticmethod
    cdef wrap(CFileInfo info):
        cdef FileInfo self = FileInfo.__new__(FileInfo)
        self.info = info
        return self

    cdef inline CFileInfo unwrap(self) nogil:
        return self.info

    def __repr__(self):
        def getvalue(attr):
            try:
                return getattr(self, attr)
            except ValueError:
                return ''

        s = '<FileInfo for {!r}: type={}'.format(self.path, str(self.type))
        if self.type == FileType.File:
            s += ', size={}'.format(self.size)
        s += '>'
        return s

    @property
    def type(self):
        """
        Type of the file.

        The returned enum values can be the following:

        - FileType.NotFound: target does not exist
        - FileType.Unknown: target exists but its type is unknown (could be a
          special file such as a Unix socket or character device, or
          Windows NUL / CON / ...)
        - FileType.File: target is a regular file
        - FileType.Directory: target is a regular directory

        Returns
        -------
        type : FileType
        """
        return FileType(<int8_t> self.info.type())

    @property
    def path(self):
        """
        The full file path in the filesystem.
        """
        return frombytes(self.info.path())

    @property
    def base_name(self):
        """
        The file base name.

        Component after the last directory separator.
        """
        return frombytes(self.info.base_name())

    @property
    def size(self):
        """
        The size in bytes, if available.

        Only regular files are guaranteed to have a size.
        """
        if self.info.type() != CFileType_File:
            return None
        return self.info.size()

    @property
    def extension(self):
        """
        The file extension.
        """
        return frombytes(self.info.extension())

    @property
    def mtime(self):
        """
        The time of last modification, if available.

        Returns
        -------
        mtime : datetime.datetime
        """
        cdef int64_t nanoseconds
        nanoseconds = TimePoint_to_ns(self.info.mtime())
        return datetime.fromtimestamp(nanoseconds / 1.0e9, timezone.utc)

    @property
    def mtime_ns(self):
        """
        The time of last modification, if available, expressed in nanoseconds
        since the Unix epoch.

        Returns
        -------
        mtime_ns : int
        """
        return TimePoint_to_ns(self.info.mtime())


cdef class FileSelector:
    """
    File and directory selector.

    It contains a set of options that describes how to search for files and
    directories.

    Parameters
    ----------
    base_dir : str
        The directory in which to select files. Relative paths also work, use
        '.' for the current directory and '..' for the parent.
    allow_not_found : bool, default False
        The behavior if `base_dir` doesn't exist in the filesystem.
        If false, an error is returned.
        If true, an empty selection is returned.
    recursive : bool, default False
        Whether to recurse into subdirectories.
    """

    def __init__(self, base_dir, bint allow_not_found=False,
                 bint recursive=False):
        self.base_dir = base_dir
        self.recursive = recursive
        self.allow_not_found = allow_not_found

    cdef inline CFileSelector unwrap(self) nogil:
        return self.selector

    @property
    def base_dir(self):
        return frombytes(self.selector.base_dir)

    @base_dir.setter
    def base_dir(self, base_dir):
        self.selector.base_dir = _path_as_bytes(base_dir)

    @property
    def allow_not_found(self):
        return self.selector.allow_not_found

    @allow_not_found.setter
    def allow_not_found(self, bint allow_not_found):
        self.selector.allow_not_found = allow_not_found

    @property
    def recursive(self):
        return self.selector.recursive

    @recursive.setter
    def recursive(self, bint recursive):
        self.selector.recursive = recursive

    def __repr__(self):
        return ("<FileSelector base_dir={0.base_dir!r} "
                "recursive={0.recursive}>".format(self))


cdef class FileSystem:
    """
    Abstract file system API.
    """

    def __init__(self):
        raise TypeError("FileSystem is an abstract class, instantiate one of "
                        "the subclasses instead: LocalFileSystem or "
                        "SubTreeFileSystem")

    @staticmethod
    def from_uri(uri):
        """
        Create a new FileSystem from URI or Path.

        Recognized URI schemes are "file", "mock", "s3fs", "hdfs" and "viewfs".
        In addition, the argument can be a pathlib.Path object, or a string
        describing an absolute local path.

        Parameters
        ----------
        uri : string
            URI-based path, for example: file:///some/local/path.

        Returns
        -------
        With (filesystem, path) tuple where path is the abstract path inside
        the FileSystem instance.
        """
        cdef:
            c_string path
            CResult[shared_ptr[CFileSystem]] result

        if isinstance(uri, pathlib.Path):
            # Make absolute
            uri = uri.resolve().absolute()
        uri = _stringify_path(uri)
        result = CFileSystemFromUriOrPath(tobytes(uri), &path)
        return FileSystem.wrap(GetResultValue(result)), frombytes(path)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        self.wrapped = wrapped
        self.fs = wrapped.get()

    @staticmethod
    cdef wrap(const shared_ptr[CFileSystem]& sp):
        cdef FileSystem self

        typ = frombytes(sp.get().type_name())
        if typ == 'local':
            self = LocalFileSystem.__new__(LocalFileSystem)
        elif typ == 'mock':
            self = _MockFileSystem.__new__(_MockFileSystem)
        elif typ == 'subtree':
            self = SubTreeFileSystem.__new__(SubTreeFileSystem)
        elif typ == 's3':
            from pyarrow._s3fs import S3FileSystem
            self = S3FileSystem.__new__(S3FileSystem)
        elif typ == 'hdfs':
            from pyarrow._hdfs import HadoopFileSystem
            self = HadoopFileSystem.__new__(HadoopFileSystem)
        else:
            raise TypeError('Cannot wrap FileSystem pointer')

        self.init(sp)
        return self

    cdef inline shared_ptr[CFileSystem] unwrap(self) nogil:
        return self.wrapped

    def equals(self, FileSystem other):
        return self.fs.Equals(other.unwrap())

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def get_file_info(self, paths_or_selector):
        """
        Get info for the given files.

        Any symlink is automatically dereferenced, recursively. A non-existing
        or unreachable file returns a FileStat object and has a FileType of
        value NotFound. An exception indicates a truly exceptional condition
        (low-level I/O error, etc.).

        Parameters
        ----------
        paths_or_selector: FileSelector or list of path-likes
            Either a selector object or a list of path-like objects.
            The selector's base directory will not be part of the results, even
            if it exists. If it doesn't exist, use `allow_not_found`.

        Returns
        -------
        file_infos : list of FileInfo
        """
        cdef:
            vector[CFileInfo] infos
            vector[c_string] paths
            CFileSelector selector

        if isinstance(paths_or_selector, FileSelector):
            with nogil:
                selector = (<FileSelector>paths_or_selector).selector
                infos = GetResultValue(self.fs.GetFileInfo(selector))
        elif isinstance(paths_or_selector, (list, tuple)):
            paths = [_path_as_bytes(s) for s in paths_or_selector]
            with nogil:
                infos = GetResultValue(self.fs.GetFileInfo(paths))
        else:
            raise TypeError('Must pass either paths or a FileSelector')

        return [FileInfo.wrap(info) for info in infos]

    def create_dir(self, path, *, bint recursive=True):
        """
        Create a directory and subdirectories.

        This function succeeds if the directory already exists.

        Parameters
        ----------
        path : str
            The path of the new directory.
        recursive: bool, default True
            Create nested directories as well.
        """
        cdef c_string directory = _path_as_bytes(path)
        with nogil:
            check_status(self.fs.CreateDir(directory, recursive=recursive))

    def delete_dir(self, path):
        """Delete a directory and its contents, recursively.

        Parameters
        ----------
        path : str
            The path of the directory to be deleted.
        """
        cdef c_string directory = _path_as_bytes(path)
        with nogil:
            check_status(self.fs.DeleteDir(directory))

    def move(self, src, dest):
        """
        Move / rename a file or directory.

        If the destination exists:
        - if it is a non-empty directory, an error is returned
        - otherwise, if it has the same type as the source, it is replaced
        - otherwise, behavior is unspecified (implementation-dependent).

        Parameters
        ----------
        src : str
            The path of the file or the directory to be moved.
        dest : str
            The destination path where the file or directory is moved to.
        """
        cdef:
            c_string source = _path_as_bytes(src)
            c_string destination = _path_as_bytes(dest)
        with nogil:
            check_status(self.fs.Move(source, destination))

    def copy_file(self, src, dest):
        """
        Copy a file.

        If the destination exists and is a directory, an error is returned.
        Otherwise, it is replaced.

        Parameters
        ----------
        src : str
            The path of the file to be copied from.
        dest : str
            The destination path where the file is copied to.
        """
        cdef:
            c_string source = _path_as_bytes(src)
            c_string destination = _path_as_bytes(dest)
        with nogil:
            check_status(self.fs.CopyFile(source, destination))

    def delete_file(self, path):
        """
        Delete a file.

        Parameters
        ----------
        path : str
            The path of the file to be deleted.
        """
        cdef c_string file = _path_as_bytes(path)
        with nogil:
            check_status(self.fs.DeleteFile(file))

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

    def open_input_file(self, path):
        """
        Open an input file for random access reading.

        Parameters
        ----------
        path : str
            The source to open for reading.

        Returns
        -------
        stram : NativeFile
        """
        cdef:
            c_string pathstr = _path_as_bytes(path)
            NativeFile stream = NativeFile()
            shared_ptr[CRandomAccessFile] in_handle

        with nogil:
            in_handle = GetResultValue(self.fs.OpenInputFile(pathstr))

        stream.set_random_access_file(in_handle)
        stream.is_readable = True
        return stream

    def open_input_stream(self, path, compression='detect', buffer_size=None):
        """
        Open an input stream for sequential reading.

        Parameters
        ----------
        source: str
            The source to open for reading.
        compression: str optional, default 'detect'
            The compression algorithm to use for on-the-fly decompression.
            If "detect" and source is a file path, then compression will be
            chosen based on the file extension.
            If None, no compression will be applied. Otherwise, a well-known
            algorithm name must be supplied (e.g. "gzip").
        buffer_size: int optional, default None
            If None or 0, no buffering will happen. Otherwise the size of the
            temporary read buffer.

        Returns
        -------
        stream : NativeFile
        """
        cdef:
            c_string pathstr = _path_as_bytes(path)
            NativeFile stream = NativeFile()
            shared_ptr[CInputStream] in_handle

        with nogil:
            in_handle = GetResultValue(self.fs.OpenInputStream(pathstr))

        stream.set_input_stream(in_handle)
        stream.is_readable = True

        return self._wrap_input_stream(
            stream, path=path, compression=compression, buffer_size=buffer_size
        )

    def open_output_stream(self, path, compression='detect', buffer_size=None):
        """
        Open an output stream for sequential writing.

        If the target already exists, existing data is truncated.

        Parameters
        ----------
        path : str
            The source to open for writing.
        compression: str optional, default 'detect'
            The compression algorithm to use for on-the-fly compression.
            If "detect" and source is a file path, then compression will be
            chosen based on the file extension.
            If None, no compression will be applied. Otherwise, a well-known
            algorithm name must be supplied (e.g. "gzip").
        buffer_size: int optional, default None
            If None or 0, no buffering will happen. Otherwise the size of the
            temporary write buffer.

        Returns
        -------
        stream : NativeFile
        """
        cdef:
            c_string pathstr = _path_as_bytes(path)
            NativeFile stream = NativeFile()
            shared_ptr[COutputStream] out_handle

        with nogil:
            out_handle = GetResultValue(self.fs.OpenOutputStream(pathstr))

        stream.set_output_stream(out_handle)
        stream.is_writable = True

        return self._wrap_output_stream(
            stream, path=path, compression=compression, buffer_size=buffer_size
        )

    def open_append_stream(self, path, compression='detect', buffer_size=None):
        """
        Open an output stream for appending.

        If the target doesn't exist, a new empty file is created.

        Parameters
        ----------
        path : str
            The source to open for writing.
        compression: str optional, default 'detect'
            The compression algorithm to use for on-the-fly compression.
            If "detect" and source is a file path, then compression will be
            chosen based on the file extension.
            If None, no compression will be applied. Otherwise, a well-known
            algorithm name must be supplied (e.g. "gzip").
        buffer_size: int optional, default None
            If None or 0, no buffering will happen. Otherwise the size of the
            temporary write buffer.

        Returns
        -------
        stream : NativeFile
        """
        cdef:
            c_string pathstr = _path_as_bytes(path)
            NativeFile stream = NativeFile()
            shared_ptr[COutputStream] out_handle

        with nogil:
            out_handle = GetResultValue(self.fs.OpenAppendStream(pathstr))

        stream.set_output_stream(out_handle)
        stream.is_writable = True

        return self._wrap_output_stream(
            stream, path=path, compression=compression, buffer_size=buffer_size
        )


cdef class LocalFileSystem(FileSystem):
    """
    A FileSystem implementation accessing files on the local machine.

    Details such as symlinks are abstracted away (symlinks are always followed,
    except when deleting an entry).

    Parameters
    ----------
    use_mmap: bool, default False
        Whether open_input_stream and open_input_file should return
        a mmap'ed file or a regular file.
    """

    def __init__(self, use_mmap=False):
        cdef:
            CLocalFileSystemOptions opts
            shared_ptr[CLocalFileSystem] fs

        opts = CLocalFileSystemOptions.Defaults()
        opts.use_mmap = use_mmap

        fs = make_shared[CLocalFileSystem](opts)
        self.init(<shared_ptr[CFileSystem]> fs)

    cdef init(self, const shared_ptr[CFileSystem]& c_fs):
        FileSystem.init(self, c_fs)
        self.localfs = <CLocalFileSystem*> c_fs.get()

    def __reduce__(self):
        cdef CLocalFileSystemOptions opts = self.localfs.options()
        return LocalFileSystem, (opts.use_mmap,)


cdef class SubTreeFileSystem(FileSystem):
    """
    Delegates to another implementation after prepending a fixed base path.

    This is useful to expose a logical view of a subtree of a filesystem,
    for example a directory in a LocalFileSystem.

    Note, that this makes no security guarantee. For example, symlinks may
    allow to "escape" the subtree and access other parts of the underlying
    filesystem.

    Parameters
    ----------
    base_path: str
        The root of the subtree.
    base_fs: FileSystem
        FileSystem object the operations delegated to.
    """

    def __init__(self, base_path, FileSystem base_fs):
        cdef:
            c_string pathstr
            shared_ptr[CSubTreeFileSystem] wrapped

        pathstr = _path_as_bytes(base_path)
        wrapped = make_shared[CSubTreeFileSystem](pathstr, base_fs.wrapped)

        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.subtreefs = <CSubTreeFileSystem*> wrapped.get()

    def __reduce__(self):
        return SubTreeFileSystem, (
            frombytes(self.subtreefs.base_path()),
            FileSystem.wrap(self.subtreefs.base_fs())
        )

    @property
    def base_path(self):
        return frombytes(self.subtreefs.base_path())

    @property
    def base_fs(self):
        return FileSystem.wrap(self.subtreefs.base_fs())


cdef class _MockFileSystem(FileSystem):

    def __init__(self, datetime current_time=None):
        cdef shared_ptr[CMockFileSystem] wrapped

        current_time = current_time or datetime.now()
        wrapped = make_shared[CMockFileSystem](
            PyDateTime_to_TimePoint(<PyDateTime_DateTime*> current_time)
        )

        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.mockfs = <CMockFileSystem*> wrapped.get()
