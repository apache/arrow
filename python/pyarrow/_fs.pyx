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

import six

from pyarrow.compat import frombytes, tobytes
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport PyDateTime_from_TimePoint
from pyarrow.includes.libarrow_fs cimport *
from pyarrow.util import _stringify_path
from pyarrow.lib import _detect_compression
from pyarrow.lib cimport (
    check_status,
    NativeFile,
    BufferedOutputStream,
    BufferedInputStream,
    CompressedInputStream,
    CompressedOutputStream
)


cdef inline c_string _path_as_bytes(path) except *:
    # handle only abstract paths, not bound to any filesystem like pathlib is,
    # so we only accept plain strings
    if not isinstance(path, six.string_types):
        raise TypeError('Path must be a string')
    # tobytes always uses utf-8, which is more or less ok, at least on Windows
    # since the C++ side then decodes from utf-8. On Unix, os.fsencode may be
    # better.
    return tobytes(path)


cpdef enum FileType:
    NonExistent = <int8_t> CFileType_NonExistent
    Unknown = <int8_t> CFileType_Unknown
    File = <int8_t> CFileType_File
    Directory = <int8_t> CFileType_Directory


cdef class FileStats:
    """FileSystem entry stats"""

    cdef CFileStats stats

    def __init__(self):
        raise TypeError('dont initialize me')

    @staticmethod
    cdef FileStats wrap(CFileStats stats):
        cdef FileStats self = FileStats.__new__(FileStats)
        self.stats = stats
        return self

    def __repr__(self):
        def getvalue(attr):
            try:
                return getattr(self, attr)
            except ValueError:
                return ''
        attrs = ['type', 'path', 'base_name', 'size', 'extension', 'mtime']
        attrs = '\n'.join('{}: {}'.format(a, getvalue(a)) for a in attrs)
        return '{}\n{}'.format(object.__repr__(self), attrs)

    @property
    def type(self):
        """Type of the file

        The returned enum variants have the following meanings:
        - FileType.NonExistent: target does not exist
        - FileType.Unknown: target exists but its type is unknown (could be a
                            special file such as a Unix socket or character
                            device, or Windows NUL / CON / ...)
        - FileType.File: target is a regular file
        - FileType.Directory: target is a regular directory

        Returns
        -------
        type : FileType
        """
        return FileType(<int8_t> self.stats.type())

    @property
    def path(self):
        """The full file path in the filesystem."""
        return frombytes(self.stats.path())

    @property
    def base_name(self):
        """The file base name

        Component after the last directory separator.
        """
        return frombytes(self.stats.base_name())

    @property
    def size(self):
        """The size in bytes, if available

        Only regular files are guaranteed to have a size.
        """
        if self.stats.type() != CFileType_File:
            raise ValueError(
                'Only regular files are guaranteed to have a size'
            )
        return self.stats.size()

    @property
    def extension(self):
        """The file extension"""
        return frombytes(self.stats.extension())

    @property
    def mtime(self):
        """The time of last modification, if available.

        Returns
        -------
        mtime : datetime.datetime
        """
        cdef PyObject *out
        check_status(PyDateTime_from_TimePoint(self.stats.mtime(), &out))
        return PyObject_to_object(out)


cdef class Selector:
    """File and directory selector.

    It contains a set of options that describes how to search for files and
    directories.

    Parameters
    ----------
    base_dir : str or pathlib.Path
        The directory in which to select files. Relative paths also work, use
        '.' for the current directory and '..' for the parent.
    allow_non_existent : bool, default False
        The behavior if `base_dir` doesn't exist in the filesystem.
        If false, an error is returned.
        If true, an empty selection is returned.
    recursive : bool, default False
        Whether to recurse into subdirectories.
    """
    cdef CSelector selector

    def __init__(self, base_dir, bint allow_non_existent=False,
                 bint recursive=False):
        self.base_dir = base_dir
        self.recursive = recursive
        self.allow_non_existent = allow_non_existent

    @property
    def base_dir(self):
        return frombytes(self.selector.base_dir)

    @base_dir.setter
    def base_dir(self, base_dir):
        self.selector.base_dir = _path_as_bytes(base_dir)

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
    """Abstract file system API"""

    cdef:
        shared_ptr[CFileSystem] wrapped
        CFileSystem* fs

    def __init__(self):
        raise TypeError("FileSystem is an abstract class, instantiate one of "
                        "the subclasses instead: LocalFileSystem or "
                        "SubTreeFileSystem")

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        self.wrapped = wrapped
        self.fs = wrapped.get()

    def get_target_stats(self, paths_or_selector):
        """Get statistics for the given target.

        Any symlink is automatically dereferenced, recursively. A non-existing
        or unreachable file returns a FileStats object and has a FileType of
        value NonExistent. An exception indicates a truly exceptional condition
        (low-level I/O error, etc.).

        Parameters
        ----------
        paths_or_selector: Selector or list of path-likes
            Either a Selector object or a list of path-like objects.
            The selector's base directory will not be part of the results, even
            if it exists. If it doesn't exist, use `allow_non_existent`.

        Returns
        -------
        file_stats : list of FileStats
        """
        cdef:
            vector[CFileStats] stats
            vector[c_string] paths
            CSelector selector

        if isinstance(paths_or_selector, Selector):
            with nogil:
                selector = (<Selector>paths_or_selector).selector
                check_status(self.fs.GetTargetStats(selector, &stats))
        elif isinstance(paths_or_selector, (list, tuple)):
            paths = [_path_as_bytes(s) for s in paths_or_selector]
            with nogil:
                check_status(self.fs.GetTargetStats(paths, &stats))
        else:
            raise TypeError('Must pass either paths or a Selector')

        return [FileStats.wrap(stat) for stat in stats]

    def create_dir(self, path, bint recursive=True):
        """Create a directory and subdirectories.

        This function succeeds if the directory already exists.

        Parameters
        ----------
        path : str or pathlib.Path
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
        path : str or pathlib.Path
            The path of the directory to be deleted.
        """
        cdef c_string directory = _path_as_bytes(path)
        with nogil:
            check_status(self.fs.DeleteDir(directory))

    def move(self, src, dest):
        """Move / rename a file or directory.

        If the destination exists:
        - if it is a non-empty directory, an error is returned
        - otherwise, if it has the same type as the source, it is replaced
        - otherwise, behavior is unspecified (implementation-dependent).

        Parameters
        ----------
        src : str or pathlib.Path
            The path of the file or the directory to be moved.
        dest : str or pathlib.Path
            The destination path where the file or directory is moved to.
        """
        cdef:
            c_string source = _path_as_bytes(src)
            c_string destination = _path_as_bytes(dest)
        with nogil:
            check_status(self.fs.Move(source, destination))

    def copy_file(self, src, dest):
        """Copy a file.

        If the destination exists and is a directory, an error is returned.
        Otherwise, it is replaced.

        Parameters
        ----------
        src : str or pathlib.Path
            The path of the file to be copied from.
        dest : str or pathlib.Path
            The destination path where the file is copied to.
        """
        cdef:
            c_string source = _path_as_bytes(src)
            c_string destination = _path_as_bytes(dest)
        with nogil:
            check_status(self.fs.CopyFile(source, destination))

    def delete_file(self, path):
        """Delete a file.

        Parameters
        ----------
        path : str or pathlib.Path
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
        """Open an input file for random access reading.

        Parameters
        ----------
        path : Union[str, pathlib.Path]
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
            check_status(self.fs.OpenInputFile(pathstr, &in_handle))

        stream.set_random_access_file(in_handle)
        stream.is_readable = True
        return stream

    def open_input_stream(self, path, compression='detect', buffer_size=None):
        """Open an input stream for sequential reading.

        Parameters
        ----------
        source: str or pathlib.Path
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
            check_status(self.fs.OpenInputStream(pathstr, &in_handle))

        stream.set_input_stream(in_handle)
        stream.is_readable = True

        return self._wrap_input_stream(
            stream, path=path, compression=compression, buffer_size=buffer_size
        )

    def open_output_stream(self, path, compression='detect', buffer_size=None):
        """Open an output stream for sequential writing.

        If the target already exists, existing data is truncated.

        Parameters
        ----------
        path : str or pathlib.Path
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
            check_status(self.fs.OpenOutputStream(pathstr, &out_handle))

        stream.set_output_stream(out_handle)
        stream.is_writable = True

        return self._wrap_output_stream(
            stream, path=path, compression=compression, buffer_size=buffer_size
        )

    def open_append_stream(self, path, compression='detect', buffer_size=None):
        """Open an output stream for appending.

        If the target doesn't exist, a new empty file is created.

        Parameters
        ----------
        path : str or pathlib.Path
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
            check_status(self.fs.OpenAppendStream(pathstr, &out_handle))

        stream.set_output_stream(out_handle)
        stream.is_writable = True

        return self._wrap_output_stream(
            stream, path=path, compression=compression, buffer_size=buffer_size
        )


cdef class LocalFileSystem(FileSystem):
    """A FileSystem implementation accessing files on the local machine.

    Details such as symlinks are abstracted away (symlinks are always followed,
    except when deleting an entry).
    """

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
    """Delegates to another implementation after prepending a fixed base path.

    This is useful to expose a logical view of a subtree of a filesystem,
    for example a directory in a LocalFileSystem.

    Note, that this makes no security guarantee. For example, symlinks may
    allow to "escape" the subtree and access other parts of the underlying
    filesystem.
    """

    cdef:
        CSubTreeFileSystem* subtreefs

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
