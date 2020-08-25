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

"""
FileSystem abstraction to interact with various local and remote filesystems.
"""

from pyarrow._fs import (  # noqa
    FileSelector,
    FileType,
    FileInfo,
    FileSystem,
    LocalFileSystem,
    SubTreeFileSystem,
    _MockFileSystem,
    FileSystemHandler,
    PyFileSystem,
)

# For backward compatibility.
FileStats = FileInfo

_not_imported = []

try:
    from pyarrow._hdfs import HadoopFileSystem  # noqa
except ImportError:
    _not_imported.append("HadoopFileSystem")

try:
    from pyarrow._s3fs import (  # noqa
        S3FileSystem, S3LogLevel, initialize_s3, finalize_s3)
except ImportError:
    _not_imported.append("S3FileSystem")
else:
    initialize_s3()


def __getattr__(name):
    if name in _not_imported:
        raise ImportError(
            "The pyarrow installation is not built with support for "
            "'{0}'".format(name)
        )

    raise AttributeError(
        "module 'pyarrow.fs' has no attribute '{0}'".format(name)
    )


def _ensure_filesystem(filesystem, use_mmap=False):
    if isinstance(filesystem, FileSystem):
        return filesystem

    # handle fsspec-compatible filesystems
    try:
        import fsspec
    except ImportError:
        pass
    else:
        if isinstance(filesystem, fsspec.AbstractFileSystem):
            if type(filesystem).__name__ == 'LocalFileSystem':
                # In case its a simple LocalFileSystem, use native arrow one
                return LocalFileSystem(use_mmap=use_mmap)
            return PyFileSystem(FSSpecHandler(filesystem))

    # map old filesystems to new ones
    from pyarrow.filesystem import LocalFileSystem as LegacyLocalFileSystem

    if isinstance(filesystem, LegacyLocalFileSystem):
        return LocalFileSystem(use_mmap=use_mmap)
    # TODO handle HDFS?

    raise TypeError("Unrecognized filesystem: {}".format(type(filesystem)))


class FSSpecHandler(FileSystemHandler):
    """
    Handler for fsspec-based Python filesystems.

    https://filesystem-spec.readthedocs.io/en/latest/index.html

    >>> PyFileSystem(FSSpecHandler(fsspec_fs))
    """

    def __init__(self, fs):
        self.fs = fs

    def __eq__(self, other):
        if isinstance(other, FSSpecHandler):
            return self.fs == other.fs
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, FSSpecHandler):
            return self.fs != other.fs
        return NotImplemented

    def get_type_name(self):
        protocol = self.fs.protocol
        if isinstance(protocol, list):
            protocol = protocol[0]
        return "fsspec+{0}".format(protocol)

    def normalize_path(self, path):
        return path

    @staticmethod
    def _create_file_info(path, info):
        size = info["size"]
        if info["type"] == "file":
            ftype = FileType.File
        elif info["type"] == "directory":
            ftype = FileType.Directory
            # some fsspec filesystems include a file size for directories
            size = None
        else:
            ftype = FileType.Unknown
        return FileInfo(path, ftype, size=size, mtime=info.get("mtime", None))

    def get_file_info(self, paths):
        infos = []
        for path in paths:
            try:
                info = self.fs.info(path)
            except FileNotFoundError:
                infos.append(FileInfo(path, FileType.NotFound))
            else:
                infos.append(self._create_file_info(path, info))
        return infos

    def get_file_info_selector(self, selector):
        if not self.fs.isdir(selector.base_dir):
            if self.fs.exists(selector.base_dir):
                raise NotADirectoryError(selector.base_dir)
            else:
                if selector.allow_not_found:
                    return []
                else:
                    raise FileNotFoundError(selector.base_dir)

        if selector.recursive:
            maxdepth = None
        else:
            maxdepth = 1

        infos = []
        selected_files = self.fs.find(
            selector.base_dir, maxdepth=maxdepth, withdirs=True, detail=True
        )
        for path, info in selected_files.items():
            infos.append(self._create_file_info(path, info))

        return infos

    def create_dir(self, path, recursive):
        # mkdir also raises FileNotFoundError when base directory is not found
        self.fs.mkdir(path, create_parents=recursive)

    def delete_dir(self, path):
        self.fs.rm(path, recursive=True)

    def _delete_dir_contents(self, path):
        for subpath in self.fs.listdir(path, detail=False):
            if self.fs.isdir(subpath):
                self.fs.rm(subpath, recursive=True)
            elif self.fs.isfile(subpath):
                self.fs.rm(subpath)

    def delete_dir_contents(self, path):
        if path.strip("/") == "":
            raise ValueError(
                "delete_dir_contents called on path '", path, "'")
        self._delete_dir_contents(path)

    def delete_root_dir_contents(self):
        self._delete_dir_contents("/")

    def delete_file(self, path):
        # fs.rm correctly raises IsADirectoryError when `path` is a directory
        # instead of a file and `recursive` is not set to True
        if not self.fs.exists(path):
            raise FileNotFoundError(path)
        self.fs.rm(path)

    def move(self, src, dest):
        self.fs.mv(src, dest, recursive=True)

    def copy_file(self, src, dest):
        # fs.copy correctly raises IsADirectoryError when `src` is a directory
        # instead of a file
        self.fs.copy(src, dest)

    def open_input_stream(self, path):
        from pyarrow import PythonFile

        if not self.fs.isfile(path):
            raise FileNotFoundError(path)

        return PythonFile(self.fs.open(path, mode="rb"), mode="r")

    def open_input_file(self, path):
        from pyarrow import PythonFile

        if not self.fs.isfile(path):
            raise FileNotFoundError(path)

        return PythonFile(self.fs.open(path, mode="rb"), mode="r")

    def open_output_stream(self, path):
        from pyarrow import PythonFile

        return PythonFile(self.fs.open(path, mode="wb"), mode="w")

    def open_append_stream(self, path):
        from pyarrow import PythonFile

        return PythonFile(self.fs.open(path, mode="ab"), mode="w")
