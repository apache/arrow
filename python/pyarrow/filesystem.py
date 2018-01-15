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

from os.path import join as pjoin
import os
import posixpath

from pyarrow.util import implements


class FileSystem(object):
    """
    Abstract filesystem interface
    """
    def cat(self, path):
        """
        Return contents of file as a bytes object

        Returns
        -------
        contents : bytes
        """
        with self.open(path, 'rb') as f:
            return f.read()

    def ls(self, path):
        """
        Return list of file paths
        """
        raise NotImplementedError

    def delete(self, path, recursive=False):
        """
        Delete the indicated file or directory

        Parameters
        ----------
        path : string
        recursive : boolean, default False
            If True, also delete child paths for directories
        """
        raise NotImplementedError

    def disk_usage(self, path):
        """
        Compute bytes used by all contents under indicated path in file tree

        Parameters
        ----------
        path : string
            Can be a file path or directory

        Returns
        -------
        usage : int
        """
        path_info = self.stat(path)
        if path_info['kind'] == 'file':
            return path_info['size']

        total = 0
        for root, directories, files in self.walk(path):
            for child_path in files:
                abspath = self._path_join(root, child_path)
                total += self.stat(abspath)['size']

        return total

    def _path_join(self, *args):
        return self.pathsep.join(args)

    def stat(self, path):
        """

        Returns
        -------
        stat : dict
        """
        raise NotImplementedError('FileSystem.stat')

    def rm(self, path, recursive=False):
        """
        Alias for FileSystem.delete
        """
        return self.delete(path, recursive=recursive)

    def mv(self, path, new_path):
        """
        Alias for FileSystem.rename
        """
        return self.rename(path, new_path)

    def rename(self, path, new_path):
        """
        Rename file, like UNIX mv command

        Parameters
        ----------
        path : string
            Path to alter
        new_path : string
            Path to move to
        """
        raise NotImplementedError('FileSystem.rename')

    def mkdir(self, path, create_parents=True):
        raise NotImplementedError

    def exists(self, path):
        raise NotImplementedError

    def isdir(self, path):
        """
        Return True if path is a directory
        """
        raise NotImplementedError

    def isfile(self, path):
        """
        Return True if path is a file
        """
        raise NotImplementedError

    def _isfilestore(self):
        """
        Returns True if this FileSystem is a unix-style file store with
        directories.
        """
        raise NotImplementedError

    def read_parquet(self, path, columns=None, metadata=None, schema=None,
                     nthreads=1, use_pandas_metadata=False):
        """
        Read Parquet data from path in file system. Can read from a single file
        or a directory of files

        Parameters
        ----------
        path : str
            Single file path or directory
        columns : List[str], optional
            Subset of columns to read
        metadata : pyarrow.parquet.FileMetaData
            Known metadata to validate files against
        schema : pyarrow.parquet.Schema
            Known schema to validate files against. Alternative to metadata
            argument
        nthreads : int, default 1
            Number of columns to read in parallel. If > 1, requires that the
            underlying file source is threadsafe
        use_pandas_metadata : boolean, default False
            If True and file has custom pandas schema metadata, ensure that
            index columns are also loaded

        Returns
        -------
        table : pyarrow.Table
        """
        from pyarrow.parquet import ParquetDataset
        dataset = ParquetDataset(path, schema=schema, metadata=metadata,
                                 filesystem=self)
        return dataset.read(columns=columns, nthreads=nthreads,
                            use_pandas_metadata=use_pandas_metadata)

    def open(self, path, mode='rb'):
        """
        Open file for reading or writing
        """
        raise NotImplementedError

    @property
    def pathsep(self):
        return '/'


class LocalFileSystem(FileSystem):

    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = LocalFileSystem()
        return cls._instance

    @implements(FileSystem.ls)
    def ls(self, path):
        return sorted(pjoin(path, x) for x in os.listdir(path))

    @implements(FileSystem.mkdir)
    def mkdir(self, path, create_parents=True):
        if create_parents:
            os.makedirs(path)
        else:
            os.mkdir(path)

    @implements(FileSystem.isdir)
    def isdir(self, path):
        return os.path.isdir(path)

    @implements(FileSystem.isfile)
    def isfile(self, path):
        return os.path.isfile(path)

    @implements(FileSystem._isfilestore)
    def _isfilestore(self):
        return True

    @implements(FileSystem.exists)
    def exists(self, path):
        return os.path.exists(path)

    @implements(FileSystem.open)
    def open(self, path, mode='rb'):
        """
        Open file for reading or writing
        """
        return open(path, mode=mode)

    @property
    def pathsep(self):
        return os.path.sep

    def walk(self, top_dir):
        """
        Directory tree generator, see os.walk
        """
        return os.walk(top_dir)


class DaskFileSystem(FileSystem):
    """
    Wraps s3fs Dask filesystem implementation like s3fs, gcsfs, etc.
    """

    def __init__(self, fs):
        self.fs = fs

    @implements(FileSystem.isdir)
    def isdir(self, path):
        raise NotImplementedError("Unsupported file system API")

    @implements(FileSystem.isfile)
    def isfile(self, path):
        raise NotImplementedError("Unsupported file system API")

    @implements(FileSystem._isfilestore)
    def _isfilestore(self):
        """
        Object Stores like S3 and GCSFS are based on key lookups, not true
        file-paths
        """
        return False

    @implements(FileSystem.delete)
    def delete(self, path, recursive=False):
        return self.fs.rm(path, recursive=recursive)

    @implements(FileSystem.exists)
    def exists(self, path):
        return self.fs.exists(path)

    @implements(FileSystem.mkdir)
    def mkdir(self, path, create_parents=True):
        if create_parents:
            return self.fs.mkdirs(path)
        else:
            return self.fs.mkdir(path)

    @implements(FileSystem.open)
    def open(self, path, mode='rb'):
        """
        Open file for reading or writing
        """
        return self.fs.open(path, mode=mode)

    def ls(self, path, detail=False):
        return self.fs.ls(path, detail=detail)

    def walk(self, top_path):
        """
        Directory tree generator, like os.walk
        """
        return self.fs.walk(top_path)


class S3FSWrapper(DaskFileSystem):

    @implements(FileSystem.isdir)
    def isdir(self, path):
        try:
            contents = self.fs.ls(path)
            if len(contents) == 1 and contents[0] == path:
                return False
            else:
                return True
        except OSError:
            return False

    @implements(FileSystem.isfile)
    def isfile(self, path):
        try:
            contents = self.fs.ls(path)
            return len(contents) == 1 and contents[0] == path
        except OSError:
            return False

    def walk(self, path, refresh=False):
        """
        Directory tree generator, like os.walk

        Generator version of what is in s3fs, which yields a flattened list of
        files
        """
        path = path.replace('s3://', '')
        directories = set()
        files = set()

        for key in list(self.fs._ls(path, refresh=refresh)):
            path = key['Key']
            if key['StorageClass'] == 'DIRECTORY':
                directories.add(path)
            elif key['StorageClass'] == 'BUCKET':
                pass
            else:
                files.add(path)

        # s3fs creates duplicate 'DIRECTORY' entries
        files = sorted([posixpath.split(f)[1] for f in files
                        if f not in directories])
        directories = sorted([posixpath.split(x)[1]
                              for x in directories])

        yield path, directories, files

        for directory in directories:
            for tup in self.walk(directory, refresh=refresh):
                yield tup
