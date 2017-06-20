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

from pyarrow.util import implements
import pyarrow.lib as lib


class Filesystem(object):
    """
    Abstract filesystem interface
    """
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

    def read_parquet(self, path, columns=None, metadata=None, schema=None,
                     nthreads=1):
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

        Returns
        -------
        table : pyarrow.Table
        """
        from pyarrow.parquet import ParquetDataset
        dataset = ParquetDataset(path, schema=schema, metadata=metadata,
                                 filesystem=self)
        return dataset.read(columns=columns, nthreads=nthreads)

    @property
    def pathsep(self):
        return '/'


class LocalFilesystem(Filesystem):

    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = LocalFilesystem()
        return cls._instance

    @implements(Filesystem.ls)
    def ls(self, path):
        return sorted(pjoin(path, x) for x in os.listdir(path))

    @implements(Filesystem.mkdir)
    def mkdir(self, path, create_parents=True):
        if create_parents:
            os.makedirs(path)
        else:
            os.mkdir(path)

    @implements(Filesystem.isdir)
    def isdir(self, path):
        return os.path.isdir(path)

    @implements(Filesystem.isfile)
    def isfile(self, path):
        return os.path.isfile(path)

    @implements(Filesystem.exists)
    def exists(self, path):
        return os.path.exists(path)

    def open(self, path, mode='rb'):
        """
        Open file for reading or writing
        """
        return open(path, mode=mode)

    @property
    def pathsep(self):
        return os.path.sep


class HdfsClient(lib._HdfsClient, Filesystem):
    """
    Connect to an HDFS cluster. All parameters are optional and should
    only be set if the defaults need to be overridden.

    Authentication should be automatic if the HDFS cluster uses Kerberos.
    However, if a username is specified, then the ticket cache will likely
    be required.

    Parameters
    ----------
    host : NameNode. Set to "default" for fs.defaultFS from core-site.xml.
    port : NameNode's port. Set to 0 for default or logical (HA) nodes.
    user : Username when connecting to HDFS; None implies login user.
    kerb_ticket : Path to Kerberos ticket cache.
    driver : {'libhdfs', 'libhdfs3'}, default 'libhdfs'
      Connect using libhdfs (JNI-based) or libhdfs3 (3rd-party C++
      library from Pivotal Labs)

    Notes
    -----
    The first time you call this method, it will take longer than usual due
    to JNI spin-up time.

    Returns
    -------
    client : HDFSClient
    """

    def __init__(self, host="default", port=0, user=None, kerb_ticket=None,
                 driver='libhdfs'):
        self._connect(host, port, user, kerb_ticket, driver)

    @implements(Filesystem.isdir)
    def isdir(self, path):
        return lib._HdfsClient.isdir(self, path)

    @implements(Filesystem.isfile)
    def isfile(self, path):
        return lib._HdfsClient.isfile(self, path)

    @implements(Filesystem.delete)
    def delete(self, path, recursive=False):
        return lib._HdfsClient.delete(self, path, recursive)

    @implements(Filesystem.mkdir)
    def mkdir(self, path, create_parents=True):
        return lib._HdfsClient.mkdir(self, path)

    def ls(self, path, full_info=False):
        """
        Retrieve directory contents and metadata, if requested.

        Parameters
        ----------
        path : HDFS path
        full_info : boolean, default False
            If False, only return list of paths

        Returns
        -------
        result : list of dicts (full_info=True) or strings (full_info=False)
        """
        return lib._HdfsClient.ls(self, path, full_info)
