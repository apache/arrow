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
import pyarrow.io as io


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

    def read_parquet(self, path, columns=None, schema=None):
        """
        Read Parquet data from path in file system. Can read from a single file
        or a directory of files

        Parameters
        ----------
        path : str
            Single file path or directory
        columns : List[str], optional
            Subset of columns to read
        schema : pyarrow.parquet.Schema
            Known schema to validate files against

        Returns
        -------
        table : pyarrow.Table
        """
        from pyarrow.parquet import read_multiple_files

        if self.isdir(path):
            paths_to_read = []
            for path in self.ls(path):
                if path == '_metadata' or path == '_common_metadata':
                    raise ValueError('No support yet for common metadata file')
                paths_to_read.append(path)
        else:
            paths_to_read = [path]

        return read_multiple_files(paths_to_read, columns=columns,
                                   filesystem=self, schema=schema)


class LocalFilesystem(Filesystem):

    @implements(Filesystem.ls)
    def ls(self, path):
        return sorted(pjoin(path, x) for x in os.listdir(path))

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


class HdfsClient(io._HdfsClient, Filesystem):
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
        return io._HdfsClient.isdir(self, path)

    @implements(Filesystem.isfile)
    def isfile(self, path):
        return io._HdfsClient.isfile(self, path)

    @implements(Filesystem.delete)
    def delete(self, path, recursive=False):
        return io._HdfsClient.delete(self, path, recursive)

    @implements(Filesystem.mkdir)
    def mkdir(self, path, create_parents=True):
        return io._HdfsClient.mkdir(self, path)

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
        return io._HdfsClient.ls(self, path, full_info)
