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
    _normalize_path,
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
    from pyarrow._s3fs import S3FileSystem, initialize_s3, finalize_s3  # noqa
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
