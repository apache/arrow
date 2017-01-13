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

import pyarrow._parquet as _parquet
from pyarrow.table import Table


class ParquetFile(object):
    """
    Open a Parquet binary file for reading

    Parameters
    ----------
    source : str or pyarrow.io.NativeFile
        Readable source. For passing Python file objects or byte buffers,
        see pyarrow.io.PythonFileInterface or pyarrow.io.BufferReader.
    metadata : ParquetFileMetadata, default None
        Use existing metadata object, rather than reading from file.
    """
    def __init__(self, source, metadata=None):
        self.reader = _parquet.ParquetReader()
        self.reader.open(source, metadata=metadata)

    @property
    def metadata(self):
        return self.reader.metadata

    @property
    def schema(self):
        return self.metadata.schema

    def read(self, nrows=None, columns=None):
        """
        Read a Table from Parquet format

        Parameters
        ----------
        columns: list
            If not None, only these columns will be read from the file.

        Returns
        -------
        pyarrow.table.Table
            Content of the file as a table (of columns)
        """
        if nrows is not None:
            raise NotImplementedError("nrows argument")

        if columns is None:
            return self.reader.read_all()
        else:
            column_idxs = [self.reader.column_name_idx(column)
                           for column in columns]
            arrays = [self.reader.read_column(column_idx)
                      for column_idx in column_idxs]
            return Table.from_arrays(columns, arrays)


def read_table(source, columns=None):
    """
    Read a Table from Parquet format

    Parameters
    ----------
    source: str or pyarrow.io.NativeFile
        Readable source. For passing Python file objects or byte buffers, see
        pyarrow.io.PythonFileInterface or pyarrow.io.BufferReader.
    columns: list
        If not None, only these columns will be read from the file.

    Returns
    -------
    pyarrow.table.Table
        Content of the file as a table (of columns)
    """
    return ParquetFile(source).read(columns=columns)


def write_table(table, sink, chunk_size=None, version=None,
                use_dictionary=True, compression=None):
    """
    Write a Table to Parquet format

    Parameters
    ----------
    table : pyarrow.Table
    sink: string or pyarrow.io.NativeFile
    chunk_size : int
        The maximum number of rows in each Parquet RowGroup. As a default,
        we will write a single RowGroup per file.
    version : {"1.0", "2.0"}, default "1.0"
        The Parquet format version, defaults to 1.0
    use_dictionary : bool or list
        Specify if we should use dictionary encoding in general or only for
        some columns.
    compression : str or dict
        Specify the compression codec, either on a general basis or per-column.
    """
    writer = _parquet.ParquetWriter(sink, use_dictionary=use_dictionary,
                                    compression=compression,
                                    version=version)
    writer.write_table(table, row_group_size=chunk_size)
