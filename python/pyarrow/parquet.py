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

from pyarrow._parquet import (ParquetReader, FileMetaData,  # noqa
                              RowGroupMetaData, Schema, ParquetWriter)
import pyarrow._parquet as _parquet  # noqa
from pyarrow.table import Table, concat_tables


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
        self.reader = ParquetReader()
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
            return Table.from_arrays(arrays, names=columns)


def read_table(source, columns=None, metadata=None):
    """
    Read a Table from Parquet format

    Parameters
    ----------
    source: str or pyarrow.io.NativeFile
        Readable source. For passing Python file objects or byte buffers, see
        pyarrow.io.PythonFileInterface or pyarrow.io.BufferReader.
    columns: list
        If not None, only these columns will be read from the file.
    metadata : FileMetaData
        If separately computed

    Returns
    -------
    pyarrow.Table
        Content of the file as a table (of columns)
    """
    return ParquetFile(source, metadata=metadata).read(columns=columns)


def read_multiple_files(paths, columns=None, filesystem=None, metadata=None,
                        schema=None):
    """
    Read multiple Parquet files as a single pyarrow.Table

    Parameters
    ----------
    paths : List[str]
        List of file paths
    columns : List[str]
        Names of columns to read from the file
    filesystem : Filesystem, default None
        If nothing passed, paths assumed to be found in the local on-disk
        filesystem
    metadata : pyarrow.parquet.FileMetaData
        Use metadata obtained elsewhere to validate file schemas
    schema : pyarrow.parquet.Schema
        Use schema obtained elsewhere to validate file schemas. Alternative to
        metadata parameter

    Returns
    -------
    pyarrow.Table
        Content of the file as a table (of columns)
    """
    if filesystem is None:
        def open_file(path, meta=None):
            return ParquetFile(path, metadata=meta)
    else:
        def open_file(path, meta=None):
            return ParquetFile(filesystem.open(path, mode='rb'), metadata=meta)

    if len(paths) == 0:
        raise ValueError('Must pass at least one file path')

    if metadata is None and schema is None:
        schema = open_file(paths[0]).schema
    elif schema is None:
        schema = metadata.schema

    # Verify schemas are all equal
    all_file_metadata = []
    for path in paths:
        file_metadata = open_file(path).metadata
        if not schema.equals(file_metadata.schema):
            raise ValueError('Schema in {0} was different. {1!s} vs {2!s}'
                             .format(path, file_metadata.schema, schema))
        all_file_metadata.append(file_metadata)

    # Read the tables
    tables = []
    for path, path_metadata in zip(paths, all_file_metadata):
        reader = open_file(path, meta=path_metadata)
        table = reader.read(columns=columns)
        tables.append(table)

    all_data = concat_tables(tables)
    return all_data


def write_table(table, sink, chunk_size=None, version='1.0',
                use_dictionary=True, compression='snappy'):
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
    writer = ParquetWriter(sink, use_dictionary=use_dictionary,
                           compression=compression,
                           version=version)
    writer.write_table(table, row_group_size=chunk_size)
