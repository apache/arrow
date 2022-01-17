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


from numbers import Integral
import warnings

from pyarrow.lib import Table
import pyarrow._orc as _orc


class ORCFile:
    """
    Reader interface for a single ORC file

    Parameters
    ----------
    source : str or pyarrow.io.NativeFile
        Readable source. For passing Python file objects or byte buffers,
        see pyarrow.io.PythonFileInterface or pyarrow.io.BufferReader.
    """

    def __init__(self, source):
        self.reader = _orc.ORCReader()
        self.reader.open(source)

    @property
    def metadata(self):
        """The file metadata, as an arrow KeyValueMetadata"""
        return self.reader.metadata()

    @property
    def schema(self):
        """The file schema, as an arrow schema"""
        return self.reader.schema()

    @property
    def nrows(self):
        """The number of rows in the file"""
        return self.reader.nrows()

    @property
    def nstripes(self):
        """The number of stripes in the file"""
        return self.reader.nstripes()

    @property
    def file_version(self):
        """Format version of the ORC file, must be 0.11 or 0.12"""
        return self.reader.file_version()

    @property
    def software_version(self):
        """Software instance and version that wrote this file"""
        return self.reader.software_version()

    @property
    def compression(self):
        """Compression codec of the file"""
        return self.reader.compression()

    @property
    def compression_size(self):
        """Number of bytes to buffer for the compression codec in the file"""
        return self.reader.compression_size()

    @property
    def writer(self):
        """Name of the writer that wrote this file.
        If the writer is unknown then its Writer ID
        (a number) is returned"""
        return self.reader.writer()

    @property
    def writer_version(self):
        """Version of the writer"""
        return self.reader.writer_version()

    @property
    def row_index_stride(self):
        """Number of rows per an entry in the row index or 0
        if there is no row index"""
        return self.reader.row_index_stride()

    @property
    def nstripe_statistics(self):
        """Number of stripe statistics"""
        return self.reader.nstripe_statistics()

    @property
    def content_length(self):
        """Length of the data stripes in the file in bytes"""
        return self.reader.content_length()

    @property
    def stripe_statistics_length(self):
        """The number of compressed bytes in the file stripe statistics"""
        return self.reader.stripe_statistics_length()

    @property
    def file_footer_length(self):
        """The number of compressed bytes in the file footer"""
        return self.reader.file_footer_length()

    @property
    def file_postscript_length(self):
        """The number of bytes in the file postscript"""
        return self.reader.file_postscript_length()

    @property
    def file_length(self):
        """The number of bytes in the file"""
        return self.reader.file_length()

    def _select_names(self, columns=None):
        if columns is None:
            return None

        schema = self.schema
        names = []
        for col in columns:
            if isinstance(col, Integral):
                col = int(col)
                if 0 <= col < len(schema):
                    col = schema[col].name
                    names.append(col)
                else:
                    raise ValueError("Column indices must be in 0 <= ind < %d,"
                                     " got %d" % (len(schema), col))
            else:
                return columns

        return names

    def read_stripe(self, n, columns=None):
        """Read a single stripe from the file.

        Parameters
        ----------
        n : int
            The stripe index
        columns : list
            If not None, only these columns will be read from the stripe. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'

        Returns
        -------
        pyarrow.lib.RecordBatch
            Content of the stripe as a RecordBatch.
        """
        columns = self._select_names(columns)
        return self.reader.read_stripe(n, columns=columns)

    def read(self, columns=None):
        """Read the whole file.

        Parameters
        ----------
        columns : list
            If not None, only these columns will be read from the file. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'

        Returns
        -------
        pyarrow.lib.Table
            Content of the file as a Table.
        """
        columns = self._select_names(columns)
        return self.reader.read(columns=columns)


_orc_writer_args_docs = """file_version : {"0.11", "0.12"}, default "0.12"
    Determine which ORC file version to use. Hive 0.11 / ORC v0 is the older
    version as defined `here <https://orc.apache.org/specification/ORCv0/>`
    while Hive 0.12 / ORC v1 is the newer one as defined
    `here <https://orc.apache.org/specification/ORCv1/>`.
batch_size : int, default 1024
    Number of rows the ORC writer writes at a time.
stripe_size : int, default 64 * 1024 * 1024
    Size of each ORC stripe.
compression : string, default 'zlib'
    Specify the compression codec.
    Valid values: {'UNCOMPRESSED', 'SNAPPY', 'ZLIB', 'LZ0', 'LZ4', 'ZSTD'}
compression_block_size : int, default 64 * 1024
    Specify the size of each compression block.
compression_strategy : string, default 'speed'
    Specify the compression strategy i.e. speed vs size reduction.
    Valid values: {'SPEED', 'COMPRESSION'}
row_index_stride : int, default 10000
    Specify the row index stride i.e. the number of rows per
    an entry in the row index.
padding_tolerance : double, default 0.0
    Set the padding tolerance.
dictionary_key_size_threshold : double, default 0.0
    Set the dictionary key size threshold. 0 to disable dictionary encoding.
    1 to always enable dictionary encoding.
bloom_filter_columns : None, set-like or list-like, default None
    Set columns that use the bloom filter.
bloom_filter_fpp: double, default 0.05
    Set false positive probability of the bloom filter.
"""


class ORCWriter:
    __doc__ = """
Writer interface for a single ORC file

Parameters
----------
where : str or pyarrow.io.NativeFile
    Writable target. For passing Python file objects or byte buffers,
    see pyarrow.io.PythonFileInterface, pyarrow.io.BufferOutputStream
    or pyarrow.io.FixedSizeBufferWriter.
{}
""".format(_orc_writer_args_docs)

    def __init__(self, where, file_version='0.12',
                 batch_size=1024,
                 stripe_size=67108864,
                 compression='zlib',
                 compression_block_size=65536,
                 compression_strategy='speed',
                 row_index_stride=10000,
                 padding_tolerance=0.0,
                 dictionary_key_size_threshold=0.0,
                 bloom_filter_columns=None,
                 bloom_filter_fpp=0.05,
                 ):

        self.writer = _orc.ORCWriter()
        self.writer.open(
            where,
            file_version=file_version,
            batch_size=batch_size,
            stripe_size=stripe_size,
            compression=compression,
            compression_block_size=compression_block_size,
            compression_strategy=compression_strategy,
            row_index_stride=row_index_stride,
            padding_tolerance=padding_tolerance,
            dictionary_key_size_threshold=dictionary_key_size_threshold,
            bloom_filter_columns=bloom_filter_columns,
            bloom_filter_fpp=bloom_filter_fpp
        )
        self.is_open = True

    def __del__(self):
        if getattr(self, 'is_open', False):
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()
        # return false since we want to propagate exceptions
        return False

    def write(self, table):
        """
        Write the table into an ORC file. The schema of the table must
        be equal to the schema used when opening the ORC file.

        Parameters
        ----------
        table : pyarrow.lib.Table
            The table to be written into the ORC file
        """
        assert self.is_open
        self.writer.write(table)

    def close(self):
        """
        Close the ORC file
        """
        if self.is_open:
            self.writer.close()
            self.is_open = False


def write_table(table, where, file_version='0.12',
                batch_size=1024,
                stripe_size=67108864,
                compression='zlib',
                compression_block_size=65536,
                compression_strategy='speed',
                row_index_stride=10000,
                padding_tolerance=0.0,
                dictionary_key_size_threshold=0.0,
                bloom_filter_columns=None,
                bloom_filter_fpp=0.05):
    if isinstance(where, Table):
        warnings.warn(
            "The order of the arguments has changed. Pass as "
            "'write_table(table, where)' instead. The old order will raise "
            "an error in the future.", FutureWarning, stacklevel=2
        )
        table, where = where, table
    with ORCWriter(
        where,
        file_version=file_version,
        batch_size=batch_size,
        stripe_size=stripe_size,
        compression=compression,
        compression_block_size=compression_block_size,
        compression_strategy=compression_strategy,
        row_index_stride=row_index_stride,
        padding_tolerance=padding_tolerance,
        dictionary_key_size_threshold=dictionary_key_size_threshold,
        bloom_filter_columns=bloom_filter_columns,
        bloom_filter_fpp=bloom_filter_fpp
    ) as writer:
        writer.write(table)


write_table.__doc__ = """
Write a table into an ORC file

Parameters
----------
table : pyarrow.lib.Table
    The table to be written into the ORC file
where : str or pyarrow.io.NativeFile
    Writable target. For passing Python file objects or byte buffers,
    see pyarrow.io.PythonFileInterface, pyarrow.io.BufferOutputStream
    or pyarrow.io.FixedSizeBufferWriter.
{}
""".format(_orc_writer_args_docs)
