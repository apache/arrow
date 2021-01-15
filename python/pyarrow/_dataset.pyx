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

"""Dataset is currently unstable. APIs subject to change without notice."""

from cpython.object cimport Py_LT, Py_EQ, Py_GT, Py_LE, Py_NE, Py_GE
from cython.operator cimport dereference as deref

import os

import pyarrow as pa
from pyarrow.lib cimport *
from pyarrow.lib import frombytes, tobytes
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow._fs cimport FileSystem, FileInfo, FileSelector
from pyarrow._csv cimport ParseOptions
from pyarrow.util import _is_path_like, _stringify_path

from pyarrow._parquet cimport (
    _create_writer_properties, _create_arrow_writer_properties,
    FileMetaData, RowGroupMetaData, ColumnChunkMetaData
)


def _forbid_instantiation(klass, subclasses_instead=True):
    msg = '{} is an abstract class thus cannot be initialized.'.format(
        klass.__name__
    )
    if subclasses_instead:
        subclasses = [cls.__name__ for cls in klass.__subclasses__]
        msg += ' Use one of the subclasses instead: {}'.format(
            ', '.join(subclasses)
        )
    raise TypeError(msg)


cdef CFileSource _make_file_source(object file, FileSystem filesystem=None):

    cdef:
        CFileSource c_source
        shared_ptr[CFileSystem] c_filesystem
        c_string c_path
        shared_ptr[CRandomAccessFile] c_file
        shared_ptr[CBuffer] c_buffer

    if isinstance(file, Buffer):
        c_buffer = pyarrow_unwrap_buffer(file)
        c_source = CFileSource(move(c_buffer))

    elif _is_path_like(file):
        if filesystem is None:
            raise ValueError("cannot construct a FileSource from "
                             "a path without a FileSystem")
        c_filesystem = filesystem.unwrap()
        c_path = tobytes(_stringify_path(file))
        c_source = CFileSource(move(c_path), move(c_filesystem))

    elif hasattr(file, 'read'):
        # Optimistically hope this is file-like
        c_file = get_native_file(file, False).get_random_access_file()
        c_source = CFileSource(move(c_file))

    else:
        raise TypeError("cannot construct a FileSource "
                        "from " + str(file))

    return c_source


cdef class Expression(_Weakrefable):

    cdef:
        CExpression expr

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const CExpression& sp):
        self.expr = sp

    @staticmethod
    cdef wrap(const CExpression& sp):
        cdef Expression self = Expression.__new__(Expression)
        self.init(sp)
        return self

    cdef inline CExpression unwrap(self):
        return self.expr

    def equals(self, Expression other):
        return self.expr.Equals(other.unwrap())

    def __str__(self):
        return frombytes(self.expr.ToString())

    def __repr__(self):
        return "<pyarrow.dataset.{0} {1}>".format(
            self.__class__.__name__, str(self)
        )

    @staticmethod
    def _deserialize(Buffer buffer not None):
        return Expression.wrap(GetResultValue(CDeserializeExpression(
            pyarrow_unwrap_buffer(buffer))))

    def __reduce__(self):
        buffer = pyarrow_wrap_buffer(GetResultValue(
            CSerializeExpression(self.expr)))
        return Expression._deserialize, (buffer,)

    @staticmethod
    cdef Expression _expr_or_scalar(object expr):
        if isinstance(expr, Expression):
            return (<Expression> expr)
        return (<Expression> Expression._scalar(expr))

    @staticmethod
    cdef Expression _call(str function_name, list arguments,
                          shared_ptr[CFunctionOptions] options=(
                              <shared_ptr[CFunctionOptions]> nullptr)):
        cdef:
            vector[CExpression] c_arguments

        for argument in arguments:
            c_arguments.push_back((<Expression> argument).expr)

        return Expression.wrap(CMakeCallExpression(tobytes(function_name),
                                                   move(c_arguments), options))

    def __richcmp__(self, other, int op):
        other = Expression._expr_or_scalar(other)
        return Expression._call({
            Py_EQ: "equal",
            Py_NE: "not_equal",
            Py_GT: "greater",
            Py_GE: "greater_equal",
            Py_LT: "less",
            Py_LE: "less_equal",
        }[op], [self, other])

    def __invert__(self):
        return Expression._call("invert", [self])

    def __and__(Expression self, other):
        other = Expression._expr_or_scalar(other)
        return Expression._call("and_kleene", [self, other])

    def __or__(Expression self, other):
        other = Expression._expr_or_scalar(other)
        return Expression._call("or_kleene", [self, other])

    def is_valid(self):
        """Checks whether the expression is not-null (valid)"""
        return Expression._call("is_valid", [self])

    def cast(self, type, bint safe=True):
        """Explicitly change the expression's data type"""
        cdef shared_ptr[CCastOptions] c_options
        c_options.reset(new CCastOptions(safe))
        c_options.get().to_type = pyarrow_unwrap_data_type(ensure_type(type))
        return Expression._call("cast", [self],
                                <shared_ptr[CFunctionOptions]> c_options)

    def isin(self, values):
        """Checks whether the expression is contained in values"""
        cdef:
            shared_ptr[CFunctionOptions] c_options
            CDatum c_values

        if not isinstance(values, pa.Array):
            values = pa.array(values)

        c_values = CDatum(pyarrow_unwrap_array(values))
        c_options.reset(new CSetLookupOptions(c_values, True))
        return Expression._call("is_in", [self], c_options)

    @staticmethod
    def _field(str name not None):
        return Expression.wrap(CMakeFieldExpression(tobytes(name)))

    @staticmethod
    def _scalar(value):
        cdef:
            Scalar scalar

        if isinstance(value, Scalar):
            scalar = value
        else:
            scalar = pa.scalar(value)

        return Expression.wrap(CMakeScalarExpression(scalar.unwrap()))


_deserialize = Expression._deserialize
cdef Expression _true = Expression._scalar(True)


cdef class Dataset(_Weakrefable):
    """
    Collection of data fragments and potentially child datasets.

    Arrow Datasets allow you to query against data that has been split across
    multiple files. This sharding of data may indicate partitioning, which
    can accelerate queries that only touch some partitions (files).
    """

    cdef:
        shared_ptr[CDataset] wrapped
        CDataset* dataset

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CDataset]& sp):
        self.wrapped = sp
        self.dataset = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CDataset]& sp):
        type_name = frombytes(sp.get().type_name())

        classes = {
            'union': UnionDataset,
            'filesystem': FileSystemDataset,
        }

        class_ = classes.get(type_name, None)
        if class_ is None:
            raise TypeError(type_name)

        cdef Dataset self = class_.__new__(class_)
        self.init(sp)
        return self

    cdef shared_ptr[CDataset] unwrap(self) nogil:
        return self.wrapped

    @property
    def partition_expression(self):
        """
        An Expression which evaluates to true for all data viewed by this
        Dataset.
        """
        return Expression.wrap(self.dataset.partition_expression())

    def replace_schema(self, Schema schema not None):
        """
        Return a copy of this Dataset with a different schema.

        The copy will view the same Fragments. If the new schema is not
        compatible with the original dataset's schema then an error will
        be raised.
        """
        cdef shared_ptr[CDataset] copy = GetResultValue(
            self.dataset.ReplaceSchema(pyarrow_unwrap_schema(schema)))
        return Dataset.wrap(move(copy))

    def get_fragments(self, Expression filter=None):
        """Returns an iterator over the fragments in this dataset.

        Parameters
        ----------
        filter : Expression, default None
            Return fragments matching the optional filter, either using the
            partition_expression or internal information like Parquet's
            statistics.

        Returns
        -------
        fragments : iterator of Fragment
        """
        cdef:
            CExpression c_filter
            CFragmentIterator c_iterator

        if filter is None:
            c_fragments = move(GetResultValue(self.dataset.GetFragments()))
        else:
            c_filter = _bind(filter, self.schema)
            c_fragments = move(GetResultValue(
                self.dataset.GetFragments(c_filter)))

        for maybe_fragment in c_fragments:
            yield Fragment.wrap(GetResultValue(move(maybe_fragment)))

    def _scanner(self, **kwargs):
        return Scanner.from_dataset(self, **kwargs)

    def scan(self, **kwargs):
        """Builds a scan operation against the dataset.

        It produces a stream of ScanTasks which is meant to be a unit of work
        to be dispatched. The tasks are not executed automatically, the user is
        responsible to execute and dispatch the individual tasks, so custom
        local task scheduling can be implemented.

        Parameters
        ----------
        columns : list of str, default None
            List of columns to project. Order and duplicates will be preserved.
            The columns will be passed down to Datasets and corresponding data
            fragments to avoid loading, copying, and deserializing columns
            that will not be required further down the compute chain.
            By default all of the available columns are projected. Raises
            an exception if any of the referenced column names does not exist
            in the dataset's Schema.
        filter : Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in the data
            source, e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        batch_size : int, default 1M
            The maximum row count for scanned record batches. If scanned
            record batches are overflowing memory then this method can be
            called to reduce their size.
        use_threads : bool, default True
            If enabled, then maximum parallelism will be used determined by
            the number of available CPU cores.
        memory_pool : MemoryPool, default None
            For memory allocations, if required. If not specified, uses the
            default pool.

        Returns
        -------
        scan_tasks : iterator of ScanTask
        """
        return self._scanner(**kwargs).scan()

    def to_batches(self, **kwargs):
        """Read the dataset as materialized record batches.

        Builds a scan operation against the dataset and sequentially executes
        the ScanTasks as the returned generator gets consumed.

        See scan method parameters documentation.

        Returns
        -------
        record_batches : iterator of RecordBatch
        """
        return self._scanner(**kwargs).to_batches()

    def to_table(self, **kwargs):
        """Read the dataset to an arrow table.

        Note that this method reads all the selected data from the dataset
        into memory.

        See scan method parameters documentation.

        Returns
        -------
        table : Table instance
        """
        return self._scanner(**kwargs).to_table()

    @property
    def schema(self):
        """The common schema of the full Dataset"""
        return pyarrow_wrap_schema(self.dataset.schema())


cdef class UnionDataset(Dataset):
    """A Dataset wrapping child datasets.

    Children's schemas must agree with the provided schema.

    Parameters
    ----------
    schema : Schema
        A known schema to conform to.
    children : list of Dataset
        One or more input children
    """

    cdef:
        CUnionDataset* union_dataset

    def __init__(self, Schema schema not None, children):
        cdef:
            Dataset child
            CDatasetVector c_children
            shared_ptr[CUnionDataset] union_dataset

        for child in children:
            c_children.push_back(child.wrapped)

        union_dataset = GetResultValue(CUnionDataset.Make(
            pyarrow_unwrap_schema(schema), move(c_children)))
        self.init(<shared_ptr[CDataset]> union_dataset)

    cdef void init(self, const shared_ptr[CDataset]& sp):
        Dataset.init(self, sp)
        self.union_dataset = <CUnionDataset*> sp.get()

    def __reduce__(self):
        return UnionDataset, (self.schema, self.children)

    @property
    def children(self):
        cdef CDatasetVector children = self.union_dataset.children()
        return [Dataset.wrap(children[i]) for i in range(children.size())]


cdef class FileSystemDataset(Dataset):
    """A Dataset of file fragments.

    A FileSystemDataset is composed of one or more FileFragment.

    Parameters
    ----------
    fragments : list[Fragments]
        List of fragments to consume.
    schema : Schema
        The top-level schema of the Dataset.
    format : FileFormat
        File format of the fragments, currently only ParquetFileFormat,
        IpcFileFormat, and CsvFileFormat are supported.
    filesystem : FileSystem
        FileSystem of the fragments.
    root_partition : Expression, optional
        The top-level partition of the DataDataset.
    """

    cdef:
        CFileSystemDataset* filesystem_dataset

    def __init__(self, fragments, Schema schema, FileFormat format,
                 FileSystem filesystem=None, root_partition=None):
        cdef:
            FileFragment fragment=None
            vector[shared_ptr[CFileFragment]] c_fragments
            CResult[shared_ptr[CDataset]] result
            shared_ptr[CFileSystem] c_filesystem

        root_partition = root_partition or _true
        if not isinstance(root_partition, Expression):
            raise TypeError(
                "Argument 'root_partition' has incorrect type (expected "
                "Epression, got {0})".format(type(root_partition))
            )

        for fragment in fragments:
            c_fragments.push_back(
                static_pointer_cast[CFileFragment, CFragment](
                    fragment.unwrap()))

            if filesystem is None:
                filesystem = fragment.filesystem

        if filesystem is not None:
            c_filesystem = filesystem.unwrap()

        result = CFileSystemDataset.Make(
            pyarrow_unwrap_schema(schema),
            (<Expression> root_partition).unwrap(),
            format.unwrap(),
            c_filesystem,
            c_fragments
        )
        self.init(GetResultValue(result))

    @property
    def filesystem(self):
        return FileSystem.wrap(self.filesystem_dataset.filesystem())

    cdef void init(self, const shared_ptr[CDataset]& sp):
        Dataset.init(self, sp)
        self.filesystem_dataset = <CFileSystemDataset*> sp.get()

    def __reduce__(self):
        return FileSystemDataset, (
            list(self.get_fragments()),
            self.schema,
            self.format,
            self.filesystem,
            self.partition_expression
        )

    @classmethod
    def from_paths(cls, paths, schema=None, format=None,
                   filesystem=None, partitions=None, root_partition=None):
        """A Dataset created from a list of paths on a particular filesystem.

        Parameters
        ----------
        paths : list of str
            List of file paths to create the fragments from.
        schema : Schema
            The top-level schema of the DataDataset.
        format : FileFormat
            File format to create fragments from, currently only
            ParquetFileFormat, IpcFileFormat, and CsvFileFormat are supported.
        filesystem : FileSystem
            The filesystem which files are from.
        partitions : List[Expression], optional
            Attach additional partition information for the file paths.
        root_partition : Expression, optional
            The top-level partition of the DataDataset.
        """
        cdef:
            FileFragment fragment

        root_partition = root_partition or _true
        for arg, class_, name in [
            (schema, Schema, 'schema'),
            (format, FileFormat, 'format'),
            (filesystem, FileSystem, 'filesystem'),
            (root_partition, Expression, 'root_partition')
        ]:
            if not isinstance(arg, class_):
                raise TypeError(
                    "Argument '{0}' has incorrect type (expected {1}, "
                    "got {2})".format(name, class_.__name__, type(arg))
                )

        partitions = partitions or [_true] * len(paths)

        if len(paths) != len(partitions):
            raise ValueError(
                'The number of files resulting from paths_or_selector '
                'must be equal to the number of partitions.'
            )

        fragments = [
            format.make_fragment(path, filesystem, partitions[i])
            for i, path in enumerate(paths)
        ]
        return FileSystemDataset(fragments, schema, format,
                                 filesystem, root_partition)

    @property
    def files(self):
        """List of the files"""
        cdef vector[c_string] files = self.filesystem_dataset.files()
        return [frombytes(f) for f in files]

    @property
    def format(self):
        """The FileFormat of this source."""
        return FileFormat.wrap(self.filesystem_dataset.format())


cdef CExpression _bind(Expression filter, Schema schema) except *:
    assert schema is not None

    if filter is None:
        return _true.unwrap()

    return GetResultValue(filter.unwrap().Bind(
        deref(pyarrow_unwrap_schema(schema).get())))


cdef class FileWriteOptions(_Weakrefable):

    cdef:
        shared_ptr[CFileWriteOptions] wrapped
        CFileWriteOptions* options

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CFileWriteOptions]& sp):
        self.wrapped = sp
        self.options = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CFileWriteOptions]& sp):
        type_name = frombytes(sp.get().type_name())

        classes = {
            'ipc': IpcFileWriteOptions,
            'parquet': ParquetFileWriteOptions,
        }

        class_ = classes.get(type_name, None)
        if class_ is None:
            raise TypeError(type_name)

        cdef FileWriteOptions self = class_.__new__(class_)
        self.init(sp)
        return self

    @property
    def format(self):
        return FileFormat.wrap(self.options.format())

    cdef inline shared_ptr[CFileWriteOptions] unwrap(self):
        return self.wrapped


cdef class FileFormat(_Weakrefable):

    cdef:
        shared_ptr[CFileFormat] wrapped
        CFileFormat* format

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CFileFormat]& sp):
        self.wrapped = sp
        self.format = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CFileFormat]& sp):
        type_name = frombytes(sp.get().type_name())

        classes = {
            'ipc': IpcFileFormat,
            'csv': CsvFileFormat,
            'parquet': ParquetFileFormat,
        }

        class_ = classes.get(type_name, None)
        if class_ is None:
            raise TypeError(type_name)

        cdef FileFormat self = class_.__new__(class_)
        self.init(sp)
        return self

    cdef inline shared_ptr[CFileFormat] unwrap(self):
        return self.wrapped

    def inspect(self, file, filesystem=None):
        """Infer the schema of a file."""
        c_source = _make_file_source(file, filesystem)
        c_schema = GetResultValue(self.format.Inspect(c_source))
        return pyarrow_wrap_schema(move(c_schema))

    def make_fragment(self, file, filesystem=None,
                      Expression partition_expression=None):
        """
        Make a FileFragment of this FileFormat. The filter may not reference
        fields absent from the provided schema. If no schema is provided then
        one will be inferred.
        """
        partition_expression = partition_expression or _true

        c_source = _make_file_source(file, filesystem)
        c_fragment = <shared_ptr[CFragment]> GetResultValue(
            self.format.MakeFragment(move(c_source),
                                     partition_expression.unwrap(),
                                     <shared_ptr[CSchema]>nullptr))
        return Fragment.wrap(move(c_fragment))

    def make_write_options(self):
        return FileWriteOptions.wrap(self.format.DefaultWriteOptions())

    @property
    def default_extname(self):
        return frombytes(self.format.type_name())

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return False


cdef class Fragment(_Weakrefable):
    """Fragment of data from a Dataset."""

    cdef:
        shared_ptr[CFragment] wrapped
        CFragment* fragment

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CFragment]& sp):
        self.wrapped = sp
        self.fragment = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CFragment]& sp):
        type_name = frombytes(sp.get().type_name())

        classes = {
            # IpcFileFormat and CsvFileFormat do not have corresponding
            # subclasses of FileFragment
            'ipc': FileFragment,
            'csv': FileFragment,
            'parquet': ParquetFileFragment,
        }

        class_ = classes.get(type_name, None)
        if class_ is None:
            class_ = Fragment

        cdef Fragment self = class_.__new__(class_)
        self.init(sp)
        return self

    cdef inline shared_ptr[CFragment] unwrap(self):
        return self.wrapped

    @property
    def physical_schema(self):
        """Return the physical schema of this Fragment. This schema can be
        different from the dataset read schema."""
        cdef:
            shared_ptr[CSchema] c_schema

        c_schema = GetResultValue(self.fragment.ReadPhysicalSchema())
        return pyarrow_wrap_schema(c_schema)

    @property
    def partition_expression(self):
        """An Expression which evaluates to true for all data viewed by this
        Fragment.
        """
        return Expression.wrap(self.fragment.partition_expression())

    def _scanner(self, **kwargs):
        return Scanner.from_fragment(self, **kwargs)

    def scan(self, Schema schema=None, **kwargs):
        """Builds a scan operation against the dataset.

        It produces a stream of ScanTasks which is meant to be a unit of work
        to be dispatched. The tasks are not executed automatically, the user is
        responsible to execute and dispatch the individual tasks, so custom
        local task scheduling can be implemented.

        Parameters
        ----------
        schema : Schema
            Schema to use for scanning. This is used to unify a Fragment to
            it's Dataset's schema. If not specified this will use the
            Fragment's physical schema which might differ for each Fragment.
        columns : list of str, default None
            List of columns to project. Order and duplicates will be preserved.
            The columns will be passed down to Datasets and corresponding data
            fragments to avoid loading, copying, and deserializing columns
            that will not be required further down the compute chain.
            By default all of the available columns are projected. Raises
            an exception if any of the referenced column names does not exist
            in the dataset's Schema.
        filter : Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in the data
            source, e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        batch_size : int, default 1M
            The maximum row count for scanned record batches. If scanned
            record batches are overflowing memory then this method can be
            called to reduce their size.
        use_threads : bool, default True
            If enabled, then maximum parallelism will be used determined by
            the number of available CPU cores.
        memory_pool : MemoryPool, default None
            For memory allocations, if required. If not specified, uses the
            default pool.

        Returns
        -------
        scan_tasks : iterator of ScanTask
        """
        return self._scanner(schema=schema, **kwargs).scan()

    def to_batches(self, Schema schema=None, **kwargs):
        """Read the fragment as materialized record batches.

        See scan method parameters documentation.

        Returns
        -------
        record_batches : iterator of RecordBatch
        """
        return self._scanner(schema=schema, **kwargs).to_batches()

    def to_table(self, Schema schema=None, **kwargs):
        """Convert this Fragment into a Table.

        Use this convenience utility with care. This will serially materialize
        the Scan result in memory before creating the Table.

        See scan method parameters documentation.

        Returns
        -------
        table : Table
        """
        return self._scanner(schema=schema, **kwargs).to_table()


cdef class FileFragment(Fragment):
    """A Fragment representing a data file."""

    cdef:
        CFileFragment* file_fragment

    cdef void init(self, const shared_ptr[CFragment]& sp):
        Fragment.init(self, sp)
        self.file_fragment = <CFileFragment*> sp.get()

    def __reduce__(self):
        buffer = self.buffer
        return self.format.make_fragment, (
            self.path if buffer is None else buffer,
            self.filesystem,
            self.partition_expression
        )

    @property
    def path(self):
        """
        The path of the data file viewed by this fragment, if it views a
        file. If instead it views a buffer, this will be "<Buffer>".
        """
        return frombytes(self.file_fragment.source().path())

    @property
    def filesystem(self):
        """
        The FileSystem containing the data file viewed by this fragment, if
        it views a file. If instead it views a buffer, this will be None.
        """
        cdef:
            shared_ptr[CFileSystem] c_fs
        c_fs = self.file_fragment.source().filesystem()

        if c_fs.get() == nullptr:
            return None

        return FileSystem.wrap(c_fs)

    @property
    def buffer(self):
        """
        The buffer viewed by this fragment, if it views a buffer. If
        instead it views a file, this will be None.
        """
        cdef:
            shared_ptr[CBuffer] c_buffer
        c_buffer = self.file_fragment.source().buffer()

        if c_buffer.get() == nullptr:
            return None

        return pyarrow_wrap_buffer(c_buffer)

    @property
    def format(self):
        """
        The format of the data file viewed by this fragment.
        """
        return FileFormat.wrap(self.file_fragment.format())


class RowGroupInfo:
    """A wrapper class for RowGroup information"""

    def __init__(self, id, metadata, schema):
        self.id = id
        self.metadata = metadata
        self.schema = schema

    @property
    def num_rows(self):
        return self.metadata.num_rows

    @property
    def total_byte_size(self):
        return self.metadata.total_byte_size

    @property
    def statistics(self):
        def name_stats(i):
            col = self.metadata.column(i)

            stats = col.statistics
            if stats is None or not stats.has_min_max:
                return None, None

            name = col.path_in_schema
            field_index = self.schema.get_field_index(name)
            if field_index < 0:
                return None, None

            typ = self.schema.field(field_index).type
            return col.path_in_schema, {
                'min': pa.scalar(stats.min, type=typ).as_py(),
                'max': pa.scalar(stats.max, type=typ).as_py()
            }

        return {
            name: stats for name, stats
            in map(name_stats, range(self.metadata.num_columns))
            if stats is not None
        }

    def __repr__(self):
        return "RowGroupInfo({})".format(self.id)

    def __eq__(self, other):
        if isinstance(other, int):
            return self.id == other
        if not isinstance(other, RowGroupInfo):
            return False
        return self.id == other.id


cdef class ParquetFileFragment(FileFragment):
    """A Fragment representing a parquet file."""

    cdef:
        CParquetFileFragment* parquet_file_fragment

    cdef void init(self, const shared_ptr[CFragment]& sp):
        FileFragment.init(self, sp)
        self.parquet_file_fragment = <CParquetFileFragment*> sp.get()

    def __reduce__(self):
        buffer = self.buffer
        row_groups = [row_group.id for row_group in self.row_groups]
        return self.format.make_fragment, (
            self.path if buffer is None else buffer,
            self.filesystem,
            self.partition_expression,
            row_groups
        )

    def ensure_complete_metadata(self):
        """
        Ensure that all metadata (statistics, physical schema, ...) have
        been read and cached in this fragment.
        """
        check_status(self.parquet_file_fragment.EnsureCompleteMetadata())

    @property
    def row_groups(self):
        metadata = self.metadata
        cdef vector[int] row_groups = self.parquet_file_fragment.row_groups()
        return [RowGroupInfo(i, metadata.row_group(i), self.physical_schema)
                for i in row_groups]

    @property
    def metadata(self):
        self.ensure_complete_metadata()
        cdef FileMetaData metadata = FileMetaData()
        metadata.init(self.parquet_file_fragment.metadata())
        return metadata

    @property
    def num_row_groups(self):
        """
        Return the number of row groups viewed by this fragment (not the
        number of row groups in the origin file).
        """
        self.ensure_complete_metadata()
        return self.parquet_file_fragment.row_groups().size()

    def split_by_row_group(self, Expression filter=None,
                           Schema schema=None):
        """
        Split the fragment into multiple fragments.

        Yield a Fragment wrapping each row group in this ParquetFileFragment.
        Row groups will be excluded whose metadata contradicts the optional
        filter.

        Parameters
        ----------
        filter : Expression, default None
            Only include the row groups which satisfy this predicate (using
            the Parquet RowGroup statistics).
        schema : Schema, default None
            Schema to use when filtering row groups. Defaults to the
            Fragment's phsyical schema

        Returns
        -------
        A list of Fragments
        """
        cdef:
            vector[shared_ptr[CFragment]] c_fragments
            CExpression c_filter
            shared_ptr[CFragment] c_fragment

        schema = schema or self.physical_schema
        c_filter = _bind(filter, schema)
        with nogil:
            c_fragments = move(GetResultValue(
                self.parquet_file_fragment.SplitByRowGroup(move(c_filter))))

        return [Fragment.wrap(c_fragment) for c_fragment in c_fragments]

    def subset(self, Expression filter=None, Schema schema=None,
               object row_group_ids=None):
        """
        Create a subset of the fragment (viewing a subset of the row groups).

        Subset can be specified by either a filter predicate (with optional
        schema) or by a list of row group IDs. Note that when using a filter,
        the resulting fragment can be empty (viewing no row groups).

        Parameters
        ----------
        filter : Expression, default None
            Only include the row groups which satisfy this predicate (using
            the Parquet RowGroup statistics).
        schema : Schema, default None
            Schema to use when filtering row groups. Defaults to the
            Fragment's phsyical schema
        row_group_ids : list of ints
            The row group IDs to include in the subset. Can only be specified
            if `filter` is None.

        Returns
        -------
        ParquetFileFragment
        """
        cdef:
            CExpression c_filter
            vector[int] c_row_group_ids
            shared_ptr[CFragment] c_fragment

        if filter is not None and row_group_ids is not None:
            raise ValueError(
                "Cannot specify both 'filter' and 'row_group_ids'."
            )

        if filter is not None:
            schema = schema or self.physical_schema
            c_filter = _bind(filter, schema)
            with nogil:
                c_fragment = move(GetResultValue(
                    self.parquet_file_fragment.SubsetWithFilter(
                        move(c_filter))))
        elif row_group_ids is not None:
            c_row_group_ids = [
                <int> row_group for row_group in sorted(set(row_group_ids))
            ]
            with nogil:
                c_fragment = move(GetResultValue(
                    self.parquet_file_fragment.SubsetWithIds(
                        move(c_row_group_ids))))
        else:
            raise ValueError(
                "Need to specify one of 'filter' or 'row_group_ids'"
            )

        return Fragment.wrap(c_fragment)


cdef class ParquetReadOptions(_Weakrefable):
    """
    Parquet format specific options for reading.

    Parameters
    ----------
    use_buffered_stream : bool, default False
        Read files through buffered input streams rather than loading entire
        row groups at once. This may be enabled to reduce memory overhead.
        Disabled by default.
    buffer_size : int, default 8192
        Size of buffered stream, if enabled. Default is 8KB.
    dictionary_columns : list of string, default None
        Names of columns which should be dictionary encoded as
        they are read.
    enable_parallel_column_conversion : bool, default False
        EXPERIMENTAL: Parallelize conversion across columns. This option is
        ignored if a scan is already parallelized across input files to avoid
        thread contention. This option will be removed after support is added
        for simultaneous parallelization across files and columns.
    """

    cdef public:
        bint use_buffered_stream
        uint32_t buffer_size
        set dictionary_columns
        bint enable_parallel_column_conversion

    def __init__(self, bint use_buffered_stream=False,
                 buffer_size=8192,
                 dictionary_columns=None,
                 bint enable_parallel_column_conversion=False):
        self.use_buffered_stream = use_buffered_stream
        if buffer_size <= 0:
            raise ValueError("Buffer size must be larger than zero")
        self.buffer_size = buffer_size
        self.dictionary_columns = set(dictionary_columns or set())
        self.enable_parallel_column_conversion = \
            enable_parallel_column_conversion

    def equals(self, ParquetReadOptions other):
        return (
            self.use_buffered_stream == other.use_buffered_stream and
            self.buffer_size == other.buffer_size and
            self.dictionary_columns == other.dictionary_columns and
            self.enable_parallel_column_conversion ==
            other.enable_parallel_column_conversion
        )

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return False


cdef class ParquetFileWriteOptions(FileWriteOptions):

    cdef:
        CParquetFileWriteOptions* parquet_options
        object _properties

    def update(self, **kwargs):
        arrow_fields = {
            "use_deprecated_int96_timestamps",
            "coerce_timestamps",
            "allow_truncated_timestamps",
        }

        setters = set()
        for name, value in kwargs.items():
            if name not in self._properties:
                raise TypeError("unexpected parquet write option: " + name)
            self._properties[name] = value
            if name in arrow_fields:
                setters.add(self._set_arrow_properties)
            else:
                setters.add(self._set_properties)

        for setter in setters:
            setter()

    def _set_properties(self):
        cdef CParquetFileWriteOptions* opts = self.parquet_options

        opts.writer_properties = _create_writer_properties(
            use_dictionary=self._properties["use_dictionary"],
            compression=self._properties["compression"],
            version=self._properties["version"],
            write_statistics=self._properties["write_statistics"],
            data_page_size=self._properties["data_page_size"],
            compression_level=self._properties["compression_level"],
            use_byte_stream_split=(
                self._properties["use_byte_stream_split"]
            ),
            data_page_version=self._properties["data_page_version"],
        )

    def _set_arrow_properties(self):
        cdef CParquetFileWriteOptions* opts = self.parquet_options

        opts.arrow_writer_properties = _create_arrow_writer_properties(
            use_deprecated_int96_timestamps=(
                self._properties["use_deprecated_int96_timestamps"]
            ),
            coerce_timestamps=self._properties["coerce_timestamps"],
            allow_truncated_timestamps=(
                self._properties["allow_truncated_timestamps"]
            ),
            writer_engine_version="V2",
        )

    cdef void init(self, const shared_ptr[CFileWriteOptions]& sp):
        FileWriteOptions.init(self, sp)
        self.parquet_options = <CParquetFileWriteOptions*> sp.get()
        self._properties = dict(
            use_dictionary=True,
            compression="snappy",
            version="1.0",
            write_statistics=None,
            data_page_size=None,
            compression_level=None,
            use_byte_stream_split=False,
            data_page_version="1.0",
            use_deprecated_int96_timestamps=False,
            coerce_timestamps=None,
            allow_truncated_timestamps=False,
        )
        self._set_properties()
        self._set_arrow_properties()


cdef class ParquetFileFormat(FileFormat):

    cdef:
        CParquetFileFormat* parquet_format

    def __init__(self, read_options=None):
        cdef:
            shared_ptr[CParquetFileFormat] wrapped
            CParquetFileFormatReaderOptions* options

        # Read options

        if read_options is None:
            read_options = ParquetReadOptions()
        elif isinstance(read_options, dict):
            read_options = ParquetReadOptions(**read_options)
        elif not isinstance(read_options, ParquetReadOptions):
            raise TypeError('`read_options` must be either a dictionary or an '
                            'instance of ParquetReadOptions')

        wrapped = make_shared[CParquetFileFormat]()
        options = &(wrapped.get().reader_options)
        options.use_buffered_stream = read_options.use_buffered_stream
        options.buffer_size = read_options.buffer_size
        options.enable_parallel_column_conversion = \
            read_options.enable_parallel_column_conversion
        if read_options.dictionary_columns is not None:
            for column in read_options.dictionary_columns:
                options.dict_columns.insert(tobytes(column))

        self.init(<shared_ptr[CFileFormat]> wrapped)

    cdef void init(self, const shared_ptr[CFileFormat]& sp):
        FileFormat.init(self, sp)
        self.parquet_format = <CParquetFileFormat*> sp.get()

    @property
    def read_options(self):
        cdef CParquetFileFormatReaderOptions* options
        options = &self.parquet_format.reader_options
        return ParquetReadOptions(
            use_buffered_stream=options.use_buffered_stream,
            buffer_size=options.buffer_size,
            dictionary_columns={frombytes(col)
                                for col in options.dict_columns},
            enable_parallel_column_conversion=(
                options.enable_parallel_column_conversion
            )
        )

    def make_write_options(self, **kwargs):
        opts = FileFormat.make_write_options(self)
        (<ParquetFileWriteOptions> opts).update(**kwargs)
        return opts

    def equals(self, ParquetFileFormat other):
        return (
            self.read_options.equals(other.read_options)
        )

    def __reduce__(self):
        return ParquetFileFormat, (self.read_options, )

    def make_fragment(self, file, filesystem=None,
                      Expression partition_expression=None, row_groups=None):
        cdef:
            vector[int] c_row_groups

        partition_expression = partition_expression or _true

        if row_groups is None:
            return super().make_fragment(file, filesystem,
                                         partition_expression)

        c_source = _make_file_source(file, filesystem)
        c_row_groups = [<int> row_group for row_group in set(row_groups)]

        c_fragment = <shared_ptr[CFragment]> GetResultValue(
            self.parquet_format.MakeFragment(move(c_source),
                                             partition_expression.unwrap(),
                                             <shared_ptr[CSchema]>nullptr,
                                             move(c_row_groups)))
        return Fragment.wrap(move(c_fragment))


cdef class IpcFileWriteOptions(FileWriteOptions):

    def __init__(self):
        _forbid_instantiation(self.__class__)


cdef class IpcFileFormat(FileFormat):

    def __init__(self):
        self.init(shared_ptr[CFileFormat](new CIpcFileFormat()))

    def equals(self, IpcFileFormat other):
        return True

    @property
    def default_extname(self):
        return "feather"

    def __reduce__(self):
        return IpcFileFormat, tuple()


cdef class CsvFileFormat(FileFormat):
    cdef:
        CCsvFileFormat* csv_format

    def __init__(self, ParseOptions parse_options=None):
        self.init(shared_ptr[CFileFormat](new CCsvFileFormat()))
        if parse_options is not None:
            self.parse_options = parse_options

    cdef void init(self, const shared_ptr[CFileFormat]& sp):
        FileFormat.init(self, sp)
        self.csv_format = <CCsvFileFormat*> sp.get()

    def make_write_options(self):
        raise NotImplemented("writing CSV datasets")

    @property
    def parse_options(self):
        return ParseOptions.wrap(self.csv_format.parse_options)

    @parse_options.setter
    def parse_options(self, ParseOptions parse_options not None):
        self.csv_format.parse_options = parse_options.options

    def equals(self, CsvFileFormat other):
        return self.parse_options.equals(other.parse_options)

    def __reduce__(self):
        return CsvFileFormat, (self.parse_options,)


cdef class Partitioning(_Weakrefable):

    cdef:
        shared_ptr[CPartitioning] wrapped
        CPartitioning* partitioning

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef init(self, const shared_ptr[CPartitioning]& sp):
        self.wrapped = sp
        self.partitioning = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CPartitioning]& sp):
        type_name = frombytes(sp.get().type_name())

        classes = {
            'schema': DirectoryPartitioning,
            'hive': HivePartitioning,
        }

        class_ = classes.get(type_name, None)
        if class_ is None:
            raise TypeError(type_name)

        cdef Partitioning self = class_.__new__(class_)
        self.init(sp)
        return self

    cdef inline shared_ptr[CPartitioning] unwrap(self):
        return self.wrapped

    def parse(self, path):
        cdef CResult[CExpression] result
        result = self.partitioning.Parse(tobytes(path))
        return Expression.wrap(GetResultValue(result))

    @property
    def schema(self):
        """The arrow Schema attached to the partitioning."""
        return pyarrow_wrap_schema(self.partitioning.schema())


cdef class PartitioningFactory(_Weakrefable):

    cdef:
        shared_ptr[CPartitioningFactory] wrapped
        CPartitioningFactory* factory

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef init(self, const shared_ptr[CPartitioningFactory]& sp):
        self.wrapped = sp
        self.factory = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CPartitioningFactory]& sp):
        cdef PartitioningFactory self = PartitioningFactory.__new__(
            PartitioningFactory
        )
        self.init(sp)
        return self

    cdef inline shared_ptr[CPartitioningFactory] unwrap(self):
        return self.wrapped


cdef vector[shared_ptr[CArray]] _partitioning_dictionaries(
        Schema schema, dictionaries) except *:
    cdef:
        vector[shared_ptr[CArray]] c_dictionaries

    dictionaries = dictionaries or {}

    for field in schema:
        dictionary = dictionaries.get(field.name)

        if (isinstance(field.type, pa.DictionaryType) and
                dictionary is not None):
            c_dictionaries.push_back(pyarrow_unwrap_array(dictionary))
        else:
            c_dictionaries.push_back(<shared_ptr[CArray]> nullptr)

    return c_dictionaries


cdef class DirectoryPartitioning(Partitioning):
    """
    A Partitioning based on a specified Schema.

    The DirectoryPartitioning expects one segment in the file path for each
    field in the schema (all fields are required to be present).
    For example given schema<year:int16, month:int8> the path "/2009/11" would
    be parsed to ("year"_ == 2009 and "month"_ == 11).

    Parameters
    ----------
    schema : Schema
        The schema that describes the partitions present in the file path.
    dictionaries : Dict[str, Array]
        If the type of any field of `schema` is a dictionary type, the
        corresponding entry of `dictionaries` must be an array containing
        every value which may be taken by the corresponding column or an
        error will be raised in parsing.

    Returns
    -------
    DirectoryPartitioning

    Examples
    --------
    >>> from pyarrow.dataset import DirectoryPartitioning
    >>> partition = DirectoryPartitioning(
    ...     pa.schema([("year", pa.int16()), ("month", pa.int8())]))
    >>> print(partitioning.parse("/2009/11"))
    ((year == 2009:int16) and (month == 11:int8))
    """

    cdef:
        CDirectoryPartitioning* directory_partitioning

    def __init__(self, Schema schema not None, dictionaries=None):
        cdef:
            shared_ptr[CDirectoryPartitioning] c_partitioning

        c_partitioning = make_shared[CDirectoryPartitioning](
            pyarrow_unwrap_schema(schema),
            _partitioning_dictionaries(schema, dictionaries)
        )
        self.init(<shared_ptr[CPartitioning]> c_partitioning)

    cdef init(self, const shared_ptr[CPartitioning]& sp):
        Partitioning.init(self, sp)
        self.directory_partitioning = <CDirectoryPartitioning*> sp.get()

    @staticmethod
    def discover(field_names, infer_dictionary=False,
                 max_partition_dictionary_size=0):
        """
        Discover a DirectoryPartitioning.

        Parameters
        ----------
        field_names : list of str
            The names to associate with the values from the subdirectory names.
        infer_dictionary : bool, default False
            When inferring a schema for partition fields, yield dictionary
            encoded types instead of plain types. This can be more efficient
            when materializing virtual columns, and Expressions parsed by the
            finished Partitioning will include dictionaries of all unique
            inspected values for each field.
        max_partition_dictionary_size : int, default 0
            Synonymous with infer_dictionary for backwards compatibility with
            1.0: setting this to -1 or None is equivalent to passing
            infer_dictionary=True.

        Returns
        -------
        DirectoryPartitioningFactory
            To be used in the FileSystemFactoryOptions.
        """
        cdef:
            CPartitioningFactoryOptions c_options
            vector[c_string] c_field_names

        if max_partition_dictionary_size in {-1, None}:
            infer_dictionary = True
        elif max_partition_dictionary_size != 0:
            raise NotImplemented("max_partition_dictionary_size must be "
                                 "0, -1, or None")

        if infer_dictionary:
            c_options.infer_dictionary = True

        c_field_names = [tobytes(s) for s in field_names]
        return PartitioningFactory.wrap(
            CDirectoryPartitioning.MakeFactory(c_field_names, c_options))


cdef class HivePartitioning(Partitioning):
    """
    A Partitioning for "/$key=$value/" nested directories as found in
    Apache Hive.

    Multi-level, directory based partitioning scheme originating from
    Apache Hive with all data files stored in the leaf directories. Data is
    partitioned by static values of a particular column in the schema.
    Partition keys are represented in the form $key=$value in directory names.
    Field order is ignored, as are missing or unrecognized field names.

    For example, given schema<year:int16, month:int8, day:int8>, a possible
    path would be "/year=2009/month=11/day=15".

    Parameters
    ----------
    schema : Schema
        The schema that describes the partitions present in the file path.
    dictionaries : Dict[str, Array]
        If the type of any field of `schema` is a dictionary type, the
        corresponding entry of `dictionaries` must be an array containing
        every value which may be taken by the corresponding column or an
        error will be raised in parsing.

    Returns
    -------
    HivePartitioning

    Examples
    --------
    >>> from pyarrow.dataset import HivePartitioning
    >>> partitioning = HivePartitioning(
    ...     pa.schema([("year", pa.int16()), ("month", pa.int8())]))
    >>> print(partitioning.parse("/year=2009/month=11"))
    ((year == 2009:int16) and (month == 11:int8))

    """

    cdef:
        CHivePartitioning* hive_partitioning

    def __init__(self, Schema schema not None, dictionaries=None):
        cdef:
            shared_ptr[CHivePartitioning] c_partitioning

        c_partitioning = make_shared[CHivePartitioning](
            pyarrow_unwrap_schema(schema),
            _partitioning_dictionaries(schema, dictionaries)
        )
        self.init(<shared_ptr[CPartitioning]> c_partitioning)

    cdef init(self, const shared_ptr[CPartitioning]& sp):
        Partitioning.init(self, sp)
        self.hive_partitioning = <CHivePartitioning*> sp.get()

    @staticmethod
    def discover(infer_dictionary=False, max_partition_dictionary_size=0):
        """
        Discover a HivePartitioning.

        Parameters
        ----------
        infer_dictionary : bool, default False
            When inferring a schema for partition fields, yield dictionary
            encoded types instead of plain. This can be more efficient when
            materializing virtual columns, and Expressions parsed by the
            finished Partitioning will include dictionaries of all unique
            inspected values for each field.
        max_partition_dictionary_size : int, default 0
            Synonymous with infer_dictionary for backwards compatibility with
            1.0: setting this to -1 or None is equivalent to passing
            infer_dictionary=True.

        Returns
        -------
        PartitioningFactory
            To be used in the FileSystemFactoryOptions.
        """
        cdef:
            CPartitioningFactoryOptions c_options

        if max_partition_dictionary_size in {-1, None}:
            infer_dictionary = True
        elif max_partition_dictionary_size != 0:
            raise NotImplemented("max_partition_dictionary_size must be "
                                 "0, -1, or None")

        if infer_dictionary:
            c_options.infer_dictionary = True

        return PartitioningFactory.wrap(
            CHivePartitioning.MakeFactory(c_options))


cdef class DatasetFactory(_Weakrefable):
    """
    DatasetFactory is used to create a Dataset, inspect the Schema
    of the fragments contained in it, and declare a partitioning.
    """

    cdef:
        shared_ptr[CDatasetFactory] wrapped
        CDatasetFactory* factory

    def __init__(self, list children):
        _forbid_instantiation(self.__class__)

    cdef init(self, const shared_ptr[CDatasetFactory]& sp):
        self.wrapped = sp
        self.factory = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CDatasetFactory]& sp):
        cdef DatasetFactory self = \
            DatasetFactory.__new__(DatasetFactory)
        self.init(sp)
        return self

    cdef inline shared_ptr[CDatasetFactory] unwrap(self) nogil:
        return self.wrapped

    @property
    def root_partition(self):
        return Expression.wrap(self.factory.root_partition())

    @root_partition.setter
    def root_partition(self, Expression expr):
        check_status(self.factory.SetRootPartition(expr.unwrap()))

    def inspect_schemas(self):
        cdef CResult[vector[shared_ptr[CSchema]]] result
        cdef CInspectOptions options
        with nogil:
            result = self.factory.InspectSchemas(options)

        schemas = []
        for s in GetResultValue(result):
            schemas.append(pyarrow_wrap_schema(s))
        return schemas

    def inspect(self):
        """
        Inspect all data fragments and return a common Schema.

        Returns
        -------
        Schema
        """
        cdef:
            CInspectOptions options
            CResult[shared_ptr[CSchema]] result
        with nogil:
            result = self.factory.Inspect(options)
        return pyarrow_wrap_schema(GetResultValue(result))

    def finish(self, Schema schema=None):
        """
        Create a Dataset using the inspected schema or an explicit schema
        (if given).

        Parameters
        ----------
        schema: Schema, default None
            The schema to conform the source to.  If None, the inspected
            schema is used.

        Returns
        -------
        Dataset
        """
        cdef:
            shared_ptr[CSchema] sp_schema
            CResult[shared_ptr[CDataset]] result

        if schema is not None:
            sp_schema = pyarrow_unwrap_schema(schema)
            with nogil:
                result = self.factory.FinishWithSchema(sp_schema)
        else:
            with nogil:
                result = self.factory.Finish()

        return Dataset.wrap(GetResultValue(result))


cdef class FileSystemFactoryOptions(_Weakrefable):
    """
    Influences the discovery of filesystem paths.

    Parameters
    ----------
    partition_base_dir : str, optional
        For the purposes of applying the partitioning, paths will be
        stripped of the partition_base_dir. Files not matching the
        partition_base_dir prefix will be skipped for partitioning discovery.
        The ignored files will still be part of the Dataset, but will not
        have partition information.
    partitioning: Partitioning/PartitioningFactory, optional
       Apply the Partitioning to every discovered Fragment. See Partitioning or
       PartitioningFactory documentation.
    exclude_invalid_files : bool, optional (default True)
        If True, invalid files will be excluded (file format specific check).
        This will incur IO for each files in a serial and single threaded
        fashion. Disabling this feature will skip the IO, but unsupported
        files may be present in the Dataset (resulting in an error at scan
        time).
    selector_ignore_prefixes : list, optional
        When discovering from a Selector (and not from an explicit file list),
        ignore files and directories matching any of these prefixes.
        By default this is ['.', '_'].
    """

    cdef:
        CFileSystemFactoryOptions options

    __slots__ = ()  # avoid mistakingly creating attributes

    def __init__(self, partition_base_dir=None, partitioning=None,
                 exclude_invalid_files=None,
                 list selector_ignore_prefixes=None):
        if isinstance(partitioning, PartitioningFactory):
            self.partitioning_factory = partitioning
        elif isinstance(partitioning, Partitioning):
            self.partitioning = partitioning

        if partition_base_dir is not None:
            self.partition_base_dir = partition_base_dir
        if exclude_invalid_files is not None:
            self.exclude_invalid_files = exclude_invalid_files
        if selector_ignore_prefixes is not None:
            self.selector_ignore_prefixes = selector_ignore_prefixes

    cdef inline CFileSystemFactoryOptions unwrap(self):
        return self.options

    @property
    def partitioning(self):
        """Partitioning to apply to discovered files.

        NOTE: setting this property will overwrite partitioning_factory.
        """
        c_partitioning = self.options.partitioning.partitioning()
        if c_partitioning.get() == nullptr:
            return None
        return Partitioning.wrap(c_partitioning)

    @partitioning.setter
    def partitioning(self, Partitioning value):
        self.options.partitioning = (<Partitioning> value).unwrap()

    @property
    def partitioning_factory(self):
        """PartitioningFactory to apply to discovered files and
        discover a Partitioning.

        NOTE: setting this property will overwrite partitioning.
        """
        c_factory = self.options.partitioning.factory()
        if c_factory.get() == nullptr:
            return None
        return PartitioningFactory.wrap(c_factory)

    @partitioning_factory.setter
    def partitioning_factory(self, PartitioningFactory value):
        self.options.partitioning = (<PartitioningFactory> value).unwrap()

    @property
    def partition_base_dir(self):
        """
        Base directory to strip paths before applying the partitioning.
        """
        return frombytes(self.options.partition_base_dir)

    @partition_base_dir.setter
    def partition_base_dir(self, value):
        self.options.partition_base_dir = tobytes(value)

    @property
    def exclude_invalid_files(self):
        """Whether to exclude invalid files."""
        return self.options.exclude_invalid_files

    @exclude_invalid_files.setter
    def exclude_invalid_files(self, bint value):
        self.options.exclude_invalid_files = value

    @property
    def selector_ignore_prefixes(self):
        """
        List of prefixes. Files matching one of those prefixes will be
        ignored by the discovery process.
        """
        return [frombytes(p) for p in self.options.selector_ignore_prefixes]

    @selector_ignore_prefixes.setter
    def selector_ignore_prefixes(self, values):
        self.options.selector_ignore_prefixes = [tobytes(v) for v in values]


cdef class FileSystemDatasetFactory(DatasetFactory):
    """
    Create a DatasetFactory from a list of paths with schema inspection.

    Parameters
    ----------
    filesystem : pyarrow.fs.FileSystem
        Filesystem to discover.
    paths_or_selector: pyarrow.fs.Selector or list of path-likes
        Either a Selector object or a list of path-like objects.
    format : FileFormat
        Currently only ParquetFileFormat and IpcFileFormat are supported.
    options : FileSystemFactoryOptions, optional
        Various flags influencing the discovery of filesystem paths.
    """

    cdef:
        CFileSystemDatasetFactory* filesystem_factory

    def __init__(self, FileSystem filesystem not None, paths_or_selector,
                 FileFormat format not None,
                 FileSystemFactoryOptions options=None):
        cdef:
            vector[c_string] paths
            CFileSelector c_selector
            CResult[shared_ptr[CDatasetFactory]] result
            shared_ptr[CFileSystem] c_filesystem
            shared_ptr[CFileFormat] c_format
            CFileSystemFactoryOptions c_options

        options = options or FileSystemFactoryOptions()
        c_options = options.unwrap()
        c_filesystem = filesystem.unwrap()
        c_format = format.unwrap()

        if isinstance(paths_or_selector, FileSelector):
            with nogil:
                c_selector = (<FileSelector> paths_or_selector).selector
                result = CFileSystemDatasetFactory.MakeFromSelector(
                    c_filesystem,
                    c_selector,
                    c_format,
                    c_options
                )
        elif isinstance(paths_or_selector, (list, tuple)):
            paths = [tobytes(s) for s in paths_or_selector]
            with nogil:
                result = CFileSystemDatasetFactory.MakeFromPaths(
                    c_filesystem,
                    paths,
                    c_format,
                    c_options
                )
        else:
            raise TypeError('Must pass either paths or a FileSelector, but '
                            'passed {}'.format(type(paths_or_selector)))

        self.init(GetResultValue(result))

    cdef init(self, shared_ptr[CDatasetFactory]& sp):
        DatasetFactory.init(self, sp)
        self.filesystem_factory = <CFileSystemDatasetFactory*> sp.get()


cdef class UnionDatasetFactory(DatasetFactory):
    """
    Provides a way to inspect/discover a Dataset's expected schema before
    materialization.

    Parameters
    ----------
    factories : list of DatasetFactory
    """

    cdef:
        CUnionDatasetFactory* union_factory

    def __init__(self, list factories):
        cdef:
            DatasetFactory factory
            vector[shared_ptr[CDatasetFactory]] c_factories
        for factory in factories:
            c_factories.push_back(factory.unwrap())
        self.init(GetResultValue(CUnionDatasetFactory.Make(c_factories)))

    cdef init(self, const shared_ptr[CDatasetFactory]& sp):
        DatasetFactory.init(self, sp)
        self.union_factory = <CUnionDatasetFactory*> sp.get()


cdef class ParquetFactoryOptions(_Weakrefable):
    """
    Influences the discovery of parquet dataset.

    Parameters
    ----------
    partition_base_dir : str, optional
        For the purposes of applying the partitioning, paths will be
        stripped of the partition_base_dir. Files not matching the
        partition_base_dir prefix will be skipped for partitioning discovery.
        The ignored files will still be part of the Dataset, but will not
        have partition information.
    partitioning : Partitioning, PartitioningFactory, optional
        The partitioning scheme applied to fragments, see ``Partitioning``.
    validate_column_chunk_paths : bool, default False
        Assert that all ColumnChunk paths are consistent. The parquet spec
        allows for ColumnChunk data to be stored in multiple files, but
        ParquetDatasetFactory supports only a single file with all ColumnChunk
        data. If this flag is set construction of a ParquetDatasetFactory will
        raise an error if ColumnChunk data is not resident in a single file.
    """

    cdef:
        CParquetFactoryOptions options

    __slots__ = ()  # avoid mistakingly creating attributes

    def __init__(self, partition_base_dir=None, partitioning=None,
                 validate_column_chunk_paths=False):
        if isinstance(partitioning, PartitioningFactory):
            self.partitioning_factory = partitioning
        elif isinstance(partitioning, Partitioning):
            self.partitioning = partitioning

        if partition_base_dir is not None:
            self.partition_base_dir = partition_base_dir

        self.options.validate_column_chunk_paths = validate_column_chunk_paths

    cdef inline CParquetFactoryOptions unwrap(self):
        return self.options

    @property
    def partitioning(self):
        """Partitioning to apply to discovered files.

        NOTE: setting this property will overwrite partitioning_factory.
        """
        c_partitioning = self.options.partitioning.partitioning()
        if c_partitioning.get() == nullptr:
            return None
        return Partitioning.wrap(c_partitioning)

    @partitioning.setter
    def partitioning(self, Partitioning value):
        self.options.partitioning = (<Partitioning> value).unwrap()

    @property
    def partitioning_factory(self):
        """PartitioningFactory to apply to discovered files and
        discover a Partitioning.

        NOTE: setting this property will overwrite partitioning.
        """
        c_factory = self.options.partitioning.factory()
        if c_factory.get() == nullptr:
            return None
        return PartitioningFactory.wrap(c_factory)

    @partitioning_factory.setter
    def partitioning_factory(self, PartitioningFactory value):
        self.options.partitioning = (<PartitioningFactory> value).unwrap()

    @property
    def partition_base_dir(self):
        """
        Base directory to strip paths before applying the partitioning.
        """
        return frombytes(self.options.partition_base_dir)

    @partition_base_dir.setter
    def partition_base_dir(self, value):
        self.options.partition_base_dir = tobytes(value)

    @property
    def validate_column_chunk_paths(self):
        """
        Base directory to strip paths before applying the partitioning.
        """
        return self.options.validate_column_chunk_paths

    @validate_column_chunk_paths.setter
    def validate_column_chunk_paths(self, value):
        self.options.validate_column_chunk_paths = value


cdef class ParquetDatasetFactory(DatasetFactory):
    """
    Create a ParquetDatasetFactory from a Parquet `_metadata` file.

    Parameters
    ----------
    metadata_path : str
        Path to the `_metadata` parquet metadata-only file generated with
        `pyarrow.parquet.write_metadata`.
    filesystem : pyarrow.fs.FileSystem
        Filesystem to read the metadata_path from, and subsequent parquet
        files.
    format : ParquetFileFormat
        Parquet format options.
    options : ParquetFactoryOptions, optional
        Various flags influencing the discovery of filesystem paths.
    """

    cdef:
        CParquetDatasetFactory* parquet_factory

    def __init__(self, metadata_path, FileSystem filesystem not None,
                 FileFormat format not None,
                 ParquetFactoryOptions options=None):
        cdef:
            c_string path
            shared_ptr[CFileSystem] c_filesystem
            shared_ptr[CParquetFileFormat] c_format
            CResult[shared_ptr[CDatasetFactory]] result
            CParquetFactoryOptions c_options

        c_path = tobytes(metadata_path)
        c_filesystem = filesystem.unwrap()
        c_format = static_pointer_cast[CParquetFileFormat, CFileFormat](
            format.unwrap())
        options = options or ParquetFactoryOptions()
        c_options = options.unwrap()

        result = CParquetDatasetFactory.MakeFromMetaDataPath(
            c_path, c_filesystem, c_format, c_options)
        self.init(GetResultValue(result))

    cdef init(self, shared_ptr[CDatasetFactory]& sp):
        DatasetFactory.init(self, sp)
        self.parquet_factory = <CParquetDatasetFactory*> sp.get()


cdef class ScanTask(_Weakrefable):
    """Read record batches from a range of a single data fragment.

    A ScanTask is meant to be a unit of work to be dispatched.
    """

    cdef:
        shared_ptr[CScanTask] wrapped
        CScanTask* task

    def __init__(self):
        _forbid_instantiation(self.__class__, subclasses_instead=False)

    cdef init(self, shared_ptr[CScanTask]& sp):
        self.wrapped = sp
        self.task = self.wrapped.get()

    @staticmethod
    cdef wrap(shared_ptr[CScanTask]& sp):
        cdef ScanTask self = ScanTask.__new__(ScanTask)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScanTask] unwrap(self) nogil:
        return self.wrapped

    def execute(self):
        """Iterate through sequence of materialized record batches.

        Execution semantics are encapsulated in the particular ScanTask
        implementation.

        Returns
        -------
        record_batches : iterator of RecordBatch
        """
        cdef shared_ptr[CRecordBatch] record_batch
        with nogil:
            for maybe_batch in GetResultValue(self.task.Execute()):
                record_batch = GetResultValue(move(maybe_batch))
                with gil:
                    yield pyarrow_wrap_batch(record_batch)


cdef shared_ptr[CScanContext] _build_scan_context(bint use_threads=True,
                                                  MemoryPool memory_pool=None):
    cdef:
        shared_ptr[CScanContext] context

    context = make_shared[CScanContext]()
    context.get().pool = maybe_unbox_memory_pool(memory_pool)
    if use_threads is not None:
        context.get().use_threads = use_threads

    return context


_DEFAULT_BATCH_SIZE = 2**20


cdef void _populate_builder(const shared_ptr[CScannerBuilder]& ptr,
                            list columns=None, Expression filter=None,
                            int batch_size=_DEFAULT_BATCH_SIZE) except *:
    cdef:
        CScannerBuilder *builder
    builder = ptr.get()

    check_status(builder.Filter(_bind(
        filter, pyarrow_wrap_schema(builder.schema()))))

    if columns is not None:
        check_status(builder.Project([tobytes(c) for c in columns]))

    check_status(builder.BatchSize(batch_size))


cdef class Scanner(_Weakrefable):
    """A materialized scan operation with context and options bound.

    A scanner is the class that glues the scan tasks, data fragments and data
    sources together.

    Parameters
    ----------
    dataset : Dataset
        Dataset to scan.
    columns : list of str, default None
        List of columns to project. Order and duplicates will be preserved.
        The columns will be passed down to Datasets and corresponding data
        fragments to avoid loading, copying, and deserializing columns
        that will not be required further down the compute chain.
        By default all of the available columns are projected. Raises
        an exception if any of the referenced column names does not exist
        in the dataset's Schema.
    filter : Expression, default None
        Scan will return only the rows matching the filter.
        If possible the predicate will be pushed down to exploit the
        partition information or internal metadata found in the data
        source, e.g. Parquet statistics. Otherwise filters the loaded
        RecordBatches before yielding them.
    batch_size : int, default 1M
        The maximum row count for scanned record batches. If scanned
        record batches are overflowing memory then this method can be
        called to reduce their size.
    use_threads : bool, default True
        If enabled, then maximum parallelism will be used determined by
        the number of available CPU cores.
    memory_pool : MemoryPool, default None
        For memory allocations, if required. If not specified, uses the
        default pool.
    """

    cdef:
        shared_ptr[CScanner] wrapped
        CScanner* scanner

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CScanner]& sp):
        self.wrapped = sp
        self.scanner = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CScanner]& sp):
        cdef Scanner self = Scanner.__new__(Scanner)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScanner] unwrap(self):
        return self.wrapped

    @staticmethod
    def from_dataset(Dataset dataset not None,
                     bint use_threads=True, MemoryPool memory_pool=None,
                     list columns=None, Expression filter=None,
                     int batch_size=_DEFAULT_BATCH_SIZE):
        cdef:
            shared_ptr[CScanContext] context
            shared_ptr[CScannerBuilder] builder
            shared_ptr[CScanner] scanner

        context = _build_scan_context(use_threads=use_threads,
                                      memory_pool=memory_pool)
        builder = make_shared[CScannerBuilder](dataset.unwrap(), context)
        _populate_builder(builder, columns=columns, filter=filter,
                          batch_size=batch_size)

        scanner = GetResultValue(builder.get().Finish())
        return Scanner.wrap(scanner)

    @staticmethod
    def from_fragment(Fragment fragment not None, Schema schema=None,
                      bint use_threads=True, MemoryPool memory_pool=None,
                      list columns=None, Expression filter=None,
                      int batch_size=_DEFAULT_BATCH_SIZE):
        cdef:
            shared_ptr[CScanContext] context
            shared_ptr[CScannerBuilder] builder
            shared_ptr[CScanner] scanner

        context = _build_scan_context(use_threads=use_threads,
                                      memory_pool=memory_pool)

        schema = schema or fragment.physical_schema

        builder = make_shared[CScannerBuilder](pyarrow_unwrap_schema(schema),
                                               fragment.unwrap(), context)
        _populate_builder(builder, columns=columns, filter=filter,
                          batch_size=batch_size)

        scanner = GetResultValue(builder.get().Finish())
        return Scanner.wrap(scanner)

    def scan(self):
        """Returns a stream of ScanTasks

        The caller is responsible to dispatch/schedule said tasks. Tasks should
        be safe to run in a concurrent fashion and outlive the iterator.

        Returns
        -------
        scan_tasks : iterator of ScanTask
        """
        for maybe_task in GetResultValue(self.scanner.Scan()):
            yield ScanTask.wrap(GetResultValue(move(maybe_task)))

    def to_batches(self):
        """Consume a Scanner in record batches.

        Sequentially executes the ScanTasks as the returned generator gets
        consumed.

        Returns
        -------
        record_batches : iterator of RecordBatch
        """
        for task in self.scan():
            for batch in task.execute():
                yield batch

    def to_table(self):
        """Convert a Scanner into a Table.

        Use this convenience utility with care. This will serially materialize
        the Scan result in memory before creating the Table.

        Returns
        -------
        table : Table
        """
        cdef CResult[shared_ptr[CTable]] result

        with nogil:
            result = self.scanner.ToTable()

        return pyarrow_wrap_table(GetResultValue(result))

    def get_fragments(self):
        """Returns an iterator over the fragments in this scan.
        """
        cdef CFragmentIterator c_fragments = move(GetResultValue(
            self.scanner.GetFragments()))
        for maybe_fragment in c_fragments:
            yield Fragment.wrap(GetResultValue(move(maybe_fragment)))


def _get_partition_keys(Expression partition_expression):
    """
    Extract partition keys (equality constraints between a field and a scalar)
    from an expression as a dict mapping the field's name to its value.

    NB: All expressions yielded by a HivePartitioning or DirectoryPartitioning
    will be conjunctions of equality conditions and are accessible through this
    function. Other subexpressions will be ignored.

    For example, an expression of
    <pyarrow.dataset.Expression ((part == A:string) and (year == 2016:int32))>
    is converted to {'part': 'a', 'year': 2016}
    """
    cdef:
        CExpression expr = partition_expression.unwrap()
        pair[CFieldRef, CDatum] ref_val

    out = {}
    for ref_val in GetResultValue(CExtractKnownFieldValues(expr)):
        assert ref_val.first.name() != nullptr
        assert ref_val.second.kind() == DatumType_SCALAR
        val = pyarrow_wrap_scalar(ref_val.second.scalar())
        out[frombytes(deref(ref_val.first.name()))] = val.as_py()
    return out


def _filesystemdataset_write(
    data not None, object base_dir not None, str basename_template not None,
    Schema schema not None, FileSystem filesystem not None,
    Partitioning partitioning not None,
    FileWriteOptions file_options not None, bint use_threads,
    int max_partitions,
):
    """
    CFileSystemDataset.Write wrapper
    """
    cdef:
        CFileSystemDatasetWriteOptions c_options
        shared_ptr[CScanner] c_scanner
        vector[shared_ptr[CRecordBatch]] c_batches

    c_options.file_write_options = file_options.unwrap()
    c_options.filesystem = filesystem.unwrap()
    c_options.base_dir = tobytes(_stringify_path(base_dir))
    c_options.partitioning = partitioning.unwrap()
    c_options.max_partitions = max_partitions
    c_options.basename_template = tobytes(basename_template)

    if isinstance(data, Dataset):
        scanner = data._scanner(use_threads=use_threads)
    else:
        # data is list of batches/tables
        for table in data:
            if isinstance(table, Table):
                for batch in table.to_batches():
                    c_batches.push_back((<RecordBatch> batch).sp_batch)
            else:
                c_batches.push_back((<RecordBatch> table).sp_batch)

        data = Fragment.wrap(shared_ptr[CFragment](
            new CInMemoryFragment(move(c_batches), _true.unwrap())))

        scanner = Scanner.from_fragment(data, schema, use_threads=use_threads)

    c_scanner = (<Scanner> scanner).unwrap()
    with nogil:
        check_status(CFileSystemDataset.Write(c_options, c_scanner))
