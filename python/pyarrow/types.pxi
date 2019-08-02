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

import atexit
import re
import warnings

from pyarrow import compat
from pyarrow.compat import builtin_pickle


# These are imprecise because the type (in pandas 0.x) depends on the presence
# of nulls
cdef dict _pandas_type_map = {
    _Type_NA: np.float64,  # NaNs
    _Type_BOOL: np.bool_,
    _Type_INT8: np.int8,
    _Type_INT16: np.int16,
    _Type_INT32: np.int32,
    _Type_INT64: np.int64,
    _Type_UINT8: np.uint8,
    _Type_UINT16: np.uint16,
    _Type_UINT32: np.uint32,
    _Type_UINT64: np.uint64,
    _Type_HALF_FLOAT: np.float16,
    _Type_FLOAT: np.float32,
    _Type_DOUBLE: np.float64,
    _Type_DATE32: np.dtype('datetime64[ns]'),
    _Type_DATE64: np.dtype('datetime64[ns]'),
    _Type_TIMESTAMP: np.dtype('datetime64[ns]'),
    _Type_BINARY: np.object_,
    _Type_FIXED_SIZE_BINARY: np.object_,
    _Type_STRING: np.object_,
    _Type_LIST: np.object_,
    _Type_DECIMAL: np.object_,
}

cdef dict _pep3118_type_map = {
    _Type_INT8: b'b',
    _Type_INT16: b'h',
    _Type_INT32: b'i',
    _Type_INT64: b'q',
    _Type_UINT8: b'B',
    _Type_UINT16: b'H',
    _Type_UINT32: b'I',
    _Type_UINT64: b'Q',
    _Type_HALF_FLOAT: b'e',
    _Type_FLOAT: b'f',
    _Type_DOUBLE: b'd',
}


cdef bytes _datatype_to_pep3118(CDataType* type):
    """
    Construct a PEP 3118 format string describing the given datatype.
    None is returned for unsupported types.
    """
    try:
        char = _pep3118_type_map[type.id()]
    except KeyError:
        return None
    else:
        if char in b'bBhHiIqQ':
            # Use "standard" int widths, not native
            return b'=' + char
        else:
            return char


def _is_primitive(Type type):
    # This is simply a redirect, the official API is in pyarrow.types.
    return is_primitive(type)


# Workaround for Cython parsing bug
# https://github.com/cython/cython/issues/2143
ctypedef CFixedWidthType* _CFixedWidthTypePtr


cdef class DataType:
    """
    Base class of all Arrow data types.

    Each data type is an *instance* of this class.
    """
    def __cinit__(self):
        pass

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly, use public "
                        "functions like pyarrow.int64, pyarrow.list_, etc. "
                        "instead.".format(self.__class__.__name__))

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        assert type != nullptr
        self.sp_type = type
        self.type = type.get()
        self.pep3118_format = _datatype_to_pep3118(self.type)

    cdef Field child(self, int i):
        cdef int index = <int> _normalize_index(i, self.type.num_children())
        return pyarrow_wrap_field(self.type.child(index))

    @property
    def id(self):
        return self.type.id()

    @property
    def bit_width(self):
        cdef _CFixedWidthTypePtr ty
        ty = dynamic_cast[_CFixedWidthTypePtr](self.type)
        if ty == nullptr:
            raise ValueError("Non-fixed width type")
        return ty.bit_width()

    @property
    def num_children(self):
        """
        The number of child fields.
        """
        return self.type.num_children()

    @property
    def num_buffers(self):
        """
        Number of data buffers required to construct Array type
        excluding children
        """
        return self.type.layout().bit_widths.size()

    def __str__(self):
        return frombytes(self.type.ToString())

    def __hash__(self):
        return hash(str(self))

    def __reduce__(self):
        return type_for_alias, (str(self),)

    def __repr__(self):
        return '{0.__class__.__name__}({0})'.format(self)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except (TypeError, ValueError):
            return NotImplemented

    def equals(self, other):
        """
        Return true if type is equivalent to passed value

        Parameters
        ----------
        other : DataType or string convertible to DataType

        Returns
        -------
        is_equal : boolean
        """
        cdef DataType other_type

        if not isinstance(other, DataType):
            if not isinstance(other, six.string_types):
                raise TypeError(other)
            other_type = type_for_alias(other)
        else:
            other_type = other

        return self.type.Equals(deref(other_type.type))

    def to_pandas_dtype(self):
        """
        Return the equivalent NumPy / Pandas dtype.
        """
        cdef Type type_id = self.type.id()
        if type_id in _pandas_type_map:
            return _pandas_type_map[type_id]
        else:
            raise NotImplementedError(str(self))


cdef class DictionaryMemo:
    """
    Tracking container for dictionary-encoded fields
    """
    def __cinit__(self):
        self.sp_memo.reset(new CDictionaryMemo())
        self.memo = self.sp_memo.get()


cdef class DictionaryType(DataType):
    """
    Concrete class for dictionary data types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.dict_type = <const CDictionaryType*> type.get()

    def __reduce__(self):
        return dictionary, (self.index_type, self.value_type, self.ordered)

    @property
    def ordered(self):
        """
        Whether the dictionary is ordered, i.e. whether the ordering of values
        in the dictionary is important.
        """
        return self.dict_type.ordered()

    @property
    def index_type(self):
        """
        The data type of dictionary indices (a signed integer type).
        """
        return pyarrow_wrap_data_type(self.dict_type.index_type())

    @property
    def value_type(self):
        """
        The dictionary value type. The dictionary values are found in an
        instance of DictionaryArray
        """
        return pyarrow_wrap_data_type(self.dict_type.value_type())


cdef class ListType(DataType):
    """
    Concrete class for list data types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.list_type = <const CListType*> type.get()

    def __reduce__(self):
        return list_, (self.value_type,)

    @property
    def value_type(self):
        """
        The data type of list values.
        """
        return pyarrow_wrap_data_type(self.list_type.value_type())


cdef class StructType(DataType):
    """
    Concrete class for struct data types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.struct_type = <const CStructType*> type.get()

    cdef Field field(self, int i):
        """
        Return a child field by its index.
        """
        return self.child(i)

    cdef Field field_by_name(self, name):
        """
        Return a child field by its name rather than its index.
        """
        cdef vector[shared_ptr[CField]] fields

        fields = self.struct_type.GetAllFieldsByName(tobytes(name))
        if fields.size() == 0:
            raise KeyError(name)
        elif fields.size() > 1:
            warnings.warn("Struct field name corresponds to more "
                          "than one field", UserWarning)
            raise KeyError(name)
        else:
            return pyarrow_wrap_field(fields[0])

    def __len__(self):
        """
        Like num_children().
        """
        return self.type.num_children()

    def __iter__(self):
        """
        Iterate over struct fields, in order.
        """
        for i in range(len(self)):
            yield self[i]

    def __getitem__(self, i):
        """
        Return the struct field with the given index or name.
        """
        if isinstance(i, six.string_types):
            return self.field_by_name(i)
        elif isinstance(i, six.integer_types):
            return self.child(i)
        else:
            raise TypeError('Expected integer or string index')

    def __reduce__(self):
        return struct, (list(self),)


cdef class UnionType(DataType):
    """
    Concrete class for struct data types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)

    @property
    def mode(self):
        """
        The mode of the union ("dense" or "sparse").
        """
        cdef CUnionType* type = <CUnionType*> self.sp_type.get()
        cdef int mode = type.mode()
        if mode == _UnionMode_DENSE:
            return 'dense'
        if mode == _UnionMode_SPARSE:
            return 'sparse'
        assert 0

    @property
    def type_codes(self):
        """
        The type code to indicate each data type in this union.
        """
        cdef CUnionType* type = <CUnionType*> self.sp_type.get()
        return type.type_codes()

    def __len__(self):
        """
        Like num_children()
        """
        return self.type.num_children()

    def __iter__(self):
        """
        Iterate over union members, in order.
        """
        for i in range(len(self)):
            yield self[i]

    def __getitem__(self, i):
        """
        Return a child member by its index.
        """
        return self.child(i)

    def __reduce__(self):
        return union, (list(self), self.mode)


cdef class TimestampType(DataType):
    """
    Concrete class for timestamp data types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.ts_type = <const CTimestampType*> type.get()

    @property
    def unit(self):
        """
        The timestamp unit ('s', 'ms', 'us' or 'ns').
        """
        return timeunit_to_string(self.ts_type.unit())

    @property
    def tz(self):
        """
        The timestamp time zone, if any, or None.
        """
        if self.ts_type.timezone().size() > 0:
            return frombytes(self.ts_type.timezone())
        else:
            return None

    def to_pandas_dtype(self):
        """
        Return the equivalent NumPy / Pandas dtype.
        """
        if self.tz is None:
            return _pandas_type_map[_Type_TIMESTAMP]
        else:
            # Return DatetimeTZ
            from pyarrow.pandas_compat import make_datetimetz
            return make_datetimetz(self.tz)

    def __reduce__(self):
        return timestamp, (self.unit, self.tz)


cdef class Time32Type(DataType):
    """
    Concrete class for time32 data types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.time_type = <const CTime32Type*> type.get()

    @property
    def unit(self):
        """
        The time unit ('s', 'ms', 'us' or 'ns').
        """
        return timeunit_to_string(self.time_type.unit())


cdef class Time64Type(DataType):
    """
    Concrete class for time64 data types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.time_type = <const CTime64Type*> type.get()

    @property
    def unit(self):
        """
        The time unit ('s', 'ms', 'us' or 'ns').
        """
        return timeunit_to_string(self.time_type.unit())


cdef class FixedSizeBinaryType(DataType):
    """
    Concrete class for fixed-size binary data types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.fixed_size_binary_type = (
            <const CFixedSizeBinaryType*> type.get())

    def __reduce__(self):
        return binary, (self.byte_width,)

    @property
    def byte_width(self):
        """
        The binary size in bytes.
        """
        return self.fixed_size_binary_type.byte_width()


cdef class Decimal128Type(FixedSizeBinaryType):
    """
    Concrete class for decimal128 data types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        FixedSizeBinaryType.init(self, type)
        self.decimal128_type = <const CDecimal128Type*> type.get()

    def __reduce__(self):
        return decimal128, (self.precision, self.scale)

    @property
    def precision(self):
        """
        The decimal precision, in number of decimal digits (an integer).
        """
        return self.decimal128_type.precision()

    @property
    def scale(self):
        """
        The decimal scale (an integer).
        """
        return self.decimal128_type.scale()


cdef class BaseExtensionType(DataType):
    """
    Concrete base class for extension types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.ext_type = <const CExtensionType*> type.get()

    @property
    def extension_name(self):
        """
        The extension type name.
        """
        return frombytes(self.ext_type.extension_name())

    @property
    def storage_type(self):
        """
        The underlying storage type.
        """
        return pyarrow_wrap_data_type(self.ext_type.storage_type())


cdef class ExtensionType(BaseExtensionType):
    """
    Concrete base class for Python-defined extension types.
    """

    def __cinit__(self):
        if type(self) is ExtensionType:
            raise TypeError("Can only instantiate subclasses of "
                            "ExtensionType")

    def __init__(self, DataType storage_type):
        cdef:
            shared_ptr[CExtensionType] cpy_ext_type

        assert storage_type is not None
        check_status(CPyExtensionType.FromClass(storage_type.sp_type,
                                                type(self), &cpy_ext_type))
        self.init(<shared_ptr[CDataType]> cpy_ext_type)

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        BaseExtensionType.init(self, type)
        self.cpy_ext_type = <const CPyExtensionType*> type.get()
        # Store weakref and serialized version of self on C++ type instance
        check_status(self.cpy_ext_type.SetInstance(self))

    def __eq__(self, other):
        # Default implementation to avoid infinite recursion through
        # DataType.__eq__ -> ExtensionType::ExtensionEquals -> DataType.__eq__
        if isinstance(other, ExtensionType):
            return (type(self) == type(other) and
                    self.storage_type == other.storage_type)
        else:
            return NotImplemented

    def __reduce__(self):
        raise NotImplementedError("Please implement {0}.__reduce__"
                                  .format(type(self).__name__))

    def __arrow_ext_serialize__(self):
        return builtin_pickle.dumps(self)

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        try:
            ty = builtin_pickle.loads(serialized)
        except Exception:
            # For some reason, it's impossible to deserialize the
            # ExtensionType instance.  Perhaps the serialized data is
            # corrupt, or more likely the type is being deserialized
            # in an environment where the original Python class or module
            # is not available.  Fall back on a generic BaseExtensionType.
            return UnknownExtensionType(storage_type, serialized)

        if ty.storage_type != storage_type:
            raise TypeError("Expected storage type {0} but got {1}"
                            .format(ty.storage_type, storage_type))
        return ty


cdef class UnknownExtensionType(ExtensionType):
    """
    A concrete class for Python-defined extension types that refer to
    an unknown Python implementation.
    """

    cdef:
        bytes serialized

    def __init__(self, DataType storage_type, serialized):
        self.serialized = serialized
        ExtensionType.__init__(self, storage_type)

    def __arrow_ext_serialize__(self):
        return self.serialized


cdef class Field:
    """
    A named field, with a data type, nullability, and optional metadata.

    Notes
    -----
    Do not use this class's constructor directly; use pyarrow.field
    """
    def __cinit__(self):
        pass

    def __init__(self):
        raise TypeError("Do not call Field's constructor directly, use "
                        "`pyarrow.field` instead.")

    cdef void init(self, const shared_ptr[CField]& field):
        self.sp_field = field
        self.field = field.get()
        self.type = pyarrow_wrap_data_type(field.get().type())

    def equals(self, Field other):
        """
        Test if this field is equal to the other

        Parameters
        ----------
        other : pyarrow.Field

        Returns
        -------
        is_equal : boolean
        """
        return self.field.Equals(deref(other.field))

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def __reduce__(self):
        return field, (self.name, self.type, self.nullable, self.metadata)

    def __str__(self):
        return 'pyarrow.Field<{0}>'.format(frombytes(self.field.ToString()))

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.field.name(), self.type, self.field.nullable()))

    @property
    def nullable(self):
        return self.field.nullable()

    @property
    def name(self):
        return frombytes(self.field.name())

    @property
    def metadata(self):
        return pyarrow_wrap_metadata(self.field.metadata())

    def add_metadata(self, metadata):
        """
        Add metadata as dict of string keys and values to Field

        Parameters
        ----------
        metadata : dict
            Keys and values must be string-like / coercible to bytes

        Returns
        -------
        field : pyarrow.Field
        """
        cdef:
            shared_ptr[CField] c_field
            shared_ptr[CKeyValueMetadata] c_meta

        if not isinstance(metadata, dict):
            raise TypeError('Metadata must be an instance of dict')

        c_meta = pyarrow_unwrap_metadata(metadata)
        with nogil:
            c_field = self.field.AddMetadata(c_meta)

        return pyarrow_wrap_field(c_field)

    def remove_metadata(self):
        """
        Create new field without metadata, if any

        Returns
        -------
        field : pyarrow.Field
        """
        cdef shared_ptr[CField] new_field
        with nogil:
            new_field = self.field.RemoveMetadata()
        return pyarrow_wrap_field(new_field)

    def flatten(self):
        """
        Flatten this field.  If a struct field, individual child fields
        will be returned with their names prefixed by the parent's name.

        Returns
        -------
        fields : List[pyarrow.Field]
        """
        cdef vector[shared_ptr[CField]] flattened
        with nogil:
            flattened = self.field.Flatten()
        return [pyarrow_wrap_field(f) for f in flattened]


cdef class Schema:

    def __cinit__(self):
        pass

    def __init__(self):
        raise TypeError("Do not call Schema's constructor directly, use "
                        "`pyarrow.schema` instead.")

    def __len__(self):
        return self.schema.num_fields()

    def __getitem__(self, key):
        cdef int index = <int> _normalize_index(key, self.schema.num_fields())
        return pyarrow_wrap_field(self.schema.field(index))

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    cdef void init(self, const vector[shared_ptr[CField]]& fields):
        self.schema = new CSchema(fields)
        self.sp_schema.reset(self.schema)

    cdef void init_schema(self, const shared_ptr[CSchema]& schema):
        self.schema = schema.get()
        self.sp_schema = schema

    def __reduce__(self):
        return schema, (list(self), self.metadata)

    def __hash__(self):
        return hash((tuple(self), self.metadata))

    @property
    def pandas_metadata(self):
        """
        Return deserialized-from-JSON pandas metadata field (if it exists)
        """
        metadata = self.metadata
        key = b'pandas'
        if metadata is None or key not in metadata:
            return None

        import json
        return json.loads(metadata[key].decode('utf8'))

    @property
    def names(self):
        """
        The schema's field names.

        Returns
        -------
        list of str
        """
        cdef int i
        result = []
        for i in range(self.schema.num_fields()):
            name = frombytes(self.schema.field(i).get().name())
            result.append(name)
        return result

    @property
    def types(self):
        """
        The schema's field types.

        Returns
        -------
        list of DataType
        """
        return [field.type for field in self]

    @property
    def metadata(self):
        return pyarrow_wrap_metadata(self.schema.metadata())

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def empty_table(self):
        """
        Provide an empty table according to the schema.

        Returns
        -------
        table: pyarrow.Table
        """
        arrays = []
        names = []
        for field in self:
            arrays.append(array([], type=field.type))
            names.append(field.name)
        return Table.from_arrays(
            arrays=arrays,
            names=names,
            metadata=self.metadata
        )

    def equals(self, Schema other not None, bint check_metadata=True):
        """
        Test if this schema is equal to the other

        Parameters
        ----------
        other :  pyarrow.Schema
        check_metadata : bool, default False
            Key/value metadata must be equal too

        Returns
        -------
        is_equal : boolean
        """
        return self.sp_schema.get().Equals(deref(other.schema),
                                           check_metadata)

    @classmethod
    def from_pandas(cls, df, preserve_index=None):
        """
        Returns implied schema from dataframe

        Parameters
        ----------
        df : pandas.DataFrame
        preserve_index : bool, default True
            Whether to store the index as an additional column (or columns, for
            MultiIndex) in the resulting `Table`.
            The default of None will store the index as a column, except for
            RangeIndex which is stored as metadata only. Use
            ``preserve_index=True`` to force it to be stored as a column.

        Returns
        -------
        pyarrow.Schema

        Examples
        --------

        >>> import pandas as pd
        >>> import pyarrow as pa
        >>> df = pd.DataFrame({
            ...     'int': [1, 2],
            ...     'str': ['a', 'b']
            ... })
        >>> pa.Schema.from_pandas(df)
        int: int64
        str: string
        __index_level_0__: int64
        """
        from pyarrow.pandas_compat import dataframe_to_types
        names, types, metadata = dataframe_to_types(
            df,
            preserve_index=preserve_index
        )
        fields = []
        for name, type_ in zip(names, types):
            fields.append(field(name, type_))
        return schema(fields, metadata)

    def field_by_name(self, name):
        """
        Access a field by its name rather than the column index.

        Parameters
        ----------
        name: str

        Returns
        -------
        field: pyarrow.Field
        """
        cdef:
            vector[shared_ptr[CField]] results

        results = self.schema.GetAllFieldsByName(tobytes(name))
        if results.size() == 0:
            return None
        elif results.size() > 1:
            warnings.warn("Schema field name corresponds to more "
                          "than one field", UserWarning)
            return None
        else:
            return pyarrow_wrap_field(results[0])

    def get_field_index(self, name):
        return self.schema.GetFieldIndex(tobytes(name))

    def append(self, Field field):
        """
        Append a field at the end of the schema.

        In contrast to Python's ``list.append()`` it does return a new
        object, leaving the original Schema unmodified.

        Parameters
        ----------
        field: Field

        Returns
        -------
        schema: Schema
            New object with appended field.
        """
        return self.insert(self.schema.num_fields(), field)

    def insert(self, int i, Field field):
        """
        Add a field at position i to the schema.

        Parameters
        ----------
        i: int
        field: Field

        Returns
        -------
        schema: Schema
        """
        cdef:
            shared_ptr[CSchema] new_schema
            shared_ptr[CField] c_field

        c_field = field.sp_field

        with nogil:
            check_status(self.schema.AddField(i, c_field, &new_schema))

        return pyarrow_wrap_schema(new_schema)

    def remove(self, int i):
        """
        Remove the field at index i from the schema.

        Parameters
        ----------
        i: int

        Returns
        -------
        schema: Schema
        """
        cdef shared_ptr[CSchema] new_schema

        with nogil:
            check_status(self.schema.RemoveField(i, &new_schema))

        return pyarrow_wrap_schema(new_schema)

    def set(self, int i, Field field):
        """
        Replace a field at position i in the schema.

        Parameters
        ----------
        i: int
        field: Field

        Returns
        -------
        schema: Schema
        """
        cdef:
            shared_ptr[CSchema] new_schema
            shared_ptr[CField] c_field

        c_field = field.sp_field

        with nogil:
            check_status(self.schema.SetField(i, c_field, &new_schema))

        return pyarrow_wrap_schema(new_schema)

    def add_metadata(self, metadata):
        """
        Add metadata as dict of string keys and values to Schema

        Parameters
        ----------
        metadata : dict
            Keys and values must be string-like / coercible to bytes

        Returns
        -------
        schema : pyarrow.Schema
        """
        cdef:
            shared_ptr[CKeyValueMetadata] c_meta
            shared_ptr[CSchema] c_schema

        if not isinstance(metadata, dict):
            raise TypeError('Metadata must be an instance of dict')

        c_meta = pyarrow_unwrap_metadata(metadata)
        with nogil:
            c_schema = self.schema.AddMetadata(c_meta)

        return pyarrow_wrap_schema(c_schema)

    def serialize(self, DictionaryMemo dictionary_memo=None, memory_pool=None):
        """
        Write Schema to Buffer as encapsulated IPC message

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            Uses default memory pool if not specified
        dictionary_memo : DictionaryMemo, optional
            If schema contains dictionaries, must pass a
            DictionaryMemo to be able to deserialize RecordBatch
            objects

        Returns
        -------
        serialized : Buffer
        """
        cdef:
            shared_ptr[CBuffer] buffer
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
            CDictionaryMemo temp_memo
            CDictionaryMemo* arg_dict_memo

        if dictionary_memo is not None:
            arg_dict_memo = dictionary_memo.memo
        else:
            arg_dict_memo = &temp_memo

        with nogil:
            check_status(SerializeSchema(deref(self.schema), arg_dict_memo,
                                         pool, &buffer))
        return pyarrow_wrap_buffer(buffer)

    def remove_metadata(self):
        """
        Create new schema without metadata, if any

        Returns
        -------
        schema : pyarrow.Schema
        """
        cdef shared_ptr[CSchema] new_schema
        with nogil:
            new_schema = self.schema.RemoveMetadata()
        return pyarrow_wrap_schema(new_schema)

    def __str__(self):
        cdef:
            c_string result

        with nogil:
            check_status(
                PrettyPrint(
                    deref(self.schema),
                    PrettyPrintOptions(0),
                    &result
                )
            )

        printed = frombytes(result)
        if self.metadata is not None:
            import pprint
            metadata_formatted = pprint.pformat(self.metadata)
            printed += '\nmetadata\n--------\n' + metadata_formatted

        return printed

    def __repr__(self):
        return self.__str__()


cdef dict _type_cache = {}


cdef DataType primitive_type(Type type):
    if type in _type_cache:
        return _type_cache[type]

    cdef DataType out = DataType.__new__(DataType)
    out.init(GetPrimitiveType(type))

    _type_cache[type] = out
    return out


# -----------------------------------------------------------
# Type factory functions


def field(name, type, bint nullable=True, metadata=None):
    """
    Create a pyarrow.Field instance

    Parameters
    ----------
    name : string or bytes
    type : pyarrow.DataType
    nullable : boolean, default True
    metadata : dict, default None
        Keys and values must be coercible to bytes

    Returns
    -------
    field : pyarrow.Field
    """
    cdef:
        Field result = Field.__new__(Field)
        DataType _type = ensure_type(type, allow_none=False)
        shared_ptr[CKeyValueMetadata] c_meta

    if metadata is not None:
        if not isinstance(metadata, dict):
            raise TypeError('Metadata must be an instance of dict')
        c_meta = pyarrow_unwrap_metadata(metadata)

    result.sp_field.reset(
        new CField(tobytes(name), _type.sp_type, nullable, c_meta)
    )
    result.field = result.sp_field.get()
    result.type = _type

    return result


cdef set PRIMITIVE_TYPES = set([
    _Type_NA, _Type_BOOL,
    _Type_UINT8, _Type_INT8,
    _Type_UINT16, _Type_INT16,
    _Type_UINT32, _Type_INT32,
    _Type_UINT64, _Type_INT64,
    _Type_TIMESTAMP, _Type_DATE32,
    _Type_TIME32, _Type_TIME64,
    _Type_DATE64,
    _Type_HALF_FLOAT,
    _Type_FLOAT,
    _Type_DOUBLE])


def null():
    """
    Create instance of null type
    """
    return primitive_type(_Type_NA)


def bool_():
    """
    Create instance of boolean type
    """
    return primitive_type(_Type_BOOL)


def uint8():
    """
    Create instance of unsigned int8 type
    """
    return primitive_type(_Type_UINT8)


def int8():
    """
    Create instance of signed int8 type
    """
    return primitive_type(_Type_INT8)


def uint16():
    """
    Create instance of unsigned uint16 type
    """
    return primitive_type(_Type_UINT16)


def int16():
    """
    Create instance of signed int16 type
    """
    return primitive_type(_Type_INT16)


def uint32():
    """
    Create instance of unsigned uint32 type
    """
    return primitive_type(_Type_UINT32)


def int32():
    """
    Create instance of signed int32 type
    """
    return primitive_type(_Type_INT32)


def uint64():
    """
    Create instance of unsigned uint64 type
    """
    return primitive_type(_Type_UINT64)


def int64():
    """
    Create instance of signed int64 type
    """
    return primitive_type(_Type_INT64)


cdef dict _timestamp_type_cache = {}
cdef dict _time_type_cache = {}


cdef timeunit_to_string(TimeUnit unit):
    if unit == TimeUnit_SECOND:
        return 's'
    elif unit == TimeUnit_MILLI:
        return 'ms'
    elif unit == TimeUnit_MICRO:
        return 'us'
    elif unit == TimeUnit_NANO:
        return 'ns'


_FIXED_OFFSET_RE = re.compile(r'([+-])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9])$')


def tzinfo_to_string(tz):
    """
    Converts a time zone object into a string indicating the name of a time
    zone, one of:
    * As used in the Olson time zone database (the "tz database" or
      "tzdata"), such as "America/New_York"
    * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30

    Parameters
    ----------
      tz : datetime.tzinfo
        Time zone object

    Returns
    -------
      name : string
        Time zone name
    """
    import pytz
    import datetime

    def fixed_offset_to_string(offset):
        seconds = int(offset.utcoffset(None).total_seconds())
        sign = '+' if seconds >= 0 else '-'
        minutes, seconds = divmod(abs(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if seconds > 0:
            raise ValueError('Offset must represent whole number of minutes')
        return '{}{:02d}:{:02d}'.format(sign, hours, minutes)

    if tz is pytz.utc:
        return tz.zone  # ARROW-4055
    elif isinstance(tz, pytz.tzinfo.BaseTzInfo):
        return tz.zone
    elif isinstance(tz, pytz._FixedOffset):
        return fixed_offset_to_string(tz)
    elif isinstance(tz, datetime.tzinfo):
        if six.PY3 and isinstance(tz, datetime.timezone):
            return fixed_offset_to_string(tz)
        else:
            raise ValueError('Unable to convert timezone `{}` to string'
                             .format(tz))
    else:
        raise TypeError('Must be an instance of `datetime.tzinfo`')


def string_to_tzinfo(name):
    """
    Converts a string indicating the name of a time zone into a time zone
    object, one of:
    * As used in the Olson time zone database (the "tz database" or
      "tzdata"), such as "America/New_York"
    * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30

    Parameters
    ----------
      name: string
        Time zone name

    Returns
    -------
      tz : datetime.tzinfo
        Time zone object
    """
    import pytz
    m = _FIXED_OFFSET_RE.match(name)
    if m:
        sign = 1 if m.group(1) == '+' else -1
        hours, minutes = map(int, m.group(2, 3))
        return pytz.FixedOffset(sign * (hours * 60 + minutes))
    else:
        return pytz.timezone(name)


def timestamp(unit, tz=None):
    """
    Create instance of timestamp type with resolution and optional time zone

    Parameters
    ----------
    unit : string
        one of 's' [second], 'ms' [millisecond], 'us' [microsecond], or 'ns'
        [nanosecond]
    tz : string, default None
        Time zone name. None indicates time zone naive

    Examples
    --------
    ::

        t1 = pa.timestamp('us')
        t2 = pa.timestamp('s', tz='America/New_York')

    Returns
    -------
    timestamp_type : TimestampType
    """
    cdef:
        TimeUnit unit_code
        c_string c_timezone

    if unit == "s":
        unit_code = TimeUnit_SECOND
    elif unit == 'ms':
        unit_code = TimeUnit_MILLI
    elif unit == 'us':
        unit_code = TimeUnit_MICRO
    elif unit == 'ns':
        unit_code = TimeUnit_NANO
    else:
        raise ValueError('Invalid TimeUnit string')

    cdef TimestampType out = TimestampType.__new__(TimestampType)

    if tz is None:
        out.init(ctimestamp(unit_code))
        if unit_code in _timestamp_type_cache:
            return _timestamp_type_cache[unit_code]
        _timestamp_type_cache[unit_code] = out
    else:
        if not isinstance(tz, six.string_types):
            tz = tzinfo_to_string(tz)

        c_timezone = tobytes(tz)
        out.init(ctimestamp(unit_code, c_timezone))

    return out


def time32(unit):
    """
    Create instance of 32-bit time (time of day) type with unit resolution

    Parameters
    ----------
    unit : string
        one of 's' [second], or 'ms' [millisecond]

    Examples
    --------
    ::

        t1 = pa.time32('s')
        t2 = pa.time32('ms')
    """
    cdef:
        TimeUnit unit_code
        c_string c_timezone

    if unit == 's':
        unit_code = TimeUnit_SECOND
    elif unit == 'ms':
        unit_code = TimeUnit_MILLI
    else:
        raise ValueError('Invalid TimeUnit for time32: {}'.format(unit))

    if unit_code in _time_type_cache:
        return _time_type_cache[unit_code]

    cdef Time32Type out = Time32Type.__new__(Time32Type)

    out.init(ctime32(unit_code))
    _time_type_cache[unit_code] = out

    return out


def time64(unit):
    """
    Create instance of 64-bit time (time of day) type with unit resolution

    Parameters
    ----------
    unit : string
        one of 'us' [microsecond], or 'ns' [nanosecond]

    Examples
    --------
    ::

        t1 = pa.time64('us')
        t2 = pa.time64('ns')
    """
    cdef:
        TimeUnit unit_code
        c_string c_timezone

    if unit == 'us':
        unit_code = TimeUnit_MICRO
    elif unit == 'ns':
        unit_code = TimeUnit_NANO
    else:
        raise ValueError('Invalid TimeUnit for time64: {}'.format(unit))

    if unit_code in _time_type_cache:
        return _time_type_cache[unit_code]

    cdef Time64Type out = Time64Type.__new__(Time64Type)

    out.init(ctime64(unit_code))
    _time_type_cache[unit_code] = out

    return out


def date32():
    """
    Create instance of 32-bit date (days since UNIX epoch 1970-01-01)
    """
    return primitive_type(_Type_DATE32)


def date64():
    """
    Create instance of 64-bit date (milliseconds since UNIX epoch 1970-01-01)
    """
    return primitive_type(_Type_DATE64)


def float16():
    """
    Create half-precision floating point type
    """
    return primitive_type(_Type_HALF_FLOAT)


def float32():
    """
    Create single-precision floating point type
    """
    return primitive_type(_Type_FLOAT)


def float64():
    """
    Create double-precision floating point type
    """
    return primitive_type(_Type_DOUBLE)


cpdef DataType decimal128(int precision, int scale=0):
    """
    Create decimal type with precision and scale and 128bit width

    Parameters
    ----------
    precision : int
    scale : int

    Returns
    -------
    decimal_type : Decimal128Type
    """
    cdef shared_ptr[CDataType] decimal_type
    if precision < 1 or precision > 38:
        raise ValueError("precision should be between 1 and 38")
    decimal_type.reset(new CDecimal128Type(precision, scale))
    return pyarrow_wrap_data_type(decimal_type)


def string():
    """
    Create UTF8 variable-length string type
    """
    return primitive_type(_Type_STRING)


def utf8():
    """
    Alias for string()
    """
    return string()


def binary(int length=-1):
    """
    Create variable-length binary type

    Parameters
    ----------
    length : int, optional, default -1
        If length == -1 then return a variable length binary type. If length is
        greater than or equal to 0 then return a fixed size binary type of
        width `length`.
    """
    if length == -1:
        return primitive_type(_Type_BINARY)

    cdef shared_ptr[CDataType] fixed_size_binary_type
    fixed_size_binary_type.reset(new CFixedSizeBinaryType(length))
    return pyarrow_wrap_data_type(fixed_size_binary_type)


def large_binary():
    """
    Create large variable-length binary type

    This data type may not be supported by all Arrow implementations.  Unless
    you need to represent data larger than 2GB, you should prefer binary().
    """
    return primitive_type(_Type_LARGE_BINARY)


def large_string():
    """
    Create large UTF8 variable-length string type

    This data type may not be supported by all Arrow implementations.  Unless
    you need to represent data larger than 2GB, you should prefer string().
    """
    return primitive_type(_Type_LARGE_STRING)


def large_utf8():
    """
    Alias for large_string()
    """
    return large_string()


cpdef ListType list_(value_type):
    """
    Create ListType instance from child data type or field

    Parameters
    ----------
    value_type : DataType or Field

    Returns
    -------
    list_type : DataType
    """
    cdef:
        DataType data_type
        Field _field
        shared_ptr[CDataType] list_type
        ListType out = ListType.__new__(ListType)

    if isinstance(value_type, DataType):
        _field = field('item', value_type)
    elif isinstance(value_type, Field):
        _field = value_type
    else:
        raise TypeError('List requires DataType or Field')

    list_type.reset(new CListType(_field.sp_field))
    out.init(list_type)
    return out


cpdef DictionaryType dictionary(index_type, value_type, bint ordered=False):
    """
    Dictionary (categorical, or simply encoded) type

    Parameters
    ----------
    index_type : DataType
    value_type : DataType
    ordered : boolean

    Returns
    -------
    type : DictionaryType
    """
    cdef:
        DataType _index_type = ensure_type(index_type, allow_none=False)
        DataType _value_type = ensure_type(value_type, allow_none=False)
        DictionaryType out = DictionaryType.__new__(DictionaryType)
        shared_ptr[CDataType] dict_type

    dict_type.reset(new CDictionaryType(_index_type.sp_type,
                                        _value_type.sp_type, ordered == 1))
    out.init(dict_type)
    return out


def struct(fields):
    """
    Create StructType instance from fields

    Parameters
    ----------
    fields : iterable of Fields or tuples, or mapping of strings to DataTypes

    Examples
    --------
    ::

        import pyarrow as pa
        fields = [
            ('f1', pa.int32()),
            ('f2', pa.string()),
        ]
        struct_type = pa.struct(fields)

        fields = [
            pa.field('f1', pa.int32()),
            pa.field('f2', pa.string(), nullable=false),
        ]
        struct_type = pa.struct(fields)

    Returns
    -------
    type : DataType
    """
    cdef:
        Field py_field
        vector[shared_ptr[CField]] c_fields
        cdef shared_ptr[CDataType] struct_type

    if isinstance(fields, compat.Mapping):
        fields = fields.items()

    for item in fields:
        if isinstance(item, tuple):
            py_field = field(*item)
        else:
            py_field = item
        c_fields.push_back(py_field.sp_field)

    struct_type.reset(new CStructType(c_fields))
    return pyarrow_wrap_data_type(struct_type)


def union(children_fields, mode):
    """
    Create UnionType from children fields.

    Parameters
    ----------
    fields : sequence of Field values
    mode : str
        'dense' or 'sparse'

    Returns
    -------
    type : DataType
    """
    cdef:
        Field child_field
        vector[shared_ptr[CField]] c_fields
        vector[uint8_t] type_codes
        shared_ptr[CDataType] union_type
        int i

    if isinstance(mode, int):
        if mode not in (_UnionMode_SPARSE, _UnionMode_DENSE):
            raise ValueError("Invalid union mode {0!r}".format(mode))
    else:
        if mode == 'sparse':
            mode = _UnionMode_SPARSE
        elif mode == 'dense':
            mode = _UnionMode_DENSE
        else:
            raise ValueError("Invalid union mode {0!r}".format(mode))

    for i, child_field in enumerate(children_fields):
        type_codes.push_back(i)
        c_fields.push_back(child_field.sp_field)

    if mode == UnionMode_SPARSE:
        union_type.reset(new CUnionType(c_fields, type_codes,
                                        _UnionMode_SPARSE))
    else:
        union_type.reset(new CUnionType(c_fields, type_codes,
                                        _UnionMode_DENSE))

    return pyarrow_wrap_data_type(union_type)


cdef dict _type_aliases = {
    'null': null,
    'bool': bool_,
    'boolean': bool_,
    'i1': int8,
    'int8': int8,
    'i2': int16,
    'int16': int16,
    'i4': int32,
    'int32': int32,
    'i8': int64,
    'int64': int64,
    'u1': uint8,
    'uint8': uint8,
    'u2': uint16,
    'uint16': uint16,
    'u4': uint32,
    'uint32': uint32,
    'u8': uint64,
    'uint64': uint64,
    'f2': float16,
    'halffloat': float16,
    'float16': float16,
    'f4': float32,
    'float': float32,
    'float32': float32,
    'f8': float64,
    'double': float64,
    'float64': float64,
    'string': string,
    'str': string,
    'utf8': string,
    'binary': binary,
    'large_string': large_string,
    'large_str': large_string,
    'large_utf8': large_string,
    'large_binary': large_binary,
    'date32': date32,
    'date64': date64,
    'date32[day]': date32,
    'date64[ms]': date64,
    'time32[s]': time32('s'),
    'time32[ms]': time32('ms'),
    'time64[us]': time64('us'),
    'time64[ns]': time64('ns'),
    'timestamp[s]': timestamp('s'),
    'timestamp[ms]': timestamp('ms'),
    'timestamp[us]': timestamp('us'),
    'timestamp[ns]': timestamp('ns'),
}


def type_for_alias(name):
    """
    Return DataType given a string alias if one exists

    Returns
    -------
    type : DataType
    """
    name = name.lower()
    try:
        alias = _type_aliases[name]
    except KeyError:
        raise ValueError('No type alias for {0}'.format(name))

    if isinstance(alias, DataType):
        return alias
    return alias()


cdef DataType ensure_type(object ty, c_bool allow_none=False):
    if allow_none and ty is None:
        return None
    elif isinstance(ty, DataType):
        return ty
    elif isinstance(ty, six.string_types):
        return type_for_alias(ty)
    else:
        raise TypeError('DataType expected, got {!r}'.format(type(ty)))


def schema(fields, metadata=None):
    """
    Construct pyarrow.Schema from collection of fields

    Parameters
    ----------
    field : iterable of Fields or tuples, or mapping of strings to DataTypes
    metadata : dict, default None
        Keys and values must be coercible to bytes

    Examples
    --------
    ::

        import pyarrow as pa
        fields = [
            ('some_int', pa.int32()),
            ('some_string', pa.string()),
        ]
        schema = pa.schema(fields)

    Returns
    -------
    schema : pyarrow.Schema
    """
    cdef:
        shared_ptr[CKeyValueMetadata] c_meta
        shared_ptr[CSchema] c_schema
        Schema result
        Field py_field
        vector[shared_ptr[CField]] c_fields

    if isinstance(fields, compat.Mapping):
        fields = fields.items()

    for item in fields:
        if isinstance(item, tuple):
            py_field = field(*item)
        else:
            py_field = item
        if py_field is None:
            raise TypeError("field or tuple expected, got None")
        c_fields.push_back(py_field.sp_field)

    if metadata is not None:
        if not isinstance(metadata, dict):
            raise TypeError('Metadata must be an instance of dict')
        c_meta = pyarrow_unwrap_metadata(metadata)

    c_schema.reset(new CSchema(c_fields, c_meta))
    result = Schema.__new__(Schema)
    result.init_schema(c_schema)

    return result


def from_numpy_dtype(object dtype):
    """
    Convert NumPy dtype to pyarrow.DataType
    """
    cdef shared_ptr[CDataType] c_type
    dtype = np.dtype(dtype)
    with nogil:
        check_status(NumPyDtypeToArrow(dtype, &c_type))

    return pyarrow_wrap_data_type(c_type)


def is_boolean_value(object obj):
    return IsPyBool(obj)


def is_integer_value(object obj):
    return IsPyInt(obj)


def is_float_value(object obj):
    return IsPyFloat(obj)


def _register_py_extension_type():
    cdef:
        DataType storage_type
        shared_ptr[CExtensionType] cpy_ext_type

    # Make a dummy C++ ExtensionType
    storage_type = null()
    check_status(CPyExtensionType.FromClass(storage_type.sp_type,
                                            ExtensionType, &cpy_ext_type))
    check_status(
        RegisterPyExtensionType(<shared_ptr[CDataType]> cpy_ext_type))


def _unregister_py_extension_type():
    # This needs to be done explicitly before the Python interpreter is
    # finalized.  If the C++ type is destroyed later in the process
    # teardown stage, it will invoke CPython APIs such as Py_DECREF
    # with a destroyed interpreter.
    check_status(UnregisterPyExtensionType())


_register_py_extension_type()
atexit.register(_unregister_py_extension_type)
