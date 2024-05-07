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

from cpython.pycapsule cimport (
    PyCapsule_CheckExact,
    PyCapsule_GetPointer,
    PyCapsule_GetName,
    PyCapsule_New,
    PyCapsule_IsValid
)

import atexit
from collections.abc import Mapping
import pickle
import re
import sys
import warnings
from cython import sizeof

# These are imprecise because the type (in pandas 0.x) depends on the presence
# of nulls
cdef dict _pandas_type_map = {
    _Type_NA: np.object_,  # NaNs
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
    # Pandas does not support [D]ay, so default to [ms] for date32
    _Type_DATE32: np.dtype('datetime64[ms]'),
    _Type_DATE64: np.dtype('datetime64[ms]'),
    _Type_TIMESTAMP: {
        's': np.dtype('datetime64[s]'),
        'ms': np.dtype('datetime64[ms]'),
        'us': np.dtype('datetime64[us]'),
        'ns': np.dtype('datetime64[ns]'),
    },
    _Type_DURATION: {
        's': np.dtype('timedelta64[s]'),
        'ms': np.dtype('timedelta64[ms]'),
        'us': np.dtype('timedelta64[us]'),
        'ns': np.dtype('timedelta64[ns]'),
    },
    _Type_BINARY: np.object_,
    _Type_FIXED_SIZE_BINARY: np.object_,
    _Type_STRING: np.object_,
    _Type_LIST: np.object_,
    _Type_MAP: np.object_,
    _Type_DECIMAL128: np.object_,
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


cdef void* _as_c_pointer(v, allow_null=False) except *:
    """
    Convert a Python object to a raw C pointer.

    Used mainly for the C data interface.
    Integers are accepted as well as capsule objects with a NULL name.
    (the latter for compatibility with raw pointers exported by reticulate)
    """
    cdef void* c_ptr
    cdef const char* capsule_name
    if isinstance(v, int):
        c_ptr = <void*> <uintptr_t > v
    elif isinstance(v, float):
        warnings.warn(
            "Passing a pointer value as a float is unsafe and only "
            "supported for compatibility with older versions of the R "
            "Arrow library", UserWarning, stacklevel=2)
        c_ptr = <void*> <uintptr_t > v
    elif PyCapsule_CheckExact(v):
        # An R external pointer was how the R bindings passed pointer values to
        # Python from versions 7 to 15 (inclusive); however, the reticulate 1.35.0
        # update changed the name of the capsule from NULL to "r_extptr".
        # Newer versions of the R package pass a Python integer; however, this
        # workaround ensures that old versions of the R package continue to work
        # with newer versions of pyarrow.
        capsule_name = PyCapsule_GetName(v)
        if capsule_name == NULL or capsule_name == b"r_extptr":
            c_ptr = PyCapsule_GetPointer(v, capsule_name)
        else:
            capsule_name_str = capsule_name.decode()
            raise ValueError(
                f"Can't convert PyCapsule with name '{capsule_name_str}' to pointer address"
            )
    else:
        raise TypeError(f"Expected a pointer value, got {type(v)!r}")
    if not allow_null and c_ptr == NULL:
        raise ValueError(f"Null pointer (value before cast = {v!r})")
    return c_ptr


def _is_primitive(Type type):
    # This is simply a redirect, the official API is in pyarrow.types.
    return is_primitive(type)


def _get_pandas_type(arrow_type, coerce_to_ns=False):
    cdef Type type_id = arrow_type.id
    if type_id not in _pandas_type_map:
        return None
    if coerce_to_ns:
        # ARROW-3789: Coerce date/timestamp types to datetime64[ns]
        if type_id == _Type_DURATION:
            return np.dtype('timedelta64[ns]')
        return np.dtype('datetime64[ns]')
    pandas_type = _pandas_type_map[type_id]
    if isinstance(pandas_type, dict):
        unit = getattr(arrow_type, 'unit', None)
        pandas_type = pandas_type.get(unit, None)
    return pandas_type


def _get_pandas_tz_type(arrow_type, coerce_to_ns=False):
    from pyarrow.pandas_compat import make_datetimetz
    unit = 'ns' if coerce_to_ns else arrow_type.unit
    return make_datetimetz(unit, arrow_type.tz)


def _to_pandas_dtype(arrow_type, options=None):
    coerce_to_ns = (options and options.get('coerce_temporal_nanoseconds', False)) or (
        _pandas_api.is_v1() and arrow_type.id in
        [_Type_DATE32, _Type_DATE64, _Type_TIMESTAMP, _Type_DURATION])

    if getattr(arrow_type, 'tz', None):
        dtype = _get_pandas_tz_type(arrow_type, coerce_to_ns)
    else:
        dtype = _get_pandas_type(arrow_type, coerce_to_ns)

    if not dtype:
        raise NotImplementedError(str(arrow_type))

    return dtype


# Workaround for Cython parsing bug
# https://github.com/cython/cython/issues/2143
ctypedef CFixedWidthType* _CFixedWidthTypePtr


cdef class DataType(_Weakrefable):
    """
    Base class of all Arrow data types.

    Each data type is an *instance* of this class.

    Examples
    --------
    Instance of int64 type:

    >>> import pyarrow as pa
    >>> pa.int64()
    DataType(int64)
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

    cpdef Field field(self, i):
        """
        Parameters
        ----------
        i : int

        Returns
        -------
        pyarrow.Field
        """
        if not isinstance(i, int):
            raise TypeError(f"Expected int index, got type '{type(i)}'")
        cdef int index = <int> _normalize_index(i, self.type.num_fields())
        return pyarrow_wrap_field(self.type.field(index))

    @property
    def id(self):
        return self.type.id()

    @property
    def bit_width(self):
        """
        Bit width for fixed width type.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.int64()
        DataType(int64)
        >>> pa.int64().bit_width
        64
        """
        cdef _CFixedWidthTypePtr ty
        ty = dynamic_cast[_CFixedWidthTypePtr](self.type)
        if ty == nullptr:
            raise ValueError("Non-fixed width type")
        return ty.bit_width()

    @property
    def byte_width(self):
        """
        Byte width for fixed width type.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.int64()
        DataType(int64)
        >>> pa.int64().byte_width
        8
        """
        cdef _CFixedWidthTypePtr ty
        ty = dynamic_cast[_CFixedWidthTypePtr](self.type)
        if ty == nullptr:
            raise ValueError("Non-fixed width type")
        byte_width = ty.byte_width()
        if byte_width == 0 and self.bit_width != 0:
            raise ValueError("Less than one byte")
        return byte_width

    @property
    def num_fields(self):
        """
        The number of child fields.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.int64()
        DataType(int64)
        >>> pa.int64().num_fields
        0
        >>> pa.list_(pa.string())
        ListType(list<item: string>)
        >>> pa.list_(pa.string()).num_fields
        1
        >>> struct = pa.struct({'x': pa.int32(), 'y': pa.string()})
        >>> struct.num_fields
        2
        """
        return self.type.num_fields()

    @property
    def num_buffers(self):
        """
        Number of data buffers required to construct Array type
        excluding children.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.int64().num_buffers
        2
        >>> pa.string().num_buffers
        3
        """
        return self.type.layout().buffers.size()

    def __str__(self):
        return frombytes(self.type.ToString(), safe=True)

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

    def equals(self, other, *, check_metadata=False):
        """
        Return true if type is equivalent to passed value.

        Parameters
        ----------
        other : DataType or string convertible to DataType
        check_metadata : bool
            Whether nested Field metadata equality should be checked as well.

        Returns
        -------
        is_equal : bool

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.int64().equals(pa.string())
        False
        >>> pa.int64().equals(pa.int64())
        True
        """
        cdef:
            DataType other_type
            c_bool c_check_metadata

        other_type = ensure_type(other)
        c_check_metadata = check_metadata
        return self.type.Equals(deref(other_type.type), c_check_metadata)

    def to_pandas_dtype(self):
        """
        Return the equivalent NumPy / Pandas dtype.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.int64().to_pandas_dtype()
        <class 'numpy.int64'>
        """
        return _to_pandas_dtype(self)

    def _export_to_c(self, out_ptr):
        """
        Export to a C ArrowSchema struct, given its pointer.

        Be careful: if you don't pass the ArrowSchema struct to a consumer,
        its memory will leak.  This is a low-level function intended for
        expert users.
        """
        check_status(ExportType(deref(self.type),
                                <ArrowSchema*> _as_c_pointer(out_ptr)))

    @staticmethod
    def _import_from_c(in_ptr):
        """
        Import DataType from a C ArrowSchema struct, given its pointer.

        This is a low-level function intended for expert users.
        """
        result = GetResultValue(ImportType(<ArrowSchema*>
                                           _as_c_pointer(in_ptr)))
        return pyarrow_wrap_data_type(result)

    def __arrow_c_schema__(self):
        """
        Export to a ArrowSchema PyCapsule

        Unlike _export_to_c, this will not leak memory if the capsule is not used.
        """
        cdef ArrowSchema* c_schema
        capsule = alloc_c_schema(&c_schema)

        with nogil:
            check_status(ExportType(deref(self.type), c_schema))

        return capsule

    @staticmethod
    def _import_from_c_capsule(schema):
        """
        Import a DataType from a ArrowSchema PyCapsule

        Parameters
        ----------
        schema : PyCapsule
            A valid PyCapsule with name 'arrow_schema' containing an
            ArrowSchema pointer.
        """
        cdef:
            ArrowSchema* c_schema
            shared_ptr[CDataType] c_type

        if not PyCapsule_IsValid(schema, 'arrow_schema'):
            raise TypeError(
                "Not an ArrowSchema object"
            )
        c_schema = <ArrowSchema*> PyCapsule_GetPointer(schema, 'arrow_schema')

        with nogil:
            c_type = GetResultValue(ImportType(c_schema))

        return pyarrow_wrap_data_type(c_type)


cdef class DictionaryMemo(_Weakrefable):
    """
    Tracking container for dictionary-encoded fields.
    """

    def __cinit__(self):
        self.sp_memo.reset(new CDictionaryMemo())
        self.memo = self.sp_memo.get()


cdef class DictionaryType(DataType):
    """
    Concrete class for dictionary data types.

    Examples
    --------
    Create an instance of dictionary type:

    >>> import pyarrow as pa
    >>> pa.dictionary(pa.int64(), pa.utf8())
    DictionaryType(dictionary<values=string, indices=int64, ordered=0>)
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

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.dictionary(pa.int64(), pa.utf8()).ordered
        False
        """
        return self.dict_type.ordered()

    @property
    def index_type(self):
        """
        The data type of dictionary indices (a signed integer type).

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.dictionary(pa.int16(), pa.utf8()).index_type
        DataType(int16)
        """
        return pyarrow_wrap_data_type(self.dict_type.index_type())

    @property
    def value_type(self):
        """
        The dictionary value type.

        The dictionary values are found in an instance of DictionaryArray.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.dictionary(pa.int16(), pa.utf8()).value_type
        DataType(string)
        """
        return pyarrow_wrap_data_type(self.dict_type.value_type())


cdef class ListType(DataType):
    """
    Concrete class for list data types.

    Examples
    --------
    Create an instance of ListType:

    >>> import pyarrow as pa
    >>> pa.list_(pa.string())
    ListType(list<item: string>)
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.list_type = <const CListType*> type.get()

    def __reduce__(self):
        return list_, (self.value_field,)

    @property
    def value_field(self):
        """
        The field for list values.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.list_(pa.string()).value_field
        pyarrow.Field<item: string>
        """
        return pyarrow_wrap_field(self.list_type.value_field())

    @property
    def value_type(self):
        """
        The data type of list values.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.list_(pa.string()).value_type
        DataType(string)
        """
        return pyarrow_wrap_data_type(self.list_type.value_type())


cdef class LargeListType(DataType):
    """
    Concrete class for large list data types
    (like ListType, but with 64-bit offsets).

    Examples
    --------
    Create an instance of LargeListType:

    >>> import pyarrow as pa
    >>> pa.large_list(pa.string())
    LargeListType(large_list<item: string>)
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.list_type = <const CLargeListType*> type.get()

    def __reduce__(self):
        return large_list, (self.value_field,)

    @property
    def value_field(self):
        return pyarrow_wrap_field(self.list_type.value_field())

    @property
    def value_type(self):
        """
        The data type of large list values.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.large_list(pa.string()).value_type
        DataType(string)
        """
        return pyarrow_wrap_data_type(self.list_type.value_type())


cdef class ListViewType(DataType):
    """
    Concrete class for list view data types.

    Examples
    --------
    Create an instance of ListViewType:

    >>> import pyarrow as pa
    >>> pa.list_view(pa.string())
    ListViewType(list_view<item: string>)
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.list_view_type = <const CListViewType*> type.get()

    def __reduce__(self):
        return list_view, (self.value_field,)

    @property
    def value_field(self):
        """
        The field for list view values.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.list_view(pa.string()).value_field
        pyarrow.Field<item: string>
        """
        return pyarrow_wrap_field(self.list_view_type.value_field())

    @property
    def value_type(self):
        """
        The data type of list view values.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.list_view(pa.string()).value_type
        DataType(string)
        """
        return pyarrow_wrap_data_type(self.list_view_type.value_type())


cdef class LargeListViewType(DataType):
    """
    Concrete class for large list view data types
    (like ListViewType, but with 64-bit offsets).

    Examples
    --------
    Create an instance of LargeListViewType:

    >>> import pyarrow as pa
    >>> pa.large_list_view(pa.string())
    LargeListViewType(large_list_view<item: string>)
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.list_view_type = <const CLargeListViewType*> type.get()

    def __reduce__(self):
        return large_list_view, (self.value_field,)

    @property
    def value_field(self):
        """
        The field for large list view values.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.large_list_view(pa.string()).value_field
        pyarrow.Field<item: string>
        """
        return pyarrow_wrap_field(self.list_view_type.value_field())

    @property
    def value_type(self):
        """
        The data type of large list view values.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.large_list_view(pa.string()).value_type
        DataType(string)
        """
        return pyarrow_wrap_data_type(self.list_view_type.value_type())


cdef class MapType(DataType):
    """
    Concrete class for map data types.

    Examples
    --------
    Create an instance of MapType:

    >>> import pyarrow as pa
    >>> pa.map_(pa.string(), pa.int32())
    MapType(map<string, int32>)
    >>> pa.map_(pa.string(), pa.int32(), keys_sorted=True)
    MapType(map<string, int32, keys_sorted>)
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.map_type = <const CMapType*> type.get()

    def __reduce__(self):
        return map_, (self.key_field, self.item_field)

    @property
    def key_field(self):
        """
        The field for keys in the map entries.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.map_(pa.string(), pa.int32()).key_field
        pyarrow.Field<key: string not null>
        """
        return pyarrow_wrap_field(self.map_type.key_field())

    @property
    def key_type(self):
        """
        The data type of keys in the map entries.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.map_(pa.string(), pa.int32()).key_type
        DataType(string)
        """
        return pyarrow_wrap_data_type(self.map_type.key_type())

    @property
    def item_field(self):
        """
        The field for items in the map entries.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.map_(pa.string(), pa.int32()).item_field
        pyarrow.Field<value: int32>
        """
        return pyarrow_wrap_field(self.map_type.item_field())

    @property
    def item_type(self):
        """
        The data type of items in the map entries.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.map_(pa.string(), pa.int32()).item_type
        DataType(int32)
        """
        return pyarrow_wrap_data_type(self.map_type.item_type())

    @property
    def keys_sorted(self):
        """
        Should the entries be sorted according to keys.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.map_(pa.string(), pa.int32(), keys_sorted=True).keys_sorted
        True
        """
        return self.map_type.keys_sorted()


cdef class FixedSizeListType(DataType):
    """
    Concrete class for fixed size list data types.

    Examples
    --------
    Create an instance of FixedSizeListType:

    >>> import pyarrow as pa
    >>> pa.list_(pa.int32(), 2)
    FixedSizeListType(fixed_size_list<item: int32>[2])
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.list_type = <const CFixedSizeListType*> type.get()

    def __reduce__(self):
        return list_, (self.value_type, self.list_size)

    @property
    def value_field(self):
        """
        The field for list values.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.list_(pa.int32(), 2).value_field
        pyarrow.Field<item: int32>
        """
        return pyarrow_wrap_field(self.list_type.value_field())

    @property
    def value_type(self):
        """
        The data type of large list values.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.list_(pa.int32(), 2).value_type
        DataType(int32)
        """
        return pyarrow_wrap_data_type(self.list_type.value_type())

    @property
    def list_size(self):
        """
        The size of the fixed size lists.

        Examples
        --------
        >>> import pyarrow as pa
        >>> pa.list_(pa.int32(), 2).list_size
        2
        """
        return self.list_type.list_size()


cdef class StructType(DataType):
    """
    Concrete class for struct data types.

    ``StructType`` supports direct indexing using ``[...]`` (implemented via
    ``__getitem__``) to access its fields.
    It will return the struct field with the given index or name.

    Examples
    --------
    >>> import pyarrow as pa

    Accessing fields using direct indexing:

    >>> struct_type = pa.struct({'x': pa.int32(), 'y': pa.string()})
    >>> struct_type[0]
    pyarrow.Field<x: int32>
    >>> struct_type['y']
    pyarrow.Field<y: string>

    Accessing fields using ``field()``:

    >>> struct_type.field(1)
    pyarrow.Field<y: string>
    >>> struct_type.field('x')
    pyarrow.Field<x: int32>

    # Creating a schema from the struct type's fields:
    >>> pa.schema(list(struct_type))
    x: int32
    y: string
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.struct_type = <const CStructType*> type.get()

    cdef Field field_by_name(self, name):
        """
        Return a child field by its name.

        Parameters
        ----------
        name : str
            The name of the field to look up.

        Returns
        -------
        field : Field
            The child field with the given name.

        Raises
        ------
        KeyError
            If the name isn't found, or if several fields have the given
            name.
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

    def get_field_index(self, name):
        """
        Return index of the unique field with the given name.

        Parameters
        ----------
        name : str
            The name of the field to look up.

        Returns
        -------
        index : int
            The index of the field with the given name; -1 if the
            name isn't found or there are several fields with the given
            name.

        Examples
        --------
        >>> import pyarrow as pa
        >>> struct_type = pa.struct({'x': pa.int32(), 'y': pa.string()})

        Index of the field with a name 'y':

        >>> struct_type.get_field_index('y')
        1

        Index of the field that does not exist:

        >>> struct_type.get_field_index('z')
        -1
        """
        return self.struct_type.GetFieldIndex(tobytes(name))

    cpdef Field field(self, i):
        """
        Select a field by its column name or numeric index.

        Parameters
        ----------
        i : int or str

        Returns
        -------
        pyarrow.Field

        Examples
        --------

        >>> import pyarrow as pa
        >>> struct_type = pa.struct({'x': pa.int32(), 'y': pa.string()})

        Select the second field:

        >>> struct_type.field(1)
        pyarrow.Field<y: string>

        Select the field named 'x':

        >>> struct_type.field('x')
        pyarrow.Field<x: int32>
        """
        if isinstance(i, (bytes, str)):
            return self.field_by_name(i)
        elif isinstance(i, int):
            return DataType.field(self, i)
        else:
            raise TypeError('Expected integer or string index')

    def get_all_field_indices(self, name):
        """
        Return sorted list of indices for the fields with the given name.

        Parameters
        ----------
        name : str
            The name of the field to look up.

        Returns
        -------
        indices : List[int]

        Examples
        --------
        >>> import pyarrow as pa
        >>> struct_type = pa.struct({'x': pa.int32(), 'y': pa.string()})
        >>> struct_type.get_all_field_indices('x')
        [0]
        """
        return self.struct_type.GetAllFieldIndices(tobytes(name))

    def __len__(self):
        """
        Like num_fields().
        """
        return self.type.num_fields()

    def __iter__(self):
        """
        Iterate over struct fields, in order.
        """
        for i in range(len(self)):
            yield self[i]

    def __getitem__(self, i):
        """
        Return the struct field with the given index or name.

        Alias of ``field``.
        """
        return self.field(i)

    def __reduce__(self):
        return struct, (list(self),)


cdef class UnionType(DataType):
    """
    Base class for union data types.

    Examples
    --------
    Create an instance of a dense UnionType using ``pa.union``:

    >>> import pyarrow as pa
    >>> pa.union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())],
    ...          mode=pa.lib.UnionMode_DENSE),
    (DenseUnionType(dense_union<a: fixed_size_binary[10]=0, b: string=1>),)

    Create an instance of a dense UnionType using ``pa.dense_union``:

    >>> pa.dense_union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())])
    DenseUnionType(dense_union<a: fixed_size_binary[10]=0, b: string=1>)

    Create an instance of a sparse UnionType using ``pa.union``:

    >>> pa.union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())],
    ...          mode=pa.lib.UnionMode_SPARSE),
    (SparseUnionType(sparse_union<a: fixed_size_binary[10]=0, b: string=1>),)

    Create an instance of a sparse UnionType using ``pa.sparse_union``:

    >>> pa.sparse_union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())])
    SparseUnionType(sparse_union<a: fixed_size_binary[10]=0, b: string=1>)
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)

    @property
    def mode(self):
        """
        The mode of the union ("dense" or "sparse").

        Examples
        --------
        >>> import pyarrow as pa
        >>> union = pa.sparse_union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())])
        >>> union.mode
        'sparse'
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

        Examples
        --------
        >>> import pyarrow as pa
        >>> union = pa.sparse_union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())])
        >>> union.type_codes
        [0, 1]
        """
        cdef CUnionType* type = <CUnionType*> self.sp_type.get()
        return type.type_codes()

    def __len__(self):
        """
        Like num_fields().
        """
        return self.type.num_fields()

    def __iter__(self):
        """
        Iterate over union members, in order.
        """
        for i in range(len(self)):
            yield self[i]

    cpdef Field field(self, i):
        """
        Return a child field by its numeric index.

        Parameters
        ----------
        i : int

        Returns
        -------
        pyarrow.Field

        Examples
        --------
        >>> import pyarrow as pa
        >>> union = pa.sparse_union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())])
        >>> union[0]
        pyarrow.Field<a: fixed_size_binary[10]>
        """
        if isinstance(i, int):
            return DataType.field(self, i)
        else:
            raise TypeError('Expected integer')

    def __getitem__(self, i):
        """
        Return a child field by its index.

        Alias of ``field``.
        """
        return self.field(i)

    def __reduce__(self):
        return union, (list(self), self.mode, self.type_codes)


cdef class SparseUnionType(UnionType):
    """
    Concrete class for sparse union types.

    Examples
    --------
    Create an instance of a sparse UnionType using ``pa.union``:

    >>> import pyarrow as pa
    >>> pa.union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())],
    ...          mode=pa.lib.UnionMode_SPARSE),
    (SparseUnionType(sparse_union<a: fixed_size_binary[10]=0, b: string=1>),)

    Create an instance of a sparse UnionType using ``pa.sparse_union``:

    >>> pa.sparse_union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())])
    SparseUnionType(sparse_union<a: fixed_size_binary[10]=0, b: string=1>)
    """


cdef class DenseUnionType(UnionType):
    """
    Concrete class for dense union types.

    Examples
    --------
    Create an instance of a dense UnionType using ``pa.union``:

    >>> import pyarrow as pa
    >>> pa.union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())],
    ...          mode=pa.lib.UnionMode_DENSE),
    (DenseUnionType(dense_union<a: fixed_size_binary[10]=0, b: string=1>),)

    Create an instance of a dense UnionType using ``pa.dense_union``:

    >>> pa.dense_union([pa.field('a', pa.binary(10)), pa.field('b', pa.string())])
    DenseUnionType(dense_union<a: fixed_size_binary[10]=0, b: string=1>)
    """


cdef class TimestampType(DataType):
    """
    Concrete class for timestamp data types.

    Examples
    --------
    >>> import pyarrow as pa

    Create an instance of timestamp type:

    >>> pa.timestamp('us')
    TimestampType(timestamp[us])

    Create an instance of timestamp type with timezone:

    >>> pa.timestamp('s', tz='UTC')
    TimestampType(timestamp[s, tz=UTC])
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.ts_type = <const CTimestampType*> type.get()

    @property
    def unit(self):
        """
        The timestamp unit ('s', 'ms', 'us' or 'ns').

        Examples
        --------
        >>> import pyarrow as pa
        >>> t = pa.timestamp('us')
        >>> t.unit
        'us'
        """
        return timeunit_to_string(self.ts_type.unit())

    @property
    def tz(self):
        """
        The timestamp time zone, if any, or None.

        Examples
        --------
        >>> import pyarrow as pa
        >>> t = pa.timestamp('s', tz='UTC')
        >>> t.tz
        'UTC'
        """
        if self.ts_type.timezone().size() > 0:
            return frombytes(self.ts_type.timezone())
        else:
            return None

    def __reduce__(self):
        return timestamp, (self.unit, self.tz)


cdef class Time32Type(DataType):
    """
    Concrete class for time32 data types.

    Supported time unit resolutions are 's' [second]
    and 'ms' [millisecond].

    Examples
    --------
    Create an instance of time32 type:

    >>> import pyarrow as pa
    >>> pa.time32('ms')
    Time32Type(time32[ms])
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.time_type = <const CTime32Type*> type.get()

    @property
    def unit(self):
        """
        The time unit ('s' or 'ms').

        Examples
        --------
        >>> import pyarrow as pa
        >>> t = pa.time32('ms')
        >>> t.unit
        'ms'
        """
        return timeunit_to_string(self.time_type.unit())


cdef class Time64Type(DataType):
    """
    Concrete class for time64 data types.

    Supported time unit resolutions are 'us' [microsecond]
    and 'ns' [nanosecond].

    Examples
    --------
    Create an instance of time64 type:

    >>> import pyarrow as pa
    >>> pa.time64('us')
    Time64Type(time64[us])
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.time_type = <const CTime64Type*> type.get()

    @property
    def unit(self):
        """
        The time unit ('us' or 'ns').

        Examples
        --------
        >>> import pyarrow as pa
        >>> t = pa.time64('us')
        >>> t.unit
        'us'
        """
        return timeunit_to_string(self.time_type.unit())


cdef class DurationType(DataType):
    """
    Concrete class for duration data types.

    Examples
    --------
    Create an instance of duration type:

    >>> import pyarrow as pa
    >>> pa.duration('s')
    DurationType(duration[s])
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.duration_type = <const CDurationType*> type.get()

    @property
    def unit(self):
        """
        The duration unit ('s', 'ms', 'us' or 'ns').

        Examples
        --------
        >>> import pyarrow as pa
        >>> t = pa.duration('s')
        >>> t.unit
        's'
        """
        return timeunit_to_string(self.duration_type.unit())


cdef class FixedSizeBinaryType(DataType):
    """
    Concrete class for fixed-size binary data types.

    Examples
    --------
    Create an instance of fixed-size binary type:

    >>> import pyarrow as pa
    >>> pa.binary(3)
    FixedSizeBinaryType(fixed_size_binary[3])
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.fixed_size_binary_type = (
            <const CFixedSizeBinaryType*> type.get())

    def __reduce__(self):
        return binary, (self.byte_width,)


cdef class Decimal128Type(FixedSizeBinaryType):
    """
    Concrete class for decimal128 data types.

    Examples
    --------
    Create an instance of decimal128 type:

    >>> import pyarrow as pa
    >>> pa.decimal128(5, 2)
    Decimal128Type(decimal128(5, 2))
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

        Examples
        --------
        >>> import pyarrow as pa
        >>> t = pa.decimal128(5, 2)
        >>> t.precision
        5
        """
        return self.decimal128_type.precision()

    @property
    def scale(self):
        """
        The decimal scale (an integer).

        Examples
        --------
        >>> import pyarrow as pa
        >>> t = pa.decimal128(5, 2)
        >>> t.scale
        2
        """
        return self.decimal128_type.scale()


cdef class Decimal256Type(FixedSizeBinaryType):
    """
    Concrete class for decimal256 data types.

    Examples
    --------
    Create an instance of decimal256 type:

    >>> import pyarrow as pa
    >>> pa.decimal256(76, 38)
    Decimal256Type(decimal256(76, 38))
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        FixedSizeBinaryType.init(self, type)
        self.decimal256_type = <const CDecimal256Type*> type.get()

    def __reduce__(self):
        return decimal256, (self.precision, self.scale)

    @property
    def precision(self):
        """
        The decimal precision, in number of decimal digits (an integer).

        Examples
        --------
        >>> import pyarrow as pa
        >>> t = pa.decimal256(76, 38)
        >>> t.precision
        76
        """
        return self.decimal256_type.precision()

    @property
    def scale(self):
        """
        The decimal scale (an integer).

        Examples
        --------
        >>> import pyarrow as pa
        >>> t = pa.decimal256(76, 38)
        >>> t.scale
        38
        """
        return self.decimal256_type.scale()


cdef class RunEndEncodedType(DataType):
    """
    Concrete class for run-end encoded types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.run_end_encoded_type = <const CRunEndEncodedType*> type.get()

    def __reduce__(self):
        return run_end_encoded, (self.run_end_type, self.value_type)

    @property
    def run_end_type(self):
        return pyarrow_wrap_data_type(self.run_end_encoded_type.run_end_type())

    @property
    def value_type(self):
        return pyarrow_wrap_data_type(self.run_end_encoded_type.value_type())


cdef class BaseExtensionType(DataType):
    """
    Concrete base class for extension types.
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        DataType.init(self, type)
        self.ext_type = <const CExtensionType*> type.get()

    def __arrow_ext_class__(self):
        """
        The associated array extension class
        """
        return ExtensionArray

    def __arrow_ext_scalar_class__(self):
        """
        The associated scalar class
        """
        return ExtensionScalar

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

    def wrap_array(self, storage):
        """
        Wrap the given storage array as an extension array.

        Parameters
        ----------
        storage : Array or ChunkedArray

        Returns
        -------
        array : Array or ChunkedArray
            Extension array wrapping the storage array
        """
        cdef:
            shared_ptr[CDataType] c_storage_type

        if isinstance(storage, Array):
            c_storage_type = (<Array> storage).ap.type()
        elif isinstance(storage, ChunkedArray):
            c_storage_type = (<ChunkedArray> storage).chunked_array.type()
        else:
            raise TypeError(
                f"Expected array or chunked array, got {storage.__class__}")

        if not c_storage_type.get().Equals(deref(self.ext_type)
                                           .storage_type(), False):
            raise TypeError(
                f"Incompatible storage type for {self}: "
                f"expected {self.storage_type}, got {storage.type}")

        if isinstance(storage, Array):
            return pyarrow_wrap_array(
                self.ext_type.WrapArray(
                    self.sp_type, (<Array> storage).sp_array))
        else:
            return pyarrow_wrap_chunked_array(
                self.ext_type.WrapArray(
                    self.sp_type, (<ChunkedArray> storage).sp_chunked_array))


cdef class ExtensionType(BaseExtensionType):
    """
    Concrete base class for Python-defined extension types.

    Parameters
    ----------
    storage_type : DataType
        The underlying storage type for the extension type.
    extension_name : str
        A unique name distinguishing this extension type. The name will be
        used when deserializing IPC data.

    Examples
    --------
    Define a UuidType extension type subclassing ExtensionType:

    >>> import pyarrow as pa
    >>> class UuidType(pa.ExtensionType):
    ...    def __init__(self):
    ...       pa.ExtensionType.__init__(self, pa.binary(16), "my_package.uuid")
    ...    def __arrow_ext_serialize__(self):
    ...       # since we don't have a parameterized type, we don't need extra
    ...       # metadata to be deserialized
    ...       return b''
    ...    @classmethod
    ...    def __arrow_ext_deserialize__(self, storage_type, serialized):
    ...       # return an instance of this subclass given the serialized
    ...       # metadata.
    ...       return UuidType()
    ...

    Register the extension type:

    >>> pa.register_extension_type(UuidType())

    Create an instance of UuidType extension type:

    >>> uuid_type = UuidType()

    Inspect the extension type:

    >>> uuid_type.extension_name
    'my_package.uuid'
    >>> uuid_type.storage_type
    FixedSizeBinaryType(fixed_size_binary[16])

    Wrap an array as an extension array:

    >>> import uuid
    >>> storage_array = pa.array([uuid.uuid4().bytes for _ in range(4)], pa.binary(16))
    >>> uuid_type.wrap_array(storage_array)
    <pyarrow.lib.ExtensionArray object at ...>
    [
      ...
    ]

    Or do the same with creating an ExtensionArray:

    >>> pa.ExtensionArray.from_storage(uuid_type, storage_array)
    <pyarrow.lib.ExtensionArray object at ...>
    [
      ...
    ]

    Unregister the extension type:

    >>> pa.unregister_extension_type("my_package.uuid")
    """

    def __cinit__(self):
        if type(self) is ExtensionType:
            raise TypeError("Can only instantiate subclasses of "
                            "ExtensionType")

    def __init__(self, DataType storage_type, extension_name):
        """
        Initialize an extension type instance.

        This should be called at the end of the subclass'
        ``__init__`` method.
        """
        cdef:
            shared_ptr[CExtensionType] cpy_ext_type
            c_string c_extension_name

        c_extension_name = tobytes(extension_name)

        assert storage_type is not None
        check_status(CPyExtensionType.FromClass(
            storage_type.sp_type, c_extension_name, type(self),
            &cpy_ext_type))
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
                    self.extension_name == other.extension_name and
                    self.storage_type == other.storage_type)
        else:
            return NotImplemented

    def __repr__(self):
        fmt = '{0.__class__.__name__}({1})'
        return fmt.format(self, repr(self.storage_type))

    def __arrow_ext_serialize__(self):
        """
        Serialized representation of metadata to reconstruct the type object.

        This method should return a bytes object, and those serialized bytes
        are stored in the custom metadata of the Field holding an extension
        type in an IPC message.
        The bytes are passed to ``__arrow_ext_deserialize`` and should hold
        sufficient information to reconstruct the data type instance.
        """
        return NotImplementedError

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        """
        Return an extension type instance from the storage type and serialized
        metadata.

        This method should return an instance of the ExtensionType subclass
        that matches the passed storage type and serialized metadata (the
        return value of ``__arrow_ext_serialize__``).
        """
        return NotImplementedError

    def __reduce__(self):
        return self.__arrow_ext_deserialize__, (self.storage_type, self.__arrow_ext_serialize__())

    def __arrow_ext_class__(self):
        """Return an extension array class to be used for building or
        deserializing arrays with this extension type.

        This method should return a subclass of the ExtensionArray class. By
        default, if not specialized in the extension implementation, an
        extension type array will be a built-in ExtensionArray instance.
        """
        return ExtensionArray

    def __arrow_ext_scalar_class__(self):
        """Return an extension scalar class for building scalars with this
        extension type.

        This method should return subclass of the ExtensionScalar class. By
        default, if not specialized in the extension implementation, an
        extension type scalar will be a built-in ExtensionScalar instance.
        """
        return ExtensionScalar


cdef class FixedShapeTensorType(BaseExtensionType):
    """
    Concrete class for fixed shape tensor extension type.

    Examples
    --------
    Create an instance of fixed shape tensor extension type:

    >>> import pyarrow as pa
    >>> pa.fixed_shape_tensor(pa.int32(), [2, 2])
    FixedShapeTensorType(extension<arrow.fixed_shape_tensor[value_type=int32, shape=[2,2]]>)

    Create an instance of fixed shape tensor extension type with
    permutation:

    >>> tensor_type = pa.fixed_shape_tensor(pa.int8(), (2, 2, 3),
    ...                                     permutation=[0, 2, 1])
    >>> tensor_type.permutation
    [0, 2, 1]
    """

    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        BaseExtensionType.init(self, type)
        self.tensor_ext_type = <const CFixedShapeTensorType*> type.get()

    @property
    def value_type(self):
        """
        Data type of an individual tensor.
        """
        return pyarrow_wrap_data_type(self.tensor_ext_type.value_type())

    @property
    def shape(self):
        """
        Shape of the tensors.
        """
        return self.tensor_ext_type.shape()

    @property
    def dim_names(self):
        """
        Explicit names of the dimensions.
        """
        list_of_bytes = self.tensor_ext_type.dim_names()
        if len(list_of_bytes) != 0:
            return [frombytes(x) for x in list_of_bytes]
        else:
            return None

    @property
    def permutation(self):
        """
        Indices of the dimensions ordering.
        """
        indices = self.tensor_ext_type.permutation()
        if len(indices) != 0:
            return indices
        else:
            return None

    def __arrow_ext_class__(self):
        return FixedShapeTensorArray

    def __reduce__(self):
        return fixed_shape_tensor, (self.value_type, self.shape,
                                    self.dim_names, self.permutation)

    def __arrow_ext_scalar_class__(self):
        return FixedShapeTensorScalar


_py_extension_type_auto_load = False


cdef class PyExtensionType(ExtensionType):
    """
    Concrete base class for Python-defined extension types based on pickle
    for (de)serialization.

    .. warning::
       This class is deprecated and its deserialization is disabled by default.
       :class:`ExtensionType` is recommended instead.

    Parameters
    ----------
    storage_type : DataType
        The storage type for which the extension is built.
    """

    def __cinit__(self):
        if type(self) is PyExtensionType:
            raise TypeError("Can only instantiate subclasses of "
                            "PyExtensionType")

    def __init__(self, DataType storage_type):
        warnings.warn(
            "pyarrow.PyExtensionType is deprecated "
            "and will refuse deserialization by default. "
            "Instead, please derive from pyarrow.ExtensionType and implement "
            "your own serialization mechanism.",
            FutureWarning)
        ExtensionType.__init__(self, storage_type, "arrow.py_extension_type")

    def __reduce__(self):
        raise NotImplementedError("Please implement {0}.__reduce__"
                                  .format(type(self).__name__))

    def __arrow_ext_serialize__(self):
        return pickle.dumps(self)

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        if not _py_extension_type_auto_load:
            warnings.warn(
                "pickle-based deserialization of pyarrow.PyExtensionType subclasses "
                "is disabled by default; if you only ingest "
                "trusted data files, you may re-enable this using "
                "`pyarrow.PyExtensionType.set_auto_load(True)`.\n"
                "In the future, Python-defined extension subclasses should "
                "derive from pyarrow.ExtensionType (not pyarrow.PyExtensionType) "
                "and implement their own serialization mechanism.\n",
                RuntimeWarning)
            return UnknownExtensionType(storage_type, serialized)
        try:
            ty = pickle.loads(serialized)
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

    # XXX Cython marks extension types as immutable, so cannot expose this
    # as a writable class attribute.
    @classmethod
    def set_auto_load(cls, value):
        """
        Enable or disable auto-loading of serialized PyExtensionType instances.

        Parameters
        ----------
        value : bool
            Whether to enable auto-loading.
        """
        global _py_extension_type_auto_load
        assert isinstance(value, bool)
        _py_extension_type_auto_load = value


cdef class UnknownExtensionType(PyExtensionType):
    """
    A concrete class for Python-defined extension types that refer to
    an unknown Python implementation.

    Parameters
    ----------
    storage_type : DataType
        The storage type for which the extension is built.
    serialized : bytes
        The serialised output.
    """

    cdef:
        bytes serialized

    def __init__(self, DataType storage_type, serialized):
        self.serialized = serialized
        PyExtensionType.__init__(self, storage_type)

    def __arrow_ext_serialize__(self):
        return self.serialized


_python_extension_types_registry = []


def register_extension_type(ext_type):
    """
    Register a Python extension type.

    Registration is based on the extension name (so different registered types
    need unique extension names). Registration needs an extension type
    instance, but then works for any instance of the same subclass regardless
    of parametrization of the type.

    Parameters
    ----------
    ext_type : BaseExtensionType instance
        The ExtensionType subclass to register.

    Examples
    --------
    Define a UuidType extension type subclassing ExtensionType:

    >>> import pyarrow as pa
    >>> class UuidType(pa.ExtensionType):
    ...    def __init__(self):
    ...       pa.ExtensionType.__init__(self, pa.binary(16), "my_package.uuid")
    ...    def __arrow_ext_serialize__(self):
    ...       # since we don't have a parameterized type, we don't need extra
    ...       # metadata to be deserialized
    ...       return b''
    ...    @classmethod
    ...    def __arrow_ext_deserialize__(self, storage_type, serialized):
    ...       # return an instance of this subclass given the serialized
    ...       # metadata.
    ...       return UuidType()
    ...

    Register the extension type:

    >>> pa.register_extension_type(UuidType())

    Unregister the extension type:

    >>> pa.unregister_extension_type("my_package.uuid")
    """
    cdef:
        DataType _type = ensure_type(ext_type, allow_none=False)

    if not isinstance(_type, BaseExtensionType):
        raise TypeError("Only extension types can be registered")

    # register on the C++ side
    check_status(
        RegisterPyExtensionType(<shared_ptr[CDataType]> _type.sp_type))

    # register on the python side
    _python_extension_types_registry.append(_type)


def unregister_extension_type(type_name):
    """
    Unregister a Python extension type.

    Parameters
    ----------
    type_name : str
        The name of the ExtensionType subclass to unregister.

    Examples
    --------
    Define a UuidType extension type subclassing ExtensionType:

    >>> import pyarrow as pa
    >>> class UuidType(pa.ExtensionType):
    ...    def __init__(self):
    ...       pa.ExtensionType.__init__(self, pa.binary(16), "my_package.uuid")
    ...    def __arrow_ext_serialize__(self):
    ...       # since we don't have a parameterized type, we don't need extra
    ...       # metadata to be deserialized
    ...       return b''
    ...    @classmethod
    ...    def __arrow_ext_deserialize__(self, storage_type, serialized):
    ...       # return an instance of this subclass given the serialized
    ...       # metadata.
    ...       return UuidType()
    ...

    Register the extension type:

    >>> pa.register_extension_type(UuidType())

    Unregister the extension type:

    >>> pa.unregister_extension_type("my_package.uuid")
    """
    cdef:
        c_string c_type_name = tobytes(type_name)
    check_status(UnregisterPyExtensionType(c_type_name))


cdef class KeyValueMetadata(_Metadata, Mapping):
    """
    KeyValueMetadata

    Parameters
    ----------
    __arg0__ : dict
        A dict of the key-value metadata
    **kwargs : optional
        additional key-value metadata
    """

    def __init__(self, __arg0__=None, **kwargs):
        cdef:
            vector[c_string] keys, values
            shared_ptr[const CKeyValueMetadata] result

        items = []
        if __arg0__ is not None:
            other = (__arg0__.items() if isinstance(__arg0__, Mapping)
                     else __arg0__)
            items.extend((tobytes(k), v) for k, v in other)

        prior_keys = {k for k, v in items}
        for k, v in kwargs.items():
            k = tobytes(k)
            if k in prior_keys:
                raise KeyError("Duplicate key {}, "
                               "use pass all items as list of tuples if you "
                               "intend to have duplicate keys")
            items.append((k, v))

        keys.reserve(len(items))
        for key, value in items:
            keys.push_back(tobytes(key))
            values.push_back(tobytes(value))
        result.reset(new CKeyValueMetadata(move(keys), move(values)))
        self.init(result)

    cdef void init(self, const shared_ptr[const CKeyValueMetadata]& wrapped):
        self.wrapped = wrapped
        self.metadata = wrapped.get()

    @staticmethod
    cdef wrap(const shared_ptr[const CKeyValueMetadata]& sp):
        cdef KeyValueMetadata self = KeyValueMetadata.__new__(KeyValueMetadata)
        self.init(sp)
        return self

    cdef inline shared_ptr[const CKeyValueMetadata] unwrap(self) nogil:
        return self.wrapped

    def equals(self, KeyValueMetadata other):
        """
        Parameters
        ----------
        other : pyarrow.KeyValueMetadata

        Returns
        -------
        bool
        """
        return self.metadata.Equals(deref(other.wrapped))

    def __repr__(self):
        return str(self)

    def __str__(self):
        return frombytes(self.metadata.ToString(), safe=True)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            pass

        if isinstance(other, Mapping):
            try:
                other = KeyValueMetadata(other)
                return self.equals(other)
            except TypeError:
                pass

        return NotImplemented

    def __len__(self):
        return self.metadata.size()

    def __contains__(self, key):
        return self.metadata.Contains(tobytes(key))

    def __getitem__(self, key):
        return GetResultValue(self.metadata.Get(tobytes(key)))

    def __iter__(self):
        return self.keys()

    def __reduce__(self):
        return KeyValueMetadata, (list(self.items()),)

    def key(self, i):
        """
        Parameters
        ----------
        i : int

        Returns
        -------
        byte
        """
        return self.metadata.key(i)

    def value(self, i):
        """
        Parameters
        ----------
        i : int

        Returns
        -------
        byte
        """
        return self.metadata.value(i)

    def keys(self):
        for i in range(self.metadata.size()):
            yield self.metadata.key(i)

    def values(self):
        for i in range(self.metadata.size()):
            yield self.metadata.value(i)

    def items(self):
        for i in range(self.metadata.size()):
            yield (self.metadata.key(i), self.metadata.value(i))

    def get_all(self, key):
        """
        Parameters
        ----------
        key : str

        Returns
        -------
        list[byte]
        """
        key = tobytes(key)
        return [v for k, v in self.items() if k == key]

    def to_dict(self):
        """
        Convert KeyValueMetadata to dict. If a key occurs twice, the value for
        the first one is returned
        """
        cdef object key  # to force coercion to Python
        result = ordered_dict()
        for i in range(self.metadata.size()):
            key = self.metadata.key(i)
            if key not in result:
                result[key] = self.metadata.value(i)
        return result


cpdef KeyValueMetadata ensure_metadata(object meta, c_bool allow_none=False):
    if allow_none and meta is None:
        return None
    elif isinstance(meta, KeyValueMetadata):
        return meta
    else:
        return KeyValueMetadata(meta)


cdef class Field(_Weakrefable):
    """
    A named field, with a data type, nullability, and optional metadata.

    Notes
    -----
    Do not use this class's constructor directly; use pyarrow.field

    Examples
    --------
    Create an instance of pyarrow.Field:

    >>> import pyarrow as pa
    >>> pa.field('key', pa.int32())
    pyarrow.Field<key: int32>
    >>> pa.field('key', pa.int32(), nullable=False)
    pyarrow.Field<key: int32 not null>
    >>> field = pa.field('key', pa.int32(),
    ...                  metadata={"key": "Something important"})
    >>> field
    pyarrow.Field<key: int32>
    >>> field.metadata
    {b'key': b'Something important'}

    Use the field to create a struct type:

    >>> pa.struct([field])
    StructType(struct<key: int32>)
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

    def equals(self, Field other, bint check_metadata=False):
        """
        Test if this field is equal to the other

        Parameters
        ----------
        other : pyarrow.Field
        check_metadata : bool, default False
            Whether Field metadata equality should be checked as well.

        Returns
        -------
        is_equal : bool

        Examples
        --------
        >>> import pyarrow as pa
        >>> f1 = pa.field('key', pa.int32())
        >>> f2 = pa.field('key', pa.int32(), nullable=False)
        >>> f1.equals(f2)
        False
        >>> f1.equals(f1)
        True
        """
        return self.field.Equals(deref(other.field), check_metadata)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def __reduce__(self):
        return field, (self.name, self.type, self.nullable, self.metadata)

    def __str__(self):
        return 'pyarrow.Field<{0}>'.format(
            frombytes(self.field.ToString(), safe=True))

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.field.name(), self.type, self.field.nullable()))

    @property
    def nullable(self):
        """
        The field nullability.

        Examples
        --------
        >>> import pyarrow as pa
        >>> f1 = pa.field('key', pa.int32())
        >>> f2 = pa.field('key', pa.int32(), nullable=False)
        >>> f1.nullable
        True
        >>> f2.nullable
        False
        """
        return self.field.nullable()

    @property
    def name(self):
        """
        The field name.

        Examples
        --------
        >>> import pyarrow as pa
        >>> field = pa.field('key', pa.int32())
        >>> field.name
        'key'
        """
        return frombytes(self.field.name())

    @property
    def metadata(self):
        """
        The field metadata.

        Examples
        --------
        >>> import pyarrow as pa
        >>> field = pa.field('key', pa.int32(),
        ...                  metadata={"key": "Something important"})
        >>> field.metadata
        {b'key': b'Something important'}
        """
        wrapped = pyarrow_wrap_metadata(self.field.metadata())
        if wrapped is not None:
            return wrapped.to_dict()
        else:
            return wrapped

    def with_metadata(self, metadata):
        """
        Add metadata as dict of string keys and values to Field

        Parameters
        ----------
        metadata : dict
            Keys and values must be string-like / coercible to bytes

        Returns
        -------
        field : pyarrow.Field

        Examples
        --------
        >>> import pyarrow as pa
        >>> field = pa.field('key', pa.int32())

        Create new field by adding metadata to existing one:

        >>> field_new = field.with_metadata({"key": "Something important"})
        >>> field_new
        pyarrow.Field<key: int32>
        >>> field_new.metadata
        {b'key': b'Something important'}
        """
        cdef shared_ptr[CField] c_field

        meta = ensure_metadata(metadata, allow_none=False)
        with nogil:
            c_field = self.field.WithMetadata(meta.unwrap())

        return pyarrow_wrap_field(c_field)

    def remove_metadata(self):
        """
        Create new field without metadata, if any

        Returns
        -------
        field : pyarrow.Field

        Examples
        --------
        >>> import pyarrow as pa
        >>> field = pa.field('key', pa.int32(),
        ...                  metadata={"key": "Something important"})
        >>> field.metadata
        {b'key': b'Something important'}

        Create new field by removing the metadata from the existing one:

        >>> field_new = field.remove_metadata()
        >>> field_new.metadata
        """
        cdef shared_ptr[CField] new_field
        with nogil:
            new_field = self.field.RemoveMetadata()
        return pyarrow_wrap_field(new_field)

    def with_type(self, DataType new_type):
        """
        A copy of this field with the replaced type

        Parameters
        ----------
        new_type : pyarrow.DataType

        Returns
        -------
        field : pyarrow.Field

        Examples
        --------
        >>> import pyarrow as pa
        >>> field = pa.field('key', pa.int32())
        >>> field
        pyarrow.Field<key: int32>

        Create new field by replacing type of an existing one:

        >>> field_new = field.with_type(pa.int64())
        >>> field_new
        pyarrow.Field<key: int64>
        """
        cdef:
            shared_ptr[CField] c_field
            shared_ptr[CDataType] c_datatype

        c_datatype = pyarrow_unwrap_data_type(new_type)
        with nogil:
            c_field = self.field.WithType(c_datatype)

        return pyarrow_wrap_field(c_field)

    def with_name(self, name):
        """
        A copy of this field with the replaced name

        Parameters
        ----------
        name : str

        Returns
        -------
        field : pyarrow.Field

        Examples
        --------
        >>> import pyarrow as pa
        >>> field = pa.field('key', pa.int32())
        >>> field
        pyarrow.Field<key: int32>

        Create new field by replacing the name of an existing one:

        >>> field_new = field.with_name('lock')
        >>> field_new
        pyarrow.Field<lock: int32>
        """
        cdef:
            shared_ptr[CField] c_field

        c_field = self.field.WithName(tobytes(name))

        return pyarrow_wrap_field(c_field)

    def with_nullable(self, nullable):
        """
        A copy of this field with the replaced nullability

        Parameters
        ----------
        nullable : bool

        Returns
        -------
        field: pyarrow.Field

        Examples
        --------
        >>> import pyarrow as pa
        >>> field = pa.field('key', pa.int32())
        >>> field
        pyarrow.Field<key: int32>
        >>> field.nullable
        True

        Create new field by replacing the nullability of an existing one:

        >>> field_new = field.with_nullable(False)
        >>> field_new
        pyarrow.Field<key: int32 not null>
        >>> field_new.nullable
        False
        """
        cdef:
            shared_ptr[CField] field
            c_bool c_nullable

        c_nullable = bool(nullable)
        with nogil:
            c_field = self.field.WithNullable(c_nullable)

        return pyarrow_wrap_field(c_field)

    def flatten(self):
        """
        Flatten this field.  If a struct field, individual child fields
        will be returned with their names prefixed by the parent's name.

        Returns
        -------
        fields : List[pyarrow.Field]

        Examples
        --------
        >>> import pyarrow as pa
        >>> f1 = pa.field('bar', pa.float64(), nullable=False)
        >>> f2 = pa.field('foo', pa.int32()).with_metadata({"key": "Something important"})
        >>> ff = pa.field('ff', pa.struct([f1, f2]), nullable=False)

        Flatten a struct field:

        >>> ff
        pyarrow.Field<ff: struct<bar: double not null, foo: int32> not null>
        >>> ff.flatten()
        [pyarrow.Field<ff.bar: double not null>, pyarrow.Field<ff.foo: int32>]
        """
        cdef vector[shared_ptr[CField]] flattened
        with nogil:
            flattened = self.field.Flatten()
        return [pyarrow_wrap_field(f) for f in flattened]

    def _export_to_c(self, out_ptr):
        """
        Export to a C ArrowSchema struct, given its pointer.

        Be careful: if you don't pass the ArrowSchema struct to a consumer,
        its memory will leak.  This is a low-level function intended for
        expert users.
        """
        check_status(ExportField(deref(self.field),
                                 <ArrowSchema*> _as_c_pointer(out_ptr)))

    @staticmethod
    def _import_from_c(in_ptr):
        """
        Import Field from a C ArrowSchema struct, given its pointer.

        This is a low-level function intended for expert users.
        """
        cdef void* c_ptr = _as_c_pointer(in_ptr)
        with nogil:
            result = GetResultValue(ImportField(<ArrowSchema*> c_ptr))
        return pyarrow_wrap_field(result)

    def __arrow_c_schema__(self):
        """
        Export to a ArrowSchema PyCapsule

        Unlike _export_to_c, this will not leak memory if the capsule is not used.
        """
        cdef ArrowSchema* c_schema
        capsule = alloc_c_schema(&c_schema)

        with nogil:
            check_status(ExportField(deref(self.field), c_schema))

        return capsule

    @staticmethod
    def _import_from_c_capsule(schema):
        """
        Import a Field from a ArrowSchema PyCapsule

        Parameters
        ----------
        schema : PyCapsule
            A valid PyCapsule with name 'arrow_schema' containing an
            ArrowSchema pointer.
        """
        cdef:
            ArrowSchema* c_schema
            shared_ptr[CField] c_field

        if not PyCapsule_IsValid(schema, 'arrow_schema'):
            raise ValueError(
                "Not an ArrowSchema object"
            )
        c_schema = <ArrowSchema*> PyCapsule_GetPointer(schema, 'arrow_schema')

        with nogil:
            c_field = GetResultValue(ImportField(c_schema))

        return pyarrow_wrap_field(c_field)


cdef class Schema(_Weakrefable):
    """
    A named collection of types a.k.a schema. A schema defines the
    column names and types in a record batch or table data structure.
    They also contain metadata about the columns. For example, schemas
    converted from Pandas contain metadata about their original Pandas
    types so they can be converted back to the same types.

    Warnings
    --------
    Do not call this class's constructor directly. Instead use
    :func:`pyarrow.schema` factory function which makes a new Arrow
    Schema object.

    Examples
    --------
    Create a new Arrow Schema object:

    >>> import pyarrow as pa
    >>> pa.schema([
    ...     ('some_int', pa.int32()),
    ...     ('some_string', pa.string())
    ... ])
    some_int: int32
    some_string: string

    Create Arrow Schema with metadata:

    >>> pa.schema([
    ...     pa.field('n_legs', pa.int64()),
    ...     pa.field('animals', pa.string())],
    ...     metadata={"n_legs": "Number of legs per animal"})
    n_legs: int64
    animals: string
    -- schema metadata --
    n_legs: 'Number of legs per animal'
    """

    def __cinit__(self):
        pass

    def __init__(self):
        raise TypeError("Do not call Schema's constructor directly, use "
                        "`pyarrow.schema` instead.")

    def __len__(self):
        return self.schema.num_fields()

    def __getitem__(self, key):
        # access by integer index
        return self._field(key)

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

    def __sizeof__(self):
        size = 0
        if self.metadata:
            for key, value in self.metadata.items():
                size += sys.getsizeof(key)
                size += sys.getsizeof(value)

        return size + super(Schema, self).__sizeof__()

    @property
    def pandas_metadata(self):
        """
        Return deserialized-from-JSON pandas metadata field (if it exists)

        Examples
        --------
        >>> import pyarrow as pa
        >>> import pandas as pd
        >>> df = pd.DataFrame({'n_legs': [2, 4, 5, 100],
        ...                    'animals': ["Flamingo", "Horse", "Brittle stars", "Centipede"]})
        >>> schema = pa.Table.from_pandas(df).schema

        Select pandas metadata field from Arrow Schema:

        >>> schema.pandas_metadata
        {'index_columns': [{'kind': 'range', 'name': None, 'start': 0, 'stop': 4, 'step': 1}], ...
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

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Get the names of the schema's fields:

        >>> schema.names
        ['n_legs', 'animals']
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

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Get the types of the schema's fields:

        >>> schema.types
        [DataType(int64), DataType(string)]
        """
        return [field.type for field in self]

    @property
    def metadata(self):
        """
        The schema's metadata.

        Returns
        -------
        metadata: dict

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())],
        ...     metadata={"n_legs": "Number of legs per animal"})

        Get the metadata of the schema's fields:

        >>> schema.metadata
        {b'n_legs': b'Number of legs per animal'}
        """
        wrapped = pyarrow_wrap_metadata(self.schema.metadata())
        if wrapped is not None:
            return wrapped.to_dict()
        else:
            return wrapped

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

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Create an empty table with schema's fields:

        >>> schema.empty_table()
        pyarrow.Table
        n_legs: int64
        animals: string
        ----
        n_legs: [[]]
        animals: [[]]
        """
        arrays = [_empty_array(field.type) for field in self]
        return Table.from_arrays(arrays, schema=self)

    def equals(self, Schema other not None, bint check_metadata=False):
        """
        Test if this schema is equal to the other

        Parameters
        ----------
        other :  pyarrow.Schema
        check_metadata : bool, default False
            Key/value metadata must be equal too

        Returns
        -------
        is_equal : bool

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema1 = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())],
        ...     metadata={"n_legs": "Number of legs per animal"})
        >>> schema2 = pa.schema([
        ...     ('some_int', pa.int32()),
        ...     ('some_string', pa.string())
        ... ])

        Test two equal schemas:

        >>> schema1.equals(schema1)
        True

        Test two unequal schemas:

        >>> schema1.equals(schema2)
        False
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

        Create an Arrow Schema from the schema of a pandas dataframe:

        >>> pa.Schema.from_pandas(df)
        int: int64
        str: string
        -- schema metadata --
        pandas: '{"index_columns": [{"kind": "range", "name": null, ...
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

    def field(self, i):
        """
        Select a field by its column name or numeric index.

        Parameters
        ----------
        i : int or string

        Returns
        -------
        pyarrow.Field

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Select the second field:

        >>> schema.field(1)
        pyarrow.Field<animals: string>

        Select the field of the column named 'n_legs':

        >>> schema.field('n_legs')
        pyarrow.Field<n_legs: int64>
        """
        if isinstance(i, (bytes, str)):
            field_index = self.get_field_index(i)
            if field_index < 0:
                raise KeyError("Column {} does not exist in schema".format(i))
            else:
                return self._field(field_index)
        elif isinstance(i, int):
            return self._field(i)
        else:
            raise TypeError("Index must either be string or integer")

    def _field(self, int i):
        """
        Select a field by its numeric index.

        Parameters
        ----------
        i : int

        Returns
        -------
        pyarrow.Field
        """
        cdef int index = <int> _normalize_index(i, self.schema.num_fields())
        return pyarrow_wrap_field(self.schema.field(index))

    def field_by_name(self, name):
        """
        DEPRECATED

        Parameters
        ----------
        name : str

        Returns
        -------
        field: pyarrow.Field
        """
        cdef:
            vector[shared_ptr[CField]] results

        warnings.warn(
            "The 'field_by_name' method is deprecated, use 'field' instead",
            FutureWarning, stacklevel=2)

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
        """
        Return index of the unique field with the given name.

        Parameters
        ----------
        name : str
            The name of the field to look up.

        Returns
        -------
        index : int
            The index of the field with the given name; -1 if the
            name isn't found or there are several fields with the given
            name.

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Get the index of the field named 'animals':

        >>> schema.get_field_index("animals")
        1

        Index in case of several fields with the given name:

        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string()),
        ...     pa.field('animals', pa.bool_())],
        ...     metadata={"n_legs": "Number of legs per animal"})
        >>> schema.get_field_index("animals")
        -1
        """
        return self.schema.GetFieldIndex(tobytes(name))

    def get_all_field_indices(self, name):
        """
        Return sorted list of indices for the fields with the given name.

        Parameters
        ----------
        name : str
            The name of the field to look up.

        Returns
        -------
        indices : List[int]

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string()),
        ...     pa.field('animals', pa.bool_())])

        Get the indexes of the fields named 'animals':

        >>> schema.get_all_field_indices("animals")
        [1, 2]
        """
        return self.schema.GetAllFieldIndices(tobytes(name))

    def append(self, Field field):
        """
        Append a field at the end of the schema.

        In contrast to Python's ``list.append()`` it does return a new
        object, leaving the original Schema unmodified.

        Parameters
        ----------
        field : Field

        Returns
        -------
        schema: Schema
            New object with appended field.

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Append a field 'extra' at the end of the schema:

        >>> schema_new = schema.append(pa.field('extra', pa.bool_()))
        >>> schema_new
        n_legs: int64
        animals: string
        extra: bool

        Original schema is unmodified:

        >>> schema
        n_legs: int64
        animals: string
        """
        return self.insert(self.schema.num_fields(), field)

    def insert(self, int i, Field field):
        """
        Add a field at position i to the schema.

        Parameters
        ----------
        i : int
        field : Field

        Returns
        -------
        schema: Schema

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Insert a new field on the second position:

        >>> schema.insert(1, pa.field('extra', pa.bool_()))
        n_legs: int64
        extra: bool
        animals: string
        """
        cdef:
            shared_ptr[CSchema] new_schema
            shared_ptr[CField] c_field

        c_field = field.sp_field

        with nogil:
            new_schema = GetResultValue(self.schema.AddField(i, c_field))

        return pyarrow_wrap_schema(new_schema)

    def remove(self, int i):
        """
        Remove the field at index i from the schema.

        Parameters
        ----------
        i : int

        Returns
        -------
        schema: Schema

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Remove the second field of the schema:

        >>> schema.remove(1)
        n_legs: int64
        """
        cdef shared_ptr[CSchema] new_schema

        with nogil:
            new_schema = GetResultValue(self.schema.RemoveField(i))

        return pyarrow_wrap_schema(new_schema)

    def set(self, int i, Field field):
        """
        Replace a field at position i in the schema.

        Parameters
        ----------
        i : int
        field : Field

        Returns
        -------
        schema: Schema

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Replace the second field of the schema with a new field 'extra':

        >>> schema.set(1, pa.field('replaced', pa.bool_()))
        n_legs: int64
        replaced: bool
        """
        cdef:
            shared_ptr[CSchema] new_schema
            shared_ptr[CField] c_field

        c_field = field.sp_field

        with nogil:
            new_schema = GetResultValue(self.schema.SetField(i, c_field))

        return pyarrow_wrap_schema(new_schema)

    def add_metadata(self, metadata):
        """
        DEPRECATED

        Parameters
        ----------
        metadata : dict
            Keys and values must be string-like / coercible to bytes
        """
        warnings.warn("The 'add_metadata' method is deprecated, use "
                      "'with_metadata' instead", FutureWarning, stacklevel=2)
        return self.with_metadata(metadata)

    def with_metadata(self, metadata):
        """
        Add metadata as dict of string keys and values to Schema

        Parameters
        ----------
        metadata : dict
            Keys and values must be string-like / coercible to bytes

        Returns
        -------
        schema : pyarrow.Schema

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Add metadata to existing schema field:

        >>> schema.with_metadata({"n_legs": "Number of legs per animal"})
        n_legs: int64
        animals: string
        -- schema metadata --
        n_legs: 'Number of legs per animal'
        """
        cdef shared_ptr[CSchema] c_schema

        meta = ensure_metadata(metadata, allow_none=False)
        with nogil:
            c_schema = self.schema.WithMetadata(meta.unwrap())

        return pyarrow_wrap_schema(c_schema)

    def serialize(self, memory_pool=None):
        """
        Write Schema to Buffer as encapsulated IPC message

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            Uses default memory pool if not specified

        Returns
        -------
        serialized : Buffer

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())])

        Write schema to Buffer:

        >>> schema.serialize()
        <pyarrow.Buffer address=0x... size=... is_cpu=True is_mutable=True>
        """
        cdef:
            shared_ptr[CBuffer] buffer
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            buffer = GetResultValue(SerializeSchema(deref(self.schema),
                                                    pool))
        return pyarrow_wrap_buffer(buffer)

    def remove_metadata(self):
        """
        Create new schema without metadata, if any

        Returns
        -------
        schema : pyarrow.Schema

        Examples
        --------
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field('n_legs', pa.int64()),
        ...     pa.field('animals', pa.string())],
        ...     metadata={"n_legs": "Number of legs per animal"})
        >>> schema
        n_legs: int64
        animals: string
        -- schema metadata --
        n_legs: 'Number of legs per animal'

        Create a new schema with removing the metadata from the original:

        >>> schema.remove_metadata()
        n_legs: int64
        animals: string
        """
        cdef shared_ptr[CSchema] new_schema
        with nogil:
            new_schema = self.schema.RemoveMetadata()
        return pyarrow_wrap_schema(new_schema)

    def to_string(self, truncate_metadata=True, show_field_metadata=True,
                  show_schema_metadata=True):
        """
        Return human-readable representation of Schema

        Parameters
        ----------
        truncate_metadata : boolean, default True
            Limit metadata key/value display to a single line of ~80 characters
            or less
        show_field_metadata : boolean, default True
            Display Field-level KeyValueMetadata
        show_schema_metadata : boolean, default True
            Display Schema-level KeyValueMetadata

        Returns
        -------
        str : the formatted output
        """
        cdef:
            c_string result
            PrettyPrintOptions options = PrettyPrintOptions.Defaults()

        options.indent = 0
        options.truncate_metadata = truncate_metadata
        options.show_field_metadata = show_field_metadata
        options.show_schema_metadata = show_schema_metadata

        with nogil:
            check_status(
                PrettyPrint(
                    deref(self.schema),
                    options,
                    &result
                )
            )

        return frombytes(result, safe=True)

    def _export_to_c(self, out_ptr):
        """
        Export to a C ArrowSchema struct, given its pointer.

        Be careful: if you don't pass the ArrowSchema struct to a consumer,
        its memory will leak.  This is a low-level function intended for
        expert users.
        """
        check_status(ExportSchema(deref(self.schema),
                                  <ArrowSchema*> _as_c_pointer(out_ptr)))

    @staticmethod
    def _import_from_c(in_ptr):
        """
        Import Schema from a C ArrowSchema struct, given its pointer.

        This is a low-level function intended for expert users.
        """
        cdef void* c_ptr = _as_c_pointer(in_ptr)
        with nogil:
            result = GetResultValue(ImportSchema(<ArrowSchema*> c_ptr))
        return pyarrow_wrap_schema(result)

    def __str__(self):
        return self.to_string()

    def __repr__(self):
        return self.__str__()

    def __arrow_c_schema__(self):
        """
        Export to a ArrowSchema PyCapsule

        Unlike _export_to_c, this will not leak memory if the capsule is not used.
        """
        cdef ArrowSchema* c_schema
        capsule = alloc_c_schema(&c_schema)

        with nogil:
            check_status(ExportSchema(deref(self.schema), c_schema))

        return capsule

    @staticmethod
    def _import_from_c_capsule(schema):
        """
        Import a Schema from a ArrowSchema PyCapsule

        Parameters
        ----------
        schema : PyCapsule
            A valid PyCapsule with name 'arrow_schema' containing an
            ArrowSchema pointer.
        """
        cdef:
            ArrowSchema* c_schema

        if not PyCapsule_IsValid(schema, 'arrow_schema'):
            raise ValueError(
                "Not an ArrowSchema object"
            )
        c_schema = <ArrowSchema*> PyCapsule_GetPointer(schema, 'arrow_schema')

        with nogil:
            result = GetResultValue(ImportSchema(c_schema))

        return pyarrow_wrap_schema(result)


def unify_schemas(schemas, *, promote_options="default"):
    """
    Unify schemas by merging fields by name.

    The resulting schema will contain the union of fields from all schemas.
    Fields with the same name will be merged. Note that two fields with
    different types will fail merging by default.

    - The unified field will inherit the metadata from the schema where
        that field is first defined.
    - The first N fields in the schema will be ordered the same as the
        N fields in the first schema.

    The resulting schema will inherit its metadata from the first input
    schema.

    Parameters
    ----------
    schemas : list of Schema
        Schemas to merge into a single one.
    promote_options : str, default default
        Accepts strings "default" and "permissive".
        Default: null and only null can be unified with another type.
        Permissive: types are promoted to the greater common denominator.

    Returns
    -------
    Schema

    Raises
    ------
    ArrowInvalid :
        If any input schema contains fields with duplicate names.
        If Fields of the same name are not mergeable.
    """
    cdef:
        Schema schema
        CField.CMergeOptions c_options
        vector[shared_ptr[CSchema]] c_schemas
    for schema in schemas:
        if not isinstance(schema, Schema):
            raise TypeError("Expected Schema, got {}".format(type(schema)))
        c_schemas.push_back(pyarrow_unwrap_schema(schema))

    if promote_options == "default":
        c_options = CField.CMergeOptions.Defaults()
    elif promote_options == "permissive":
        c_options = CField.CMergeOptions.Permissive()
    else:
        raise ValueError(f"Invalid merge mode: {promote_options}")

    return pyarrow_wrap_schema(
        GetResultValue(UnifySchemas(c_schemas, c_options)))


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


def field(name, type=None, nullable=None, metadata=None):
    """
    Create a pyarrow.Field instance.

    Parameters
    ----------
    name : str or bytes
        Name of the field.
        Alternatively, you can also pass an object that implements the Arrow
        PyCapsule Protocol for schemas (has an ``__arrow_c_schema__`` method).
    type : pyarrow.DataType
        Arrow datatype of the field.
    nullable : bool, default True
        Whether the field's values are nullable.
    metadata : dict, default None
        Optional field metadata, the keys and values must be coercible to
        bytes.

    Returns
    -------
    field : pyarrow.Field

    Examples
    --------
    Create an instance of pyarrow.Field:

    >>> import pyarrow as pa
    >>> pa.field('key', pa.int32())
    pyarrow.Field<key: int32>
    >>> pa.field('key', pa.int32(), nullable=False)
    pyarrow.Field<key: int32 not null>

    >>> field = pa.field('key', pa.int32(),
    ...                  metadata={"key": "Something important"})
    >>> field
    pyarrow.Field<key: int32>
    >>> field.metadata
    {b'key': b'Something important'}

    Use the field to create a struct type:

    >>> pa.struct([field])
    StructType(struct<key: int32>)
    """
    if hasattr(name, "__arrow_c_schema__"):
        if type is not None:
            raise ValueError(
                "cannot specify 'type' when creating a Field from an ArrowSchema"
            )
        field = Field._import_from_c_capsule(name.__arrow_c_schema__())
        if metadata is not None:
            field = field.with_metadata(metadata)
        if nullable is not None:
            field = field.with_nullable(nullable)
        return field

    cdef:
        Field result = Field.__new__(Field)
        DataType _type = ensure_type(type, allow_none=False)
        shared_ptr[const CKeyValueMetadata] c_meta

    nullable = True if nullable is None else nullable

    metadata = ensure_metadata(metadata, allow_none=True)
    c_meta = pyarrow_unwrap_metadata(metadata)

    if _type.type.id() == _Type_NA and not nullable:
        raise ValueError("A null type field may not be non-nullable")

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
    Create instance of null type.

    Examples
    --------
    Create an instance of a null type:

    >>> import pyarrow as pa
    >>> pa.null()
    DataType(null)
    >>> print(pa.null())
    null

    Create a ``Field`` type with a null type and a name:

    >>> pa.field('null_field', pa.null())
    pyarrow.Field<null_field: null>
    """
    return primitive_type(_Type_NA)


def bool_():
    """
    Create instance of boolean type.

    Examples
    --------
    Create an instance of a boolean type:

    >>> import pyarrow as pa
    >>> pa.bool_()
    DataType(bool)
    >>> print(pa.bool_())
    bool

    Create a ``Field`` type with a boolean type
    and a name:

    >>> pa.field('bool_field', pa.bool_())
    pyarrow.Field<bool_field: bool>
    """
    return primitive_type(_Type_BOOL)


def uint8():
    """
    Create instance of unsigned int8 type.

    Examples
    --------
    Create an instance of unsigned int8 type:

    >>> import pyarrow as pa
    >>> pa.uint8()
    DataType(uint8)
    >>> print(pa.uint8())
    uint8

    Create an array with unsigned int8 type:

    >>> pa.array([0, 1, 2], type=pa.uint8())
    <pyarrow.lib.UInt8Array object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_UINT8)


def int8():
    """
    Create instance of signed int8 type.

    Examples
    --------
    Create an instance of int8 type:

    >>> import pyarrow as pa
    >>> pa.int8()
    DataType(int8)
    >>> print(pa.int8())
    int8

    Create an array with int8 type:

    >>> pa.array([0, 1, 2], type=pa.int8())
    <pyarrow.lib.Int8Array object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_INT8)


def uint16():
    """
    Create instance of unsigned uint16 type.

    Examples
    --------
    Create an instance of unsigned int16 type:

    >>> import pyarrow as pa
    >>> pa.uint16()
    DataType(uint16)
    >>> print(pa.uint16())
    uint16

    Create an array with unsigned int16 type:

    >>> pa.array([0, 1, 2], type=pa.uint16())
    <pyarrow.lib.UInt16Array object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_UINT16)


def int16():
    """
    Create instance of signed int16 type.

    Examples
    --------
    Create an instance of int16 type:

    >>> import pyarrow as pa
    >>> pa.int16()
    DataType(int16)
    >>> print(pa.int16())
    int16

    Create an array with int16 type:

    >>> pa.array([0, 1, 2], type=pa.int16())
    <pyarrow.lib.Int16Array object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_INT16)


def uint32():
    """
    Create instance of unsigned uint32 type.

    Examples
    --------
    Create an instance of unsigned int32 type:

    >>> import pyarrow as pa
    >>> pa.uint32()
    DataType(uint32)
    >>> print(pa.uint32())
    uint32

    Create an array with unsigned int32 type:

    >>> pa.array([0, 1, 2], type=pa.uint32())
    <pyarrow.lib.UInt32Array object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_UINT32)


def int32():
    """
    Create instance of signed int32 type.

    Examples
    --------
    Create an instance of int32 type:

    >>> import pyarrow as pa
    >>> pa.int32()
    DataType(int32)
    >>> print(pa.int32())
    int32

    Create an array with int32 type:

    >>> pa.array([0, 1, 2], type=pa.int32())
    <pyarrow.lib.Int32Array object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_INT32)


def uint64():
    """
    Create instance of unsigned uint64 type.

    Examples
    --------
    Create an instance of unsigned int64 type:

    >>> import pyarrow as pa
    >>> pa.uint64()
    DataType(uint64)
    >>> print(pa.uint64())
    uint64

    Create an array with unsigned uint64 type:

    >>> pa.array([0, 1, 2], type=pa.uint64())
    <pyarrow.lib.UInt64Array object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_UINT64)


def int64():
    """
    Create instance of signed int64 type.

    Examples
    --------
    Create an instance of int64 type:

    >>> import pyarrow as pa
    >>> pa.int64()
    DataType(int64)
    >>> print(pa.int64())
    int64

    Create an array with int64 type:

    >>> pa.array([0, 1, 2], type=pa.int64())
    <pyarrow.lib.Int64Array object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_INT64)


cdef dict _timestamp_type_cache = {}
cdef dict _time_type_cache = {}
cdef dict _duration_type_cache = {}


cdef timeunit_to_string(TimeUnit unit):
    if unit == TimeUnit_SECOND:
        return 's'
    elif unit == TimeUnit_MILLI:
        return 'ms'
    elif unit == TimeUnit_MICRO:
        return 'us'
    elif unit == TimeUnit_NANO:
        return 'ns'


cdef TimeUnit string_to_timeunit(unit) except *:
    if unit == 's':
        return TimeUnit_SECOND
    elif unit == 'ms':
        return TimeUnit_MILLI
    elif unit == 'us':
        return TimeUnit_MICRO
    elif unit == 'ns':
        return TimeUnit_NANO
    else:
        raise ValueError(f"Invalid time unit: {unit!r}")


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
      name : str
        Time zone name
    """
    return frombytes(GetResultValue(TzinfoToString(<PyObject*>tz)))


def string_to_tzinfo(name):
    """
    Convert a time zone name into a time zone object.

    Supported input strings are:
    * As used in the Olson time zone database (the "tz database" or
      "tzdata"), such as "America/New_York"
    * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30

    Parameters
    ----------
      name: str
        Time zone name.

    Returns
    -------
      tz : datetime.tzinfo
        Time zone object
    """
    cdef PyObject* tz = GetResultValue(StringToTzinfo(name.encode('utf-8')))
    return PyObject_to_object(tz)


def timestamp(unit, tz=None):
    """
    Create instance of timestamp type with resolution and optional time zone.

    Parameters
    ----------
    unit : str
        one of 's' [second], 'ms' [millisecond], 'us' [microsecond], or 'ns'
        [nanosecond]
    tz : str, default None
        Time zone name. None indicates time zone naive

    Examples
    --------
    Create an instance of timestamp type:

    >>> import pyarrow as pa
    >>> pa.timestamp('us')
    TimestampType(timestamp[us])
    >>> pa.timestamp('s', tz='America/New_York')
    TimestampType(timestamp[s, tz=America/New_York])
    >>> pa.timestamp('s', tz='+07:30')
    TimestampType(timestamp[s, tz=+07:30])

    Use timestamp type when creating a scalar object:

    >>> from datetime import datetime
    >>> pa.scalar(datetime(2012, 1, 1), type=pa.timestamp('s', tz='UTC'))
    <pyarrow.TimestampScalar: '2012-01-01T00:00:00+0000'>
    >>> pa.scalar(datetime(2012, 1, 1), type=pa.timestamp('us'))
    <pyarrow.TimestampScalar: '2012-01-01T00:00:00.000000'>

    Returns
    -------
    timestamp_type : TimestampType
    """
    cdef:
        TimeUnit unit_code
        c_string c_timezone

    unit_code = string_to_timeunit(unit)

    cdef TimestampType out = TimestampType.__new__(TimestampType)

    if tz is None:
        out.init(ctimestamp(unit_code))
        if unit_code in _timestamp_type_cache:
            return _timestamp_type_cache[unit_code]
        _timestamp_type_cache[unit_code] = out
    else:
        if not isinstance(tz, (bytes, str)):
            tz = tzinfo_to_string(tz)

        c_timezone = tobytes(tz)
        out.init(ctimestamp(unit_code, c_timezone))

    return out


def time32(unit):
    """
    Create instance of 32-bit time (time of day) type with unit resolution.

    Parameters
    ----------
    unit : str
        one of 's' [second], or 'ms' [millisecond]

    Returns
    -------
    type : pyarrow.Time32Type

    Examples
    --------
    >>> import pyarrow as pa
    >>> pa.time32('s')
    Time32Type(time32[s])
    >>> pa.time32('ms')
    Time32Type(time32[ms])
    """
    cdef:
        TimeUnit unit_code
        c_string c_timezone

    if unit == 's':
        unit_code = TimeUnit_SECOND
    elif unit == 'ms':
        unit_code = TimeUnit_MILLI
    else:
        raise ValueError(f"Invalid time unit for time32: {unit!r}")

    if unit_code in _time_type_cache:
        return _time_type_cache[unit_code]

    cdef Time32Type out = Time32Type.__new__(Time32Type)

    out.init(ctime32(unit_code))
    _time_type_cache[unit_code] = out

    return out


def time64(unit):
    """
    Create instance of 64-bit time (time of day) type with unit resolution.

    Parameters
    ----------
    unit : str
        One of 'us' [microsecond], or 'ns' [nanosecond].

    Returns
    -------
    type : pyarrow.Time64Type

    Examples
    --------
    >>> import pyarrow as pa
    >>> pa.time64('us')
    Time64Type(time64[us])
    >>> pa.time64('ns')
    Time64Type(time64[ns])
    """
    cdef:
        TimeUnit unit_code
        c_string c_timezone

    if unit == 'us':
        unit_code = TimeUnit_MICRO
    elif unit == 'ns':
        unit_code = TimeUnit_NANO
    else:
        raise ValueError(f"Invalid time unit for time64: {unit!r}")

    if unit_code in _time_type_cache:
        return _time_type_cache[unit_code]

    cdef Time64Type out = Time64Type.__new__(Time64Type)

    out.init(ctime64(unit_code))
    _time_type_cache[unit_code] = out

    return out


def duration(unit):
    """
    Create instance of a duration type with unit resolution.

    Parameters
    ----------
    unit : str
        One of 's' [second], 'ms' [millisecond], 'us' [microsecond], or
        'ns' [nanosecond].

    Returns
    -------
    type : pyarrow.DurationType

    Examples
    --------
    Create an instance of duration type:

    >>> import pyarrow as pa
    >>> pa.duration('us')
    DurationType(duration[us])
    >>> pa.duration('s')
    DurationType(duration[s])

    Create an array with duration type:

    >>> pa.array([0, 1, 2], type=pa.duration('s'))
    <pyarrow.lib.DurationArray object at ...>
    [
      0,
      1,
      2
    ]
    """
    cdef:
        TimeUnit unit_code

    unit_code = string_to_timeunit(unit)

    if unit_code in _duration_type_cache:
        return _duration_type_cache[unit_code]

    cdef DurationType out = DurationType.__new__(DurationType)

    out.init(cduration(unit_code))
    _duration_type_cache[unit_code] = out

    return out


def month_day_nano_interval():
    """
    Create instance of an interval type representing months, days and
    nanoseconds between two dates.

    Examples
    --------
    Create an instance of an month_day_nano_interval type:

    >>> import pyarrow as pa
    >>> pa.month_day_nano_interval()
    DataType(month_day_nano_interval)

    Create a scalar with month_day_nano_interval type:

    >>> pa.scalar((1, 15, -30), type=pa.month_day_nano_interval())
    <pyarrow.MonthDayNanoIntervalScalar: MonthDayNano(months=1, days=15, nanoseconds=-30)>
    """
    return primitive_type(_Type_INTERVAL_MONTH_DAY_NANO)


def date32():
    """
    Create instance of 32-bit date (days since UNIX epoch 1970-01-01).

    Examples
    --------
    Create an instance of 32-bit date type:

    >>> import pyarrow as pa
    >>> pa.date32()
    DataType(date32[day])

    Create a scalar with 32-bit date type:

    >>> from datetime import date
    >>> pa.scalar(date(2012, 1, 1), type=pa.date32())
    <pyarrow.Date32Scalar: datetime.date(2012, 1, 1)>
    """
    return primitive_type(_Type_DATE32)


def date64():
    """
    Create instance of 64-bit date (milliseconds since UNIX epoch 1970-01-01).

    Examples
    --------
    Create an instance of 64-bit date type:

    >>> import pyarrow as pa
    >>> pa.date64()
    DataType(date64[ms])

    Create a scalar with 64-bit date type:

    >>> from datetime import datetime
    >>> pa.scalar(datetime(2012, 1, 1), type=pa.date64())
    <pyarrow.Date64Scalar: datetime.date(2012, 1, 1)>
    """
    return primitive_type(_Type_DATE64)


def float16():
    """
    Create half-precision floating point type.

    Examples
    --------
    Create an instance of float16 type:

    >>> import pyarrow as pa
    >>> pa.float16()
    DataType(halffloat)
    >>> print(pa.float16())
    halffloat

    Create an array with float16 type:

    >>> arr = np.array([1.5, np.nan], dtype=np.float16)
    >>> a = pa.array(arr, type=pa.float16())
    >>> a
    <pyarrow.lib.HalfFloatArray object at ...>
    [
      15872,
      32256
    ]
    >>> a.to_pylist()
    [1.5, nan]
    """
    return primitive_type(_Type_HALF_FLOAT)


def float32():
    """
    Create single-precision floating point type.

    Examples
    --------
    Create an instance of float32 type:

    >>> import pyarrow as pa
    >>> pa.float32()
    DataType(float)
    >>> print(pa.float32())
    float

    Create an array with float32 type:

    >>> pa.array([0.0, 1.0, 2.0], type=pa.float32())
    <pyarrow.lib.FloatArray object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_FLOAT)


def float64():
    """
    Create double-precision floating point type.

    Examples
    --------
    Create an instance of float64 type:

    >>> import pyarrow as pa
    >>> pa.float64()
    DataType(double)
    >>> print(pa.float64())
    double

    Create an array with float64 type:

    >>> pa.array([0.0, 1.0, 2.0], type=pa.float64())
    <pyarrow.lib.DoubleArray object at ...>
    [
      0,
      1,
      2
    ]
    """
    return primitive_type(_Type_DOUBLE)


cpdef DataType decimal128(int precision, int scale=0):
    """
    Create decimal type with precision and scale and 128-bit width.

    Arrow decimals are fixed-point decimal numbers encoded as a scaled
    integer.  The precision is the number of significant digits that the
    decimal type can represent; the scale is the number of digits after
    the decimal point (note the scale can be negative).

    As an example, ``decimal128(7, 3)`` can exactly represent the numbers
    1234.567 and -1234.567 (encoded internally as the 128-bit integers
    1234567 and -1234567, respectively), but neither 12345.67 nor 123.4567.

    ``decimal128(5, -3)`` can exactly represent the number 12345000
    (encoded internally as the 128-bit integer 12345), but neither
    123450000 nor 1234500.

    If you need a precision higher than 38 significant digits, consider
    using ``decimal256``.

    Parameters
    ----------
    precision : int
        Must be between 1 and 38
    scale : int

    Returns
    -------
    decimal_type : Decimal128Type

    Examples
    --------
    Create an instance of decimal type:

    >>> import pyarrow as pa
    >>> pa.decimal128(5, 2)
    Decimal128Type(decimal128(5, 2))

    Create an array with decimal type:

    >>> import decimal
    >>> a = decimal.Decimal('123.45')
    >>> pa.array([a], pa.decimal128(5, 2))
    <pyarrow.lib.Decimal128Array object at ...>
    [
      123.45
    ]
    """
    cdef shared_ptr[CDataType] decimal_type
    if precision < 1 or precision > 38:
        raise ValueError("precision should be between 1 and 38")
    decimal_type.reset(new CDecimal128Type(precision, scale))
    return pyarrow_wrap_data_type(decimal_type)


cpdef DataType decimal256(int precision, int scale=0):
    """
    Create decimal type with precision and scale and 256-bit width.

    Arrow decimals are fixed-point decimal numbers encoded as a scaled
    integer.  The precision is the number of significant digits that the
    decimal type can represent; the scale is the number of digits after
    the decimal point (note the scale can be negative).

    For most use cases, the maximum precision offered by ``decimal128``
    is sufficient, and it will result in a more compact and more efficient
    encoding.  ``decimal256`` is useful if you need a precision higher
    than 38 significant digits.

    Parameters
    ----------
    precision : int
        Must be between 1 and 76
    scale : int

    Returns
    -------
    decimal_type : Decimal256Type
    """
    cdef shared_ptr[CDataType] decimal_type
    if precision < 1 or precision > 76:
        raise ValueError("precision should be between 1 and 76")
    decimal_type.reset(new CDecimal256Type(precision, scale))
    return pyarrow_wrap_data_type(decimal_type)


def string():
    """
    Create UTF8 variable-length string type.

    Examples
    --------
    Create an instance of a string type:

    >>> import pyarrow as pa
    >>> pa.string()
    DataType(string)

    and use the string type to create an array:

    >>> pa.array(['foo', 'bar', 'baz'], type=pa.string())
    <pyarrow.lib.StringArray object at ...>
    [
      "foo",
      "bar",
      "baz"
    ]
    """
    return primitive_type(_Type_STRING)


def utf8():
    """
    Alias for string().

    Examples
    --------
    Create an instance of a string type:

    >>> import pyarrow as pa
    >>> pa.utf8()
    DataType(string)

    and use the string type to create an array:

    >>> pa.array(['foo', 'bar', 'baz'], type=pa.utf8())
    <pyarrow.lib.StringArray object at ...>
    [
      "foo",
      "bar",
      "baz"
    ]
    """
    return string()


def binary(int length=-1):
    """
    Create variable-length or fixed size binary type.

    Parameters
    ----------
    length : int, optional, default -1
        If length == -1 then return a variable length binary type. If length is
        greater than or equal to 0 then return a fixed size binary type of
        width `length`.

    Examples
    --------
    Create an instance of a variable-length binary type:

    >>> import pyarrow as pa
    >>> pa.binary()
    DataType(binary)

    and use the variable-length binary type to create an array:

    >>> pa.array(['foo', 'bar', 'baz'], type=pa.binary())
    <pyarrow.lib.BinaryArray object at ...>
    [
      666F6F,
      626172,
      62617A
    ]

    Create an instance of a fixed-size binary type:

    >>> pa.binary(3)
    FixedSizeBinaryType(fixed_size_binary[3])

    and use the fixed-length binary type to create an array:

    >>> pa.array(['foo', 'bar', 'baz'], type=pa.binary(3))
    <pyarrow.lib.FixedSizeBinaryArray object at ...>
    [
      666F6F,
      626172,
      62617A
    ]
    """
    if length == -1:
        return primitive_type(_Type_BINARY)

    cdef shared_ptr[CDataType] fixed_size_binary_type
    fixed_size_binary_type.reset(new CFixedSizeBinaryType(length))
    return pyarrow_wrap_data_type(fixed_size_binary_type)


def large_binary():
    """
    Create large variable-length binary type.

    This data type may not be supported by all Arrow implementations.  Unless
    you need to represent data larger than 2GB, you should prefer binary().

    Examples
    --------
    Create an instance of large variable-length binary type:

    >>> import pyarrow as pa
    >>> pa.large_binary()
    DataType(large_binary)

    and use the type to create an array:

    >>> pa.array(['foo', 'bar', 'baz'], type=pa.large_binary())
    <pyarrow.lib.LargeBinaryArray object at ...>
    [
      666F6F,
      626172,
      62617A
    ]
    """
    return primitive_type(_Type_LARGE_BINARY)


def large_string():
    """
    Create large UTF8 variable-length string type.

    This data type may not be supported by all Arrow implementations.  Unless
    you need to represent data larger than 2GB, you should prefer string().

    Examples
    --------
    Create an instance of large UTF8 variable-length binary type:

    >>> import pyarrow as pa
    >>> pa.large_string()
    DataType(large_string)

    and use the type to create an array:

    >>> pa.array(['foo', 'bar'] * 50, type=pa.large_string())
    <pyarrow.lib.LargeStringArray object at ...>
    [
      "foo",
      "bar",
      ...
      "foo",
      "bar"
    ]
    """
    return primitive_type(_Type_LARGE_STRING)


def large_utf8():
    """
    Alias for large_string().

    Examples
    --------
    Create an instance of large UTF8 variable-length binary type:

    >>> import pyarrow as pa
    >>> pa.large_utf8()
    DataType(large_string)

    and use the type to create an array:

    >>> pa.array(['foo', 'bar'] * 50, type=pa.large_utf8())
    <pyarrow.lib.LargeStringArray object at ...>
    [
      "foo",
      "bar",
      ...
      "foo",
      "bar"
    ]
    """
    return large_string()


def binary_view():
    """
    Create a variable-length binary view type.

    Examples
    --------
    Create an instance of a string type:

    >>> import pyarrow as pa
    >>> pa.binary_view()
    DataType(binary_view)
    """
    return primitive_type(_Type_BINARY_VIEW)


def string_view():
    """
    Create UTF8 variable-length string view type.

    Examples
    --------
    Create an instance of a string type:

    >>> import pyarrow as pa
    >>> pa.string_view()
    DataType(string_view)
    """
    return primitive_type(_Type_STRING_VIEW)


def list_(value_type, int list_size=-1):
    """
    Create ListType instance from child data type or field.

    Parameters
    ----------
    value_type : DataType or Field
    list_size : int, optional, default -1
        If length == -1 then return a variable length list type. If length is
        greater than or equal to 0 then return a fixed size list type.

    Returns
    -------
    list_type : DataType

    Examples
    --------
    Create an instance of ListType:

    >>> import pyarrow as pa
    >>> pa.list_(pa.string())
    ListType(list<item: string>)
    >>> pa.list_(pa.int32(), 2)
    FixedSizeListType(fixed_size_list<item: int32>[2])

    Use the ListType to create a scalar:

    >>> pa.scalar(['foo', None], type=pa.list_(pa.string(), 2))
    <pyarrow.FixedSizeListScalar: ['foo', None]>

    or an array:

    >>> pa.array([[1, 2], [3, 4]], pa.list_(pa.int32(), 2))
    <pyarrow.lib.FixedSizeListArray object at ...>
    [
      [
        1,
        2
      ],
      [
        3,
        4
      ]
    ]
    """
    cdef:
        Field _field
        shared_ptr[CDataType] list_type

    if isinstance(value_type, DataType):
        _field = field('item', value_type)
    elif isinstance(value_type, Field):
        _field = value_type
    else:
        raise TypeError('List requires DataType or Field')

    if list_size == -1:
        list_type.reset(new CListType(_field.sp_field))
    else:
        if list_size < 0:
            raise ValueError("list_size should be a positive integer")
        list_type.reset(new CFixedSizeListType(_field.sp_field, list_size))

    return pyarrow_wrap_data_type(list_type)


cpdef LargeListType large_list(value_type):
    """
    Create LargeListType instance from child data type or field.

    This data type may not be supported by all Arrow implementations.
    Unless you need to represent data larger than 2**31 elements, you should
    prefer list_().

    Parameters
    ----------
    value_type : DataType or Field

    Returns
    -------
    list_type : DataType

    Examples
    --------
    Create an instance of LargeListType:

    >>> import pyarrow as pa
    >>> pa.large_list(pa.int8())
    LargeListType(large_list<item: int8>)

    Use the LargeListType to create an array:

    >>> pa.array([[-1, 3]] * 5, type=pa.large_list(pa.int8()))
    <pyarrow.lib.LargeListArray object at ...>
    [
      [
        -1,
        3
      ],
      [
        -1,
        3
      ],
    ...
    """
    cdef:
        DataType data_type
        Field _field
        shared_ptr[CDataType] list_type
        LargeListType out = LargeListType.__new__(LargeListType)

    if isinstance(value_type, DataType):
        _field = field('item', value_type)
    elif isinstance(value_type, Field):
        _field = value_type
    else:
        raise TypeError('List requires DataType or Field')

    list_type.reset(new CLargeListType(_field.sp_field))
    out.init(list_type)
    return out


cpdef ListViewType list_view(value_type):
    """
    Create ListViewType instance from child data type or field.

    This data type may not be supported by all Arrow implementations
    because it is an alternative to the ListType.

    Parameters
    ----------
    value_type : DataType or Field

    Returns
    -------
    list_view_type : DataType

    Examples
    --------
    Create an instance of ListViewType:

    >>> import pyarrow as pa
    >>> pa.list_view(pa.string())
    ListViewType(list_view<item: string>)
    """
    cdef:
        Field _field
        shared_ptr[CDataType] list_view_type

    if isinstance(value_type, DataType):
        _field = field('item', value_type)
    elif isinstance(value_type, Field):
        _field = value_type
    else:
        raise TypeError('ListView requires DataType or Field')

    list_view_type = CMakeListViewType(_field.sp_field)
    return pyarrow_wrap_data_type(list_view_type)


cpdef LargeListViewType large_list_view(value_type):
    """
    Create LargeListViewType instance from child data type or field.

    This data type may not be supported by all Arrow implementations
    because it is an alternative to the ListType.

    Parameters
    ----------
    value_type : DataType or Field

    Returns
    -------
    list_view_type : DataType

    Examples
    --------
    Create an instance of LargeListViewType:

    >>> import pyarrow as pa
    >>> pa.large_list_view(pa.int8())
    LargeListViewType(large_list_view<item: int8>)
    """
    cdef:
        Field _field
        shared_ptr[CDataType] list_view_type

    if isinstance(value_type, DataType):
        _field = field('item', value_type)
    elif isinstance(value_type, Field):
        _field = value_type
    else:
        raise TypeError('LargeListView requires DataType or Field')

    list_view_type = CMakeLargeListViewType(_field.sp_field)
    return pyarrow_wrap_data_type(list_view_type)


cpdef MapType map_(key_type, item_type, keys_sorted=False):
    """
    Create MapType instance from key and item data types or fields.

    Parameters
    ----------
    key_type : DataType or Field
    item_type : DataType or Field
    keys_sorted : bool

    Returns
    -------
    map_type : DataType

    Examples
    --------
    Create an instance of MapType:

    >>> import pyarrow as pa
    >>> pa.map_(pa.string(), pa.int32())
    MapType(map<string, int32>)
    >>> pa.map_(pa.string(), pa.int32(), keys_sorted=True)
    MapType(map<string, int32, keys_sorted>)

    Use MapType to create an array:

    >>> data = [[{'key': 'a', 'value': 1}, {'key': 'b', 'value': 2}], [{'key': 'c', 'value': 3}]]
    >>> pa.array(data, type=pa.map_(pa.string(), pa.int32(), keys_sorted=True))
    <pyarrow.lib.MapArray object at ...>
    [
      keys:
      [
        "a",
        "b"
      ]
      values:
      [
        1,
        2
      ],
      keys:
      [
        "c"
      ]
      values:
      [
        3
      ]
    ]
    """
    cdef:
        Field _key_field
        Field _item_field
        shared_ptr[CDataType] map_type
        MapType out = MapType.__new__(MapType)

    if isinstance(key_type, Field):
        if key_type.nullable:
            raise TypeError('Map key field should be non-nullable')
        _key_field = key_type
    else:
        _key_field = field('key', ensure_type(key_type, allow_none=False),
                           nullable=False)

    if isinstance(item_type, Field):
        _item_field = item_type
    else:
        _item_field = field('value', ensure_type(item_type, allow_none=False))

    map_type.reset(new CMapType(_key_field.sp_field, _item_field.sp_field,
                                keys_sorted))
    out.init(map_type)
    return out


cpdef DictionaryType dictionary(index_type, value_type, bint ordered=False):
    """
    Dictionary (categorical, or simply encoded) type.

    Parameters
    ----------
    index_type : DataType
    value_type : DataType
    ordered : bool

    Returns
    -------
    type : DictionaryType

    Examples
    --------
    Create an instance of dictionary type:

    >>> import pyarrow as pa
    >>> pa.dictionary(pa.int64(), pa.utf8())
    DictionaryType(dictionary<values=string, indices=int64, ordered=0>)

    Use dictionary type to create an array:

    >>> pa.array(["a", "b", None, "d"], pa.dictionary(pa.int64(), pa.utf8()))
    <pyarrow.lib.DictionaryArray object at ...>
    ...
    -- dictionary:
      [
        "a",
        "b",
        "d"
      ]
    -- indices:
      [
        0,
        1,
        null,
        2
      ]
    """
    cdef:
        DataType _index_type = ensure_type(index_type, allow_none=False)
        DataType _value_type = ensure_type(value_type, allow_none=False)
        DictionaryType out = DictionaryType.__new__(DictionaryType)
        shared_ptr[CDataType] dict_type

    if _index_type.id not in {
        Type_INT8, Type_INT16, Type_INT32, Type_INT64,
        Type_UINT8, Type_UINT16, Type_UINT32, Type_UINT64,
    }:
        raise TypeError("The dictionary index type should be integer.")

    dict_type.reset(new CDictionaryType(_index_type.sp_type,
                                        _value_type.sp_type, ordered == 1))
    out.init(dict_type)
    return out


def struct(fields):
    """
    Create StructType instance from fields.

    A struct is a nested type parameterized by an ordered sequence of types
    (which can all be distinct), called its fields.

    Parameters
    ----------
    fields : iterable of Fields or tuples, or mapping of strings to DataTypes
        Each field must have a UTF8-encoded name, and these field names are
        part of the type metadata.

    Examples
    --------
    Create an instance of StructType from an iterable of tuples:

    >>> import pyarrow as pa
    >>> fields = [
    ...     ('f1', pa.int32()),
    ...     ('f2', pa.string()),
    ... ]
    >>> struct_type = pa.struct(fields)
    >>> struct_type
    StructType(struct<f1: int32, f2: string>)

    Retrieve a field from a StructType:

    >>> struct_type[0]
    pyarrow.Field<f1: int32>
    >>> struct_type['f1']
    pyarrow.Field<f1: int32>

    Create an instance of StructType from an iterable of Fields:

    >>> fields = [
    ...     pa.field('f1', pa.int32()),
    ...     pa.field('f2', pa.string(), nullable=False),
    ... ]
    >>> pa.struct(fields)
    StructType(struct<f1: int32, f2: string not null>)

    Returns
    -------
    type : DataType
    """
    cdef:
        Field py_field
        vector[shared_ptr[CField]] c_fields
        cdef shared_ptr[CDataType] struct_type

    if isinstance(fields, Mapping):
        fields = fields.items()

    for item in fields:
        if isinstance(item, tuple):
            py_field = field(*item)
        else:
            py_field = item
        c_fields.push_back(py_field.sp_field)

    struct_type.reset(new CStructType(c_fields))
    return pyarrow_wrap_data_type(struct_type)


cdef _extract_union_params(child_fields, type_codes,
                           vector[shared_ptr[CField]]* c_fields,
                           vector[int8_t]* c_type_codes):
    cdef:
        Field child_field

    for child_field in child_fields:
        c_fields[0].push_back(child_field.sp_field)

    if type_codes is not None:
        if len(type_codes) != <Py_ssize_t>(c_fields.size()):
            raise ValueError("type_codes should have the same length "
                             "as fields")
        for code in type_codes:
            c_type_codes[0].push_back(code)
    else:
        c_type_codes[0] = range(c_fields.size())


def sparse_union(child_fields, type_codes=None):
    """
    Create SparseUnionType from child fields.

    A sparse union is a nested type where each logical value is taken from
    a single child.  A buffer of 8-bit type ids indicates which child
    a given logical value is to be taken from.

    In a sparse union, each child array should have the same length as the
    union array, regardless of the actual number of union values that
    refer to it.

    Parameters
    ----------
    child_fields : sequence of Field values
        Each field must have a UTF8-encoded name, and these field names are
        part of the type metadata.
    type_codes : list of integers, default None

    Returns
    -------
    type : SparseUnionType
    """
    cdef:
        vector[shared_ptr[CField]] c_fields
        vector[int8_t] c_type_codes

    _extract_union_params(child_fields, type_codes,
                          &c_fields, &c_type_codes)

    return pyarrow_wrap_data_type(
        CMakeSparseUnionType(move(c_fields), move(c_type_codes)))


def dense_union(child_fields, type_codes=None):
    """
    Create DenseUnionType from child fields.

    A dense union is a nested type where each logical value is taken from
    a single child, at a specific offset.  A buffer of 8-bit type ids
    indicates which child a given logical value is to be taken from,
    and a buffer of 32-bit offsets indicates at which physical position
    in the given child array the logical value is to be taken from.

    Unlike a sparse union, a dense union allows encoding only the child array
    values which are actually referred to by the union array.  This is
    counterbalanced by the additional footprint of the offsets buffer, and
    the additional indirection cost when looking up values.

    Parameters
    ----------
    child_fields : sequence of Field values
        Each field must have a UTF8-encoded name, and these field names are
        part of the type metadata.
    type_codes : list of integers, default None

    Returns
    -------
    type : DenseUnionType
    """
    cdef:
        vector[shared_ptr[CField]] c_fields
        vector[int8_t] c_type_codes

    _extract_union_params(child_fields, type_codes,
                          &c_fields, &c_type_codes)

    return pyarrow_wrap_data_type(
        CMakeDenseUnionType(move(c_fields), move(c_type_codes)))


def union(child_fields, mode, type_codes=None):
    """
    Create UnionType from child fields.

    A union is a nested type where each logical value is taken from a
    single child.  A buffer of 8-bit type ids indicates which child
    a given logical value is to be taken from.

    Unions come in two flavors: sparse and dense
    (see also `pyarrow.sparse_union` and `pyarrow.dense_union`).

    Parameters
    ----------
    child_fields : sequence of Field values
        Each field must have a UTF8-encoded name, and these field names are
        part of the type metadata.
    mode : str
        Must be 'sparse' or 'dense'
    type_codes : list of integers, default None

    Returns
    -------
    type : UnionType
    """
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

    if mode == _UnionMode_SPARSE:
        return sparse_union(child_fields, type_codes)
    else:
        return dense_union(child_fields, type_codes)


def run_end_encoded(run_end_type, value_type):
    """
    Create RunEndEncodedType from run-end and value types.

    Parameters
    ----------
    run_end_type : pyarrow.DataType
        The integer type of the run_ends array. Must be 'int16', 'int32', or 'int64'.
    value_type : pyarrow.DataType
        The type of the values array.

    Returns
    -------
    type : RunEndEncodedType
    """
    cdef:
        DataType _run_end_type = ensure_type(run_end_type, allow_none=False)
        DataType _value_type = ensure_type(value_type, allow_none=False)
        shared_ptr[CDataType] ree_type

    if not _run_end_type.type.id() in [_Type_INT16, _Type_INT32, _Type_INT64]:
        raise ValueError("The run_end_type should be 'int16', 'int32', or 'int64'")
    ree_type = CMakeRunEndEncodedType(_run_end_type.sp_type, _value_type.sp_type)
    return pyarrow_wrap_data_type(ree_type)


def fixed_shape_tensor(DataType value_type, shape, dim_names=None, permutation=None):
    """
    Create instance of fixed shape tensor extension type with shape and optional
    names of tensor dimensions and indices of the desired logical
    ordering of dimensions.

    Parameters
    ----------
    value_type : DataType
        Data type of individual tensor elements.
    shape : tuple or list of integers
        The physical shape of the contained tensors.
    dim_names : tuple or list of strings, default None
        Explicit names to tensor dimensions.
    permutation : tuple or list integers, default None
        Indices of the desired ordering of the original dimensions.
        The indices contain a permutation of the values ``[0, 1, .., N-1]`` where
        N is the number of dimensions. The permutation indicates which dimension
        of the logical layout corresponds to which dimension of the physical tensor.
        For more information on this parameter see
        :ref:`fixed_shape_tensor_extension`.

    Examples
    --------
    Create an instance of fixed shape tensor extension type:

    >>> import pyarrow as pa
    >>> tensor_type = pa.fixed_shape_tensor(pa.int32(), [2, 2])
    >>> tensor_type
    FixedShapeTensorType(extension<arrow.fixed_shape_tensor[value_type=int32, shape=[2,2]]>)

    Inspect the data type:

    >>> tensor_type.value_type
    DataType(int32)
    >>> tensor_type.shape
    [2, 2]

    Create a table with fixed shape tensor extension array:

    >>> arr = [[1, 2, 3, 4], [10, 20, 30, 40], [100, 200, 300, 400]]
    >>> storage = pa.array(arr, pa.list_(pa.int32(), 4))
    >>> tensor = pa.ExtensionArray.from_storage(tensor_type, storage)
    >>> pa.table([tensor], names=["tensor_array"])
    pyarrow.Table
    tensor_array: extension<arrow.fixed_shape_tensor[value_type=int32, shape=[2,2]]>
    ----
    tensor_array: [[[1,2,3,4],[10,20,30,40],[100,200,300,400]]]

    Create an instance of fixed shape tensor extension type with names
    of tensor dimensions:

    >>> tensor_type = pa.fixed_shape_tensor(pa.int8(), (2, 2, 3),
    ...                                     dim_names=['C', 'H', 'W'])
    >>> tensor_type.dim_names
    ['C', 'H', 'W']

    Create an instance of fixed shape tensor extension type with
    permutation:

    >>> tensor_type = pa.fixed_shape_tensor(pa.int8(), (2, 2, 3),
    ...                                     permutation=[0, 2, 1])
    >>> tensor_type.permutation
    [0, 2, 1]

    Returns
    -------
    type : FixedShapeTensorType
    """

    cdef:
        vector[int64_t] c_shape
        vector[int64_t] c_permutation
        vector[c_string] c_dim_names
        shared_ptr[CDataType] c_tensor_ext_type

    assert value_type is not None
    assert shape is not None

    for i in shape:
        c_shape.push_back(i)

    if permutation is not None:
        for i in permutation:
            c_permutation.push_back(i)

    if dim_names is not None:
        for x in dim_names:
            c_dim_names.push_back(tobytes(x))

    cdef FixedShapeTensorType out = FixedShapeTensorType.__new__(FixedShapeTensorType)

    with nogil:
        c_tensor_ext_type = GetResultValue(CFixedShapeTensorType.Make(
            value_type.sp_type, c_shape, c_permutation, c_dim_names))

    out.init(c_tensor_ext_type)

    return out


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
    'binary_view': binary_view,
    'string_view': string_view,
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
    'duration[s]': duration('s'),
    'duration[ms]': duration('ms'),
    'duration[us]': duration('us'),
    'duration[ns]': duration('ns'),
    'month_day_nano_interval': month_day_nano_interval(),
}


def type_for_alias(name):
    """
    Return DataType given a string alias if one exists.

    Parameters
    ----------
    name : str
        The alias of the DataType that should be retrieved.

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


cpdef DataType ensure_type(object ty, bint allow_none=False):
    if allow_none and ty is None:
        return None
    elif isinstance(ty, DataType):
        return ty
    elif isinstance(ty, str):
        return type_for_alias(ty)
    else:
        raise TypeError('DataType expected, got {!r}'.format(type(ty)))


def schema(fields, metadata=None):
    """
    Construct pyarrow.Schema from collection of fields.

    Parameters
    ----------
    fields : iterable of Fields or tuples, or mapping of strings to DataTypes
        Can also pass an object that implements the Arrow PyCapsule Protocol
        for schemas (has an ``__arrow_c_schema__`` method).
    metadata : dict, default None
        Keys and values must be coercible to bytes.

    Examples
    --------
    Create a Schema from iterable of tuples:

    >>> import pyarrow as pa
    >>> pa.schema([
    ...     ('some_int', pa.int32()),
    ...     ('some_string', pa.string()),
    ...     pa.field('some_required_string', pa.string(), nullable=False)
    ... ])
    some_int: int32
    some_string: string
    some_required_string: string not null

    Create a Schema from iterable of Fields:

    >>> pa.schema([
    ...     pa.field('some_int', pa.int32()),
    ...     pa.field('some_string', pa.string())
    ... ])
    some_int: int32
    some_string: string

    Returns
    -------
    schema : pyarrow.Schema
    """
    cdef:
        shared_ptr[const CKeyValueMetadata] c_meta
        shared_ptr[CSchema] c_schema
        Schema result
        Field py_field
        vector[shared_ptr[CField]] c_fields

    if isinstance(fields, Mapping):
        fields = fields.items()
    elif hasattr(fields, "__arrow_c_schema__"):
        result = Schema._import_from_c_capsule(fields.__arrow_c_schema__())
        if metadata is not None:
            result = result.with_metadata(metadata)
        return result

    for item in fields:
        if isinstance(item, tuple):
            py_field = field(*item)
        else:
            py_field = item
        if py_field is None:
            raise TypeError("field or tuple expected, got None")
        c_fields.push_back(py_field.sp_field)

    metadata = ensure_metadata(metadata, allow_none=True)
    c_meta = pyarrow_unwrap_metadata(metadata)

    c_schema.reset(new CSchema(c_fields, c_meta))
    result = Schema.__new__(Schema)
    result.init_schema(c_schema)

    return result


def from_numpy_dtype(object dtype):
    """
    Convert NumPy dtype to pyarrow.DataType.

    Parameters
    ----------
    dtype : the numpy dtype to convert


    Examples
    --------
    Create a pyarrow DataType from NumPy dtype:

    >>> import pyarrow as pa
    >>> import numpy as np
    >>> pa.from_numpy_dtype(np.dtype('float16'))
    DataType(halffloat)
    >>> pa.from_numpy_dtype('U')
    DataType(string)
    >>> pa.from_numpy_dtype(bool)
    DataType(bool)
    >>> pa.from_numpy_dtype(np.str_)
    DataType(string)
    """
    dtype = np.dtype(dtype)
    return pyarrow_wrap_data_type(GetResultValue(NumPyDtypeToArrow(dtype)))


def is_boolean_value(object obj):
    """
    Check if the object is a boolean.

    Parameters
    ----------
    obj : object
        The object to check
    """
    return IsPyBool(obj)


def is_integer_value(object obj):
    """
    Check if the object is an integer.

    Parameters
    ----------
    obj : object
        The object to check
    """
    return IsPyInt(obj)


def is_float_value(object obj):
    """
    Check if the object is a float.

    Parameters
    ----------
    obj : object
        The object to check
    """
    return IsPyFloat(obj)


cdef class _ExtensionRegistryNanny(_Weakrefable):
    # Keep the registry alive until we have unregistered PyExtensionType
    cdef:
        shared_ptr[CExtensionTypeRegistry] registry

    def __cinit__(self):
        self.registry = CExtensionTypeRegistry.GetGlobalRegistry()

    def release_registry(self):
        self.registry.reset()


_registry_nanny = _ExtensionRegistryNanny()


def _register_py_extension_type():
    cdef:
        DataType storage_type
        shared_ptr[CExtensionType] cpy_ext_type
        c_string c_extension_name = tobytes("arrow.py_extension_type")

    # Make a dummy C++ ExtensionType
    storage_type = null()
    check_status(CPyExtensionType.FromClass(
        storage_type.sp_type, c_extension_name, PyExtensionType,
        &cpy_ext_type))
    check_status(
        RegisterPyExtensionType(<shared_ptr[CDataType]> cpy_ext_type))


def _unregister_py_extension_types():
    # This needs to be done explicitly before the Python interpreter is
    # finalized.  If the C++ type is destroyed later in the process
    # teardown stage, it will invoke CPython APIs such as Py_DECREF
    # with a destroyed interpreter.
    unregister_extension_type("arrow.py_extension_type")
    for ext_type in _python_extension_types_registry:
        try:
            unregister_extension_type(ext_type.extension_name)
        except KeyError:
            pass
    _registry_nanny.release_registry()


_register_py_extension_type()
atexit.register(_unregister_py_extension_types)


#
# PyCapsule export utilities
#

cdef void pycapsule_schema_deleter(object schema_capsule) noexcept:
    cdef ArrowSchema* schema = <ArrowSchema*>PyCapsule_GetPointer(
        schema_capsule, 'arrow_schema'
    )
    if schema.release != NULL:
        schema.release(schema)

    free(schema)

cdef object alloc_c_schema(ArrowSchema** c_schema):
    c_schema[0] = <ArrowSchema*> malloc(sizeof(ArrowSchema))
    # Ensure the capsule destructor doesn't call a random release pointer
    c_schema[0].release = NULL
    return PyCapsule_New(c_schema[0], 'arrow_schema', &pycapsule_schema_deleter)


cdef void pycapsule_array_deleter(object array_capsule) noexcept:
    cdef:
        ArrowArray* array
    # Do not invoke the deleter on a used/moved capsule
    array = <ArrowArray*>cpython.PyCapsule_GetPointer(
        array_capsule, 'arrow_array'
    )
    if array.release != NULL:
        array.release(array)

    free(array)

cdef object alloc_c_array(ArrowArray** c_array):
    c_array[0] = <ArrowArray*> malloc(sizeof(ArrowArray))
    # Ensure the capsule destructor doesn't call a random release pointer
    c_array[0].release = NULL
    return PyCapsule_New(c_array[0], 'arrow_array', &pycapsule_array_deleter)


cdef void pycapsule_stream_deleter(object stream_capsule) noexcept:
    cdef:
        ArrowArrayStream* stream
    # Do not invoke the deleter on a used/moved capsule
    stream = <ArrowArrayStream*>PyCapsule_GetPointer(
        stream_capsule, 'arrow_array_stream'
    )
    if stream.release != NULL:
        stream.release(stream)

    free(stream)

cdef object alloc_c_stream(ArrowArrayStream** c_stream):
    c_stream[0] = <ArrowArrayStream*> malloc(sizeof(ArrowArrayStream))
    # Ensure the capsule destructor doesn't call a random release pointer
    c_stream[0].release = NULL
    return PyCapsule_New(c_stream[0], 'arrow_array_stream', &pycapsule_stream_deleter)
