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

import re

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
    Base type for Apache Arrow data type instances. Wraps C++ arrow::DataType
    """
    def __cinit__(self):
        pass

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly, use public "
                        "functions like pyarrow.int64, pyarrow.list_, etc. "
                        "instead.".format(self.__class__.__name__))

    cdef void init(self, const shared_ptr[CDataType]& type):
        self.sp_type = type
        self.type = type.get()
        self.pep3118_format = _datatype_to_pep3118(self.type)

    property id:

        def __get__(self):
            return self.type.id()

    property bit_width:

        def __get__(self):
            cdef _CFixedWidthTypePtr ty
            ty = dynamic_cast[_CFixedWidthTypePtr](self.type)
            if ty == nullptr:
                raise ValueError("Non-fixed width type")
            return ty.bit_width()

    def __str__(self):
        if self.type is NULL:
            raise TypeError(
                '{} is incomplete. The correct way to construct types is '
                'through public API functions named '
                'pyarrow.int64, pyarrow.list_, etc.'.format(
                    type(self).__name__
                )
            )
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
            return False

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
        Return the NumPy dtype that would be used for storing this
        """
        cdef Type type_id = self.type.id()
        if type_id in _pandas_type_map:
            return _pandas_type_map[type_id]
        else:
            raise NotImplementedError(str(self))


cdef class DictionaryType(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.dict_type = <const CDictionaryType*> type.get()

    def __reduce__(self):
        return dictionary, (self.index_type, self.dictionary, self.ordered)

    property ordered:

        def __get__(self):
            return self.dict_type.ordered()

    property index_type:

        def __get__(self):
            return pyarrow_wrap_data_type(self.dict_type.index_type())

    property dictionary:

        def __get__(self):
            return pyarrow_wrap_array(self.dict_type.dictionary())


cdef class ListType(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.list_type = <const CListType*> type.get()

    def __reduce__(self):
        return list_, (self.value_type,)

    property value_type:

        def __get__(self):
            return pyarrow_wrap_data_type(self.list_type.value_type())


cdef class StructType(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)

    def __len__(self):
        return self.type.num_children()

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __getitem__(self, i):
        cdef int index = <int> _normalize_index(i, self.num_children)
        return pyarrow_wrap_field(self.type.child(index))

    property num_children:

        def __get__(self):
            return self.type.num_children()

    def __reduce__(self):
        return struct, (list(self),)


cdef class UnionType(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)

    property num_children:

        def __get__(self):
            return self.type.num_children()

    property mode:

        def __get__(self):
            cdef CUnionType* type = <CUnionType*> self.sp_type.get()
            cdef int mode = type.mode()
            if mode == _UnionMode_DENSE:
                return 'dense'
            if mode == _UnionMode_SPARSE:
                return 'sparse'
            assert 0

    def __len__(self):
        return self.type.num_children()

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __getitem__(self, i):
        cdef int index = <int> _normalize_index(i, self.num_children)
        return pyarrow_wrap_field(self.type.child(index))

    def __reduce__(self):
        return union, (list(self), self.mode)


cdef class TimestampType(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.ts_type = <const CTimestampType*> type.get()

    property unit:

        def __get__(self):
            return timeunit_to_string(self.ts_type.unit())

    property tz:

        def __get__(self):
            if self.ts_type.timezone().size() > 0:
                return frombytes(self.ts_type.timezone())
            else:
                return None

    def to_pandas_dtype(self):
        """
        Return the NumPy dtype that would be used for storing this
        """
        if self.tz is None:
            return _pandas_type_map[_Type_TIMESTAMP]
        else:
            # Return DatetimeTZ
            return pdcompat.make_datetimetz(self.tz)

    def __reduce__(self):
        return timestamp, (self.unit, self.tz)


cdef class Time32Type(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.time_type = <const CTime32Type*> type.get()

    property unit:

        def __get__(self):
            return timeunit_to_string(self.time_type.unit())


cdef class Time64Type(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.time_type = <const CTime64Type*> type.get()

    property unit:

        def __get__(self):
            return timeunit_to_string(self.time_type.unit())


cdef class FixedSizeBinaryType(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.fixed_size_binary_type = (
            <const CFixedSizeBinaryType*> type.get())

    def __reduce__(self):
        return binary, (self.byte_width,)

    property byte_width:

        def __get__(self):
            return self.fixed_size_binary_type.byte_width()


cdef class Decimal128Type(FixedSizeBinaryType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        FixedSizeBinaryType.init(self, type)
        self.decimal128_type = <const CDecimal128Type*> type.get()

    def __reduce__(self):
        return decimal128, (self.precision, self.scale)

    property precision:

        def __get__(self):
            return self.decimal128_type.precision()

    property scale:

        def __get__(self):
            return self.decimal128_type.scale()


cdef class Field:
    """
    Represents a named field, with a data type, nullability, and optional
    metadata

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
        check_metadata : boolean, default True
            Key/value metadata must be equal too

        Returns
        -------
        is_equal : boolean
        """
        return self.field.Equals(deref(other.field))

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return False

    def __reduce__(self):
        return field, (self.name, self.type, self.nullable, self.metadata)

    def __str__(self):
        return 'pyarrow.Field<{0}>'.format(frombytes(self.field.ToString()))

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.field.name(), self.type, self.field.nullable()))

    property nullable:

        def __get__(self):
            return self.field.nullable()

    property name:

        def __get__(self):
            return frombytes(self.field.name())

    property metadata:

        def __get__(self):
            cdef shared_ptr[const CKeyValueMetadata] metadata = (
                self.field.metadata())
            return box_metadata(metadata.get())

    def add_metadata(self, dict metadata):
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
        cdef shared_ptr[CKeyValueMetadata] c_meta
        convert_metadata(metadata, &c_meta)

        cdef shared_ptr[CField] new_field
        with nogil:
            new_field = self.field.AddMetadata(c_meta)

        return pyarrow_wrap_field(new_field)

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

    property names:

        def __get__(self):
            cdef int i
            result = []
            for i in range(self.schema.num_fields()):
                name = frombytes(self.schema.field(i).get().name())
                result.append(name)
            return result

    property metadata:

        def __get__(self):
            cdef shared_ptr[const CKeyValueMetadata] metadata = (
                self.schema.metadata())
            return box_metadata(metadata.get())

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return False

    def equals(self, other, bint check_metadata=True):
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
        cdef Schema _other = other
        return self.sp_schema.get().Equals(deref(_other.schema),
                                           check_metadata)

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
        return pyarrow_wrap_field(self.schema.GetFieldByName(tobytes(name)))

    def get_field_index(self, name):
        return self.schema.GetFieldIndex(tobytes(name))

    def append(self, Field field):
        """
        Append a field at the end of the schema.

        Parameters
        ----------
        field: Field

        Returns
        -------
        schema: Schema
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

    def add_metadata(self, dict metadata):
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
        cdef shared_ptr[CKeyValueMetadata] c_meta
        convert_metadata(metadata, &c_meta)

        cdef shared_ptr[CSchema] new_schema
        with nogil:
            new_schema = self.schema.AddMetadata(c_meta)

        return pyarrow_wrap_schema(new_schema)

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
        """
        cdef:
            shared_ptr[CBuffer] buffer
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            check_status(SerializeSchema(deref(self.schema),
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


cdef dict box_metadata(const CKeyValueMetadata* metadata):
    cdef unordered_map[c_string, c_string] result
    if metadata != nullptr:
        metadata.ToUnorderedMap(&result)
        return result
    else:
        return None


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

cdef int convert_metadata(dict metadata,
                          shared_ptr[CKeyValueMetadata]* out) except -1:
    cdef:
        shared_ptr[CKeyValueMetadata] meta = (
            make_shared[CKeyValueMetadata]())
        c_string key, value

    for py_key, py_value in metadata.items():
        key = tobytes(py_key)
        value = tobytes(py_value)
        meta.get().Append(key, value)
    out[0] = meta
    return 0


def field(name, type, bint nullable=True, dict metadata=None):
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
        shared_ptr[CKeyValueMetadata] c_meta
        Field result = Field.__new__(Field)
        DataType _type

    if metadata is not None:
        convert_metadata(metadata, &c_meta)

    _type = _as_type(type)

    result.sp_field.reset(new CField(tobytes(name), _type.sp_type,
                                     nullable == 1, c_meta))
    result.field = result.sp_field.get()
    result.type = _type
    return result


cdef _as_type(type):
    if isinstance(type, DataType):
        return type
    if not isinstance(type, six.string_types):
        raise TypeError(type)
    return type_for_alias(type)


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
    Create instance of boolean type
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
    if tz.zone is None:
        sign = '+' if tz._minutes >= 0 else '-'
        hours, minutes = divmod(abs(tz._minutes), 60)
        return '{}{:02d}:{:02d}'.format(sign, hours, minutes)
    else:
        return tz.zone


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

    if unit == "s":
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

    if unit == "us":
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
    decimal_type.reset(new CDecimal128Type(precision, scale))
    return pyarrow_wrap_data_type(decimal_type)


def string():
    """
    Create UTF8 variable-length string type
    """
    return primitive_type(_Type_STRING)


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
        Field field
        shared_ptr[CDataType] list_type
        ListType out = ListType.__new__(ListType)

    if isinstance(value_type, DataType):
        list_type.reset(new CListType((<DataType> value_type).sp_type))
    elif isinstance(value_type, Field):
        list_type.reset(new CListType((<Field> value_type).sp_field))
    else:
        raise ValueError('List requires DataType or Field')

    out.init(list_type)
    return out


cpdef DictionaryType dictionary(DataType index_type, Array dict_values,
                                bint ordered=False):
    """
    Dictionary (categorical, or simply encoded) type

    Parameters
    ----------
    index_type : DataType
    dictionary : Array

    Returns
    -------
    type : DictionaryType
    """
    cdef DictionaryType out = DictionaryType.__new__(DictionaryType)
    cdef shared_ptr[CDataType] dict_type
    dict_type.reset(new CDictionaryType(index_type.sp_type,
                                        dict_values.sp_array,
                                        ordered == 1))
    out.init(dict_type)
    return out


def struct(fields):
    """
    Create StructType instance from fields

    Parameters
    ----------
    fields : sequence of Field values

    Examples
    --------
    ::

        import pyarrow as pa
        fields = [
            pa.field('f1', pa.int32()),
            pa.field('f2', pa.string())
        ]
        struct_type = pa.struct(fields)

    Returns
    -------
    type : DataType
    """
    cdef:
        Field field
        vector[shared_ptr[CField]] c_fields
        cdef shared_ptr[CDataType] struct_type

    for field in fields:
        c_fields.push_back(field.sp_field)

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


def schema(fields, dict metadata=None):
    """
    Construct pyarrow.Schema from collection of fields

    Parameters
    ----------
    field : list or iterable
    metadata : dict, default None
        Keys and values must be coercible to bytes

    Returns
    -------
    schema : pyarrow.Schema
    """
    cdef:
        shared_ptr[CKeyValueMetadata] c_meta
        shared_ptr[CSchema] c_schema
        Schema result
        Field field
        vector[shared_ptr[CField]] c_fields

    for i, field in enumerate(fields):
        c_fields.push_back(field.sp_field)

    if metadata is not None:
        convert_metadata(metadata, &c_meta)

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
