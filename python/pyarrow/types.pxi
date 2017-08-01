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


cdef class DataType:
    """
    Base type for Apache Arrow data type instances. Wraps C++ arrow::DataType
    """
    def __cinit__(self):
        pass

    cdef void init(self, const shared_ptr[CDataType]& type):
        self.sp_type = type
        self.type = type.get()

    property id:

        def __get__(self):
            return self.type.id()

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

    def __repr__(self):
        return '{0.__class__.__name__}({0})'.format(self)

    def __richcmp__(DataType self, DataType other, int op):
        if op == cp.Py_EQ:
            return self.type.Equals(deref(other.type))
        elif op == cp.Py_NE:
            return not self.type.Equals(deref(other.type))
        else:
            raise TypeError('Invalid comparison')

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

    property ordered:

        def __get__(self):
            return self.dict_type.ordered()


cdef class ListType(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.list_type = <const CListType*> type.get()

    property value_type:

        def __get__(self):
            return pyarrow_wrap_data_type(self.list_type.value_type())


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

    property byte_width:

        def __get__(self):
            return self.fixed_size_binary_type.byte_width()


cdef class DecimalType(FixedSizeBinaryType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.decimal_type = <const CDecimalType*> type.get()

    property precision:

        def __get__(self):
            return self.decimal_type.precision()

    property scale:

        def __get__(self):
            return self.decimal_type.scale()


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

    cdef void init(self, const shared_ptr[CField]& field):
        self.sp_field = field
        self.field = field.get()
        self.type = pyarrow_wrap_data_type(field.get().type())

    def equals(self, Field other):
        """
        Test if this field is equal to the other
        """
        return self.field.Equals(deref(other.field))

    def __str__(self):
        self._check_null()
        return 'pyarrow.Field<{0}>'.format(frombytes(self.field.ToString()))

    def __repr__(self):
        return self.__str__()

    property nullable:

        def __get__(self):
            self._check_null()
            return self.field.nullable()

    property name:

        def __get__(self):
            self._check_null()
            return frombytes(self.field.name())

    property metadata:

        def __get__(self):
            self._check_null()
            return box_metadata(self.field.metadata().get())

    def _check_null(self):
        if self.field == NULL:
            raise ReferenceError(
                'Field not initialized (references NULL pointer)')

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


cdef class Schema:

    def __cinit__(self):
        pass

    def __len__(self):
        return self.schema.num_fields()

    def __getitem__(self, int i):

        cdef:
            Field result = Field()
            int num_fields = self.schema.num_fields()
            int index

        if not -num_fields <= i < num_fields:
            raise IndexError(
                'Schema field index {:d} is out of range'.format(i)
            )

        index = i if i >= 0 else num_fields + i
        assert index >= 0

        result.init(self.schema.field(index))
        result.type = pyarrow_wrap_data_type(result.field.type())

        return result

    cdef void init(self, const vector[shared_ptr[CField]]& fields):
        self.schema = new CSchema(fields)
        self.sp_schema.reset(self.schema)

    cdef void init_schema(self, const shared_ptr[CSchema]& schema):
        self.schema = schema.get()
        self.sp_schema = schema

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
            return box_metadata(self.schema.metadata().get())

    def equals(self, other):
        """
        Test if this schema is equal to the other
        """
        cdef Schema _other
        _other = other

        return self.sp_schema.get().Equals(deref(_other.schema))

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
        return frombytes(self.schema.ToString())

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

    cdef DataType out = DataType()
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


def field(name, DataType type, bint nullable=True, dict metadata=None):
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
        Field result = Field()

    if metadata is not None:
        convert_metadata(metadata, &c_meta)

    result.sp_field.reset(new CField(tobytes(name), type.sp_type,
                                     nullable == 1, c_meta))
    result.field = result.sp_field.get()
    result.type = type
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

    cdef TimestampType out = TimestampType()

    if tz is None:
        out.init(ctimestamp(unit_code))
        if unit_code in _timestamp_type_cache:
            return _timestamp_type_cache[unit_code]
        _timestamp_type_cache[unit_code] = out
    else:
        if not isinstance(tz, six.string_types):
            tz = tz.zone

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

    cdef Time32Type out
    if unit_code in _time_type_cache:
        return _time_type_cache[unit_code]
    else:
        out = Time32Type()
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

    cdef Time64Type out
    if unit_code in _time_type_cache:
        return _time_type_cache[unit_code]
    else:
        out = Time64Type()
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


cpdef DataType decimal(int precision, int scale=0):
    """
    Create decimal type with precision and scale

    Parameters
    ----------
    precision : int
    scale : int

    Returns
    -------
    decimal_type : DecimalType
    """
    cdef shared_ptr[CDataType] decimal_type
    decimal_type.reset(new CDecimalType(precision, scale))
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
        ListType out = ListType()

    if isinstance(value_type, DataType):
        list_type.reset(new CListType((<DataType> value_type).sp_type))
    elif isinstance(value_type, Field):
        list_type.reset(new CListType((<Field> value_type).sp_field))
    else:
        raise ValueError('List requires DataType or Field')

    out.init(list_type)
    return out


cpdef DictionaryType dictionary(DataType index_type, Array dictionary,
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
    cdef DictionaryType out = DictionaryType()
    cdef shared_ptr[CDataType] dict_type
    dict_type.reset(new CDictionaryType(index_type.sp_type,
                                        dictionary.sp_array,
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


def schema(fields):
    """
    Construct pyarrow.Schema from collection of fields

    Parameters
    ----------
    field : list or iterable

    Returns
    -------
    schema : pyarrow.Schema
    """
    cdef:
        Schema result
        Field field
        vector[shared_ptr[CField]] c_fields

    for i, field in enumerate(fields):
        c_fields.push_back(field.sp_field)

    result = Schema()
    result.init(c_fields)
    return result


def from_numpy_dtype(object dtype):
    """
    Convert NumPy dtype to pyarrow.DataType
    """
    cdef shared_ptr[CDataType] c_type
    with nogil:
        check_status(NumPyDtypeToArrow(dtype, &c_type))

    return pyarrow_wrap_data_type(c_type)
