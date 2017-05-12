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

from pyarrow.includes.libarrow cimport *

# These are imprecise because the type (in pandas 0.x) depends on the presence
# of nulls
_pandas_type_map = {
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
    _Type_LIST: np.object_
}

cdef class DataType:

    def __cinit__(self):
        pass

    cdef void init(self, const shared_ptr[CDataType]& type):
        self.sp_type = type
        self.type = type.get()

    def __str__(self):
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

    cdef init(self, const shared_ptr[CField]& field):
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
            check_status(self.field.AddMetadata(c_meta, &new_field))

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

    def __getitem__(self, i):
        if i < 0 or i >= len(self):
            raise IndexError("{0} is out of bounds".format(i))

        cdef Field result = Field()
        result.init(self.schema.field(i))
        result.type = pyarrow_wrap_data_type(result.field.type())

        return result

    cdef init(self, const vector[shared_ptr[CField]]& fields):
        self.schema = new CSchema(fields)
        self.sp_schema.reset(self.schema)

    cdef init_schema(self, const shared_ptr[CSchema]& schema):
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
            check_status(self.schema.AddMetadata(c_meta, &new_schema))

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


cdef box_metadata(const CKeyValueMetadata* metadata):
    cdef unordered_map[c_string, c_string] result
    if metadata != NULL:
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

#------------------------------------------------------------
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
                                     nullable, c_meta))
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
    _Type_DATE64,
    _Type_HALF_FLOAT,
    _Type_FLOAT,
    _Type_DOUBLE])


def null():
    return primitive_type(_Type_NA)


def bool_():
    return primitive_type(_Type_BOOL)


def uint8():
    return primitive_type(_Type_UINT8)


def int8():
    return primitive_type(_Type_INT8)


def uint16():
    return primitive_type(_Type_UINT16)


def int16():
    return primitive_type(_Type_INT16)


def uint32():
    return primitive_type(_Type_UINT32)


def int32():
    return primitive_type(_Type_INT32)


def uint64():
    return primitive_type(_Type_UINT64)


def int64():
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


def timestamp(unit_str, tz=None):
    cdef:
        TimeUnit unit
        c_string c_timezone

    if unit_str == "s":
        unit = TimeUnit_SECOND
    elif unit_str == 'ms':
        unit = TimeUnit_MILLI
    elif unit_str == 'us':
        unit = TimeUnit_MICRO
    elif unit_str == 'ns':
        unit = TimeUnit_NANO
    else:
        raise ValueError('Invalid TimeUnit string')

    cdef TimestampType out = TimestampType()

    if tz is None:
        out.init(ctimestamp(unit))
        if unit in _timestamp_type_cache:
            return _timestamp_type_cache[unit]
        _timestamp_type_cache[unit] = out
    else:
        if not isinstance(tz, six.string_types):
            tz = tz.zone

        c_timezone = tobytes(tz)
        out.init(ctimestamp(unit, c_timezone))

    return out


def time32(unit_str):
    cdef:
        TimeUnit unit
        c_string c_timezone

    if unit_str == "s":
        unit = TimeUnit_SECOND
    elif unit_str == 'ms':
        unit = TimeUnit_MILLI
    else:
        raise ValueError('Invalid TimeUnit for time32: {}'.format(unit_str))

    cdef Time32Type out
    if unit in _time_type_cache:
        return _time_type_cache[unit]
    else:
        out = Time32Type()
        out.init(ctime32(unit))
        _time_type_cache[unit] = out
        return out


def time64(unit_str):
    cdef:
        TimeUnit unit
        c_string c_timezone

    if unit_str == "us":
        unit = TimeUnit_MICRO
    elif unit_str == 'ns':
        unit = TimeUnit_NANO
    else:
        raise ValueError('Invalid TimeUnit for time64: {}'.format(unit_str))

    cdef Time64Type out
    if unit in _time_type_cache:
        return _time_type_cache[unit]
    else:
        out = Time64Type()
        out.init(ctime64(unit))
        _time_type_cache[unit] = out
        return out


def date32():
    return primitive_type(_Type_DATE32)


def date64():
    return primitive_type(_Type_DATE64)


def float16():
    return primitive_type(_Type_HALF_FLOAT)


def float32():
    return primitive_type(_Type_FLOAT)


def float64():
    return primitive_type(_Type_DOUBLE)


cpdef DataType decimal(int precision, int scale=0):
    cdef shared_ptr[CDataType] decimal_type
    decimal_type.reset(new CDecimalType(precision, scale))
    return pyarrow_wrap_data_type(decimal_type)


def string():
    """
    UTF8 string
    """
    return primitive_type(_Type_STRING)


def binary(int length=-1):
    """Binary (PyBytes-like) type

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


def list_(DataType value_type):
    cdef DataType out = DataType()
    cdef shared_ptr[CDataType] list_type
    list_type.reset(new CListType(value_type.sp_type))
    out.init(list_type)
    return out


def dictionary(DataType index_type, Array dictionary):
    """
    Dictionary (categorical, or simply encoded) type
    """
    cdef DictionaryType out = DictionaryType()
    cdef shared_ptr[CDataType] dict_type
    dict_type.reset(new CDictionaryType(index_type.sp_type,
                                        dictionary.sp_array))
    out.init(dict_type)
    return out


def struct(fields):
    """

    """
    cdef:
        DataType out = DataType()
        Field field
        vector[shared_ptr[CField]] c_fields
        cdef shared_ptr[CDataType] struct_type

    for field in fields:
        c_fields.push_back(field.sp_field)

    struct_type.reset(new CStructType(c_fields))
    out.init(struct_type)
    return out


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


NA = None


cdef class NAType(Scalar):

    def __cinit__(self):
        global NA
        if NA is not None:
            raise Exception('Cannot create multiple NAType instances')

        self.type = null()

    def __repr__(self):
        return 'NA'

    def as_py(self):
        return None


NA = NAType()


cdef class ArrayValue(Scalar):

    cdef void init(self, DataType type, const shared_ptr[CArray]& sp_array,
                   int64_t index):
        self.type = type
        self.index = index
        self._set_array(sp_array)

    cdef void _set_array(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array

    def __repr__(self):
        if hasattr(self, 'as_py'):
            return repr(self.as_py())
        else:
            return super(Scalar, self).__repr__()


cdef class BooleanValue(ArrayValue):

    def as_py(self):
        cdef CBooleanArray* ap = <CBooleanArray*> self.sp_array.get()
        return ap.Value(self.index)


cdef class Int8Value(ArrayValue):

    def as_py(self):
        cdef CInt8Array* ap = <CInt8Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class UInt8Value(ArrayValue):

    def as_py(self):
        cdef CUInt8Array* ap = <CUInt8Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class Int16Value(ArrayValue):

    def as_py(self):
        cdef CInt16Array* ap = <CInt16Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class UInt16Value(ArrayValue):

    def as_py(self):
        cdef CUInt16Array* ap = <CUInt16Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class Int32Value(ArrayValue):

    def as_py(self):
        cdef CInt32Array* ap = <CInt32Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class UInt32Value(ArrayValue):

    def as_py(self):
        cdef CUInt32Array* ap = <CUInt32Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class Int64Value(ArrayValue):

    def as_py(self):
        cdef CInt64Array* ap = <CInt64Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class UInt64Value(ArrayValue):

    def as_py(self):
        cdef CUInt64Array* ap = <CUInt64Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class Date32Value(ArrayValue):

    def as_py(self):
        cdef CDate32Array* ap = <CDate32Array*> self.sp_array.get()

        # Shift to seconds since epoch
        return datetime.datetime.utcfromtimestamp(
            int(ap.Value(self.index)) * 86400).date()


cdef class Date64Value(ArrayValue):

    def as_py(self):
        cdef CDate64Array* ap = <CDate64Array*> self.sp_array.get()
        return datetime.datetime.utcfromtimestamp(
            ap.Value(self.index) / 1000).date()


cdef class TimestampValue(ArrayValue):

    def as_py(self):
        cdef:
            CTimestampArray* ap = <CTimestampArray*> self.sp_array.get()
            CTimestampType* dtype = <CTimestampType*>ap.type().get()
            int64_t val = ap.Value(self.index)

        timezone = None
        tzinfo = None
        if dtype.timezone().size() > 0:
            timezone = frombytes(dtype.timezone())
            import pytz
            tzinfo = pytz.timezone(timezone)

        try:
            pd = _pandas()
            if dtype.unit() == TimeUnit_SECOND:
                val = val * 1000000000
            elif dtype.unit() == TimeUnit_MILLI:
                val = val * 1000000
            elif dtype.unit() == TimeUnit_MICRO:
                val = val * 1000
            return pd.Timestamp(val, tz=tzinfo)
        except ImportError:
            if dtype.unit() == TimeUnit_SECOND:
                result = datetime.datetime.utcfromtimestamp(val)
            elif dtype.unit() == TimeUnit_MILLI:
                result = datetime.datetime.utcfromtimestamp(float(val) / 1000)
            elif dtype.unit() == TimeUnit_MICRO:
                result = datetime.datetime.utcfromtimestamp(
                    float(val) / 1000000)
            else:
                # TimeUnit_NANO
                raise NotImplementedError("Cannot convert nanosecond "
                                          "timestamps without pandas")
            if timezone is not None:
                result = result.replace(tzinfo=tzinfo)
            return result


cdef class FloatValue(ArrayValue):

    def as_py(self):
        cdef CFloatArray* ap = <CFloatArray*> self.sp_array.get()
        return ap.Value(self.index)


cdef class DoubleValue(ArrayValue):

    def as_py(self):
        cdef CDoubleArray* ap = <CDoubleArray*> self.sp_array.get()
        return ap.Value(self.index)


cdef class DecimalValue(ArrayValue):

    def as_py(self):
        cdef:
            CDecimalArray* ap = <CDecimalArray*> self.sp_array.get()
            c_string s = ap.FormatValue(self.index)
        return _pydecimal.Decimal(s.decode('utf8'))


cdef class StringValue(ArrayValue):

    def as_py(self):
        cdef CStringArray* ap = <CStringArray*> self.sp_array.get()
        return ap.GetString(self.index).decode('utf-8')


cdef class BinaryValue(ArrayValue):

    def as_py(self):
        cdef:
            const uint8_t* ptr
            int32_t length
            CBinaryArray* ap = <CBinaryArray*> self.sp_array.get()

        ptr = ap.GetValue(self.index, &length)
        return cp.PyBytes_FromStringAndSize(<const char*>(ptr), length)


cdef class ListValue(ArrayValue):

    def __len__(self):
        return self.ap.value_length(self.index)

    def __getitem__(self, i):
        return self.getitem(i)

    def __iter__(self):
        for i in range(len(self)):
            yield self.getitem(i)
        raise StopIteration

    cdef void _set_array(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array
        self.ap = <CListArray*> sp_array.get()
        self.value_type = pyarrow_wrap_data_type(self.ap.value_type())

    cdef getitem(self, int64_t i):
        cdef int64_t j = self.ap.value_offset(self.index) + i
        return box_scalar(self.value_type, self.ap.values(), j)

    def as_py(self):
        cdef:
            int64_t j
            list result = []

        for j in range(len(self)):
            result.append(self.getitem(j).as_py())

        return result


cdef class FixedSizeBinaryValue(ArrayValue):

    def as_py(self):
        cdef:
            CFixedSizeBinaryArray* ap
            CFixedSizeBinaryType* ap_type
            int32_t length
            const char* data
        ap = <CFixedSizeBinaryArray*> self.sp_array.get()
        ap_type = <CFixedSizeBinaryType*> ap.type().get()
        length = ap_type.byte_width()
        data = <const char*> ap.GetValue(self.index)
        return cp.PyBytes_FromStringAndSize(data, length)



cdef dict _scalar_classes = {
    _Type_BOOL: BooleanValue,
    _Type_UINT8: Int8Value,
    _Type_UINT16: Int16Value,
    _Type_UINT32: Int32Value,
    _Type_UINT64: Int64Value,
    _Type_INT8: Int8Value,
    _Type_INT16: Int16Value,
    _Type_INT32: Int32Value,
    _Type_INT64: Int64Value,
    _Type_DATE32: Date32Value,
    _Type_DATE64: Date64Value,
    _Type_TIMESTAMP: TimestampValue,
    _Type_FLOAT: FloatValue,
    _Type_DOUBLE: DoubleValue,
    _Type_LIST: ListValue,
    _Type_BINARY: BinaryValue,
    _Type_STRING: StringValue,
    _Type_FIXED_SIZE_BINARY: FixedSizeBinaryValue,
    _Type_DECIMAL: DecimalValue,
}

cdef object box_scalar(DataType type, const shared_ptr[CArray]& sp_array,
                       int64_t index):
    cdef ArrayValue val
    if type.type.id() == _Type_NA:
        return NA
    elif sp_array.get().IsNull(index):
        return NA
    else:
        val = _scalar_classes[type.type.id()]()
        val.init(type, sp_array, index)
        return val


cdef maybe_coerce_datetime64(values, dtype, DataType type,
                             timestamps_to_ms=False):

    from pyarrow.compat import DatetimeTZDtype

    if values.dtype.type != np.datetime64:
        return values, type

    coerce_ms = timestamps_to_ms and values.dtype != 'datetime64[ms]'

    if coerce_ms:
        values = values.astype('datetime64[ms]')

    if isinstance(dtype, DatetimeTZDtype):
        tz = dtype.tz
        unit = 'ms' if coerce_ms else dtype.unit
        type = timestamp(unit, tz)
    elif type is None:
        # Trust the NumPy dtype
        type = from_numpy_dtype(values.dtype)

    return values, type



def array(object sequence, DataType type=None, MemoryPool memory_pool=None):
    """
    Create pyarrow.Array instance from a Python sequence

    Parameters
    ----------
    sequence : sequence-like object of Python objects
    type : pyarrow.DataType, optional
        If not passed, will be inferred from the data
    memory_pool : pyarrow.MemoryPool, optional
        If not passed, will allocate memory from the currently-set default
        memory pool

    Returns
    -------
    array : pyarrow.Array
    """
    cdef:
       shared_ptr[CArray] sp_array
       CMemoryPool* pool

    pool = maybe_unbox_memory_pool(memory_pool)
    if type is None:
        check_status(ConvertPySequence(sequence, pool, &sp_array))
    else:
        check_status(
            ConvertPySequence(
                sequence, pool, &sp_array, type.sp_type
            )
        )

    return pyarrow_wrap_array(sp_array)



cdef class Array:

    cdef init(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array
        self.ap = sp_array.get()
        self.type = pyarrow_wrap_data_type(self.sp_array.get().type())

    @staticmethod
    def from_pandas(obj, mask=None, DataType type=None,
                    timestamps_to_ms=False,
                    MemoryPool memory_pool=None):
        """
        Convert pandas.Series to an Arrow Array.

        Parameters
        ----------
        series : pandas.Series or numpy.ndarray

        mask : pandas.Series or numpy.ndarray, optional
            boolean mask if the object is valid or null

        type : pyarrow.DataType
            Explicit type to attempt to coerce to

        timestamps_to_ms : bool, optional
            Convert datetime columns to ms resolution. This is needed for
            compatibility with other functionality like Parquet I/O which
            only supports milliseconds.

        memory_pool: MemoryPool, optional
            Specific memory pool to use to allocate the resulting Arrow array.

        Notes
        -----
        Localized timestamps will currently be returned as UTC (pandas's native
        representation).  Timezone-naive data will be implicitly interpreted as
        UTC.

        Examples
        --------

        >>> import pandas as pd
        >>> import pyarrow as pa
        >>> pa.Array.from_pandas(pd.Series([1, 2]))
        <pyarrow.array.Int64Array object at 0x7f674e4c0e10>
        [
          1,
          2
        ]

        >>> import numpy as np
        >>> pa.Array.from_pandas(pd.Series([1, 2]), np.array([0, 1],
        ... dtype=bool))
        <pyarrow.array.Int64Array object at 0x7f9019e11208>
        [
          1,
          NA
        ]

        Returns
        -------
        pyarrow.array.Array
        """
        cdef:
            shared_ptr[CArray] out
            shared_ptr[CDataType] c_type
            CMemoryPool* pool

        if mask is not None:
            mask = get_series_values(mask)

        values = get_series_values(obj)
        pool = maybe_unbox_memory_pool(memory_pool)

        if isinstance(values, Categorical):
            return DictionaryArray.from_arrays(
                values.codes, values.categories.values,
                mask=mask, memory_pool=memory_pool)
        elif values.dtype == object:
            # Object dtype undergoes a different conversion path as more type
            # inference may be needed
            if type is not None:
                c_type = type.sp_type
            with nogil:
                check_status(PandasObjectsToArrow(
                    pool, values, mask, c_type, &out))
        else:
            values, type = maybe_coerce_datetime64(
                values, obj.dtype, type, timestamps_to_ms=timestamps_to_ms)

            if type is None:
                check_status(NumPyDtypeToArrow(values.dtype, &c_type))
            else:
                c_type = type.sp_type

            with nogil:
                check_status(PandasToArrow(
                    pool, values, mask, c_type, &out))

        return pyarrow_wrap_array(out)

    property null_count:

        def __get__(self):
            return self.sp_array.get().null_count()

    def __iter__(self):
        for i in range(len(self)):
            yield self.getitem(i)
        raise StopIteration

    def __repr__(self):
        from pyarrow.formatting import array_format
        type_format = object.__repr__(self)
        values = array_format(self, window=10)
        return '{0}\n{1}'.format(type_format, values)

    def equals(Array self, Array other):
        return self.ap.Equals(deref(other.ap))

    def __len__(self):
        if self.sp_array.get():
            return self.sp_array.get().length()
        else:
            return 0

    def isnull(self):
        raise NotImplemented

    def __getitem__(self, key):
        cdef:
            Py_ssize_t n = len(self)

        if PySlice_Check(key):
            start = key.start or 0
            while start < 0:
                start += n

            stop = key.stop if key.stop is not None else n
            while stop < 0:
                stop += n

            step = key.step or 1
            if step != 1:
                raise IndexError('only slices with step 1 supported')
            else:
                return self.slice(start, stop - start)

        while key < 0:
            key += len(self)

        return self.getitem(key)

    cdef getitem(self, int64_t i):
        return box_scalar(self.type, self.sp_array, i)

    def slice(self, offset=0, length=None):
        """
        Compute zero-copy slice of this array

        Parameters
        ----------
        offset : int, default 0
            Offset from start of array to slice
        length : int, default None
            Length of slice (default is until end of Array starting from
            offset)

        Returns
        -------
        sliced : RecordBatch
        """
        cdef:
            shared_ptr[CArray] result

        if offset < 0:
            raise IndexError('Offset must be non-negative')

        if length is None:
            result = self.ap.Slice(offset)
        else:
            result = self.ap.Slice(offset, length)

        return pyarrow_wrap_array(result)

    def to_pandas(self):
        """
        Convert to an array object suitable for use in pandas

        See also
        --------
        Column.to_pandas
        Table.to_pandas
        RecordBatch.to_pandas
        """
        cdef:
            PyObject* out

        with nogil:
            check_status(ConvertArrayToPandas(self.sp_array, self, &out))
        return wrap_array_output(out)

    def to_pylist(self):
        """
        Convert to an list of native Python objects.
        """
        return [x.as_py() for x in self]


cdef class Tensor:

    cdef init(self, const shared_ptr[CTensor]& sp_tensor):
        self.sp_tensor = sp_tensor
        self.tp = sp_tensor.get()
        self.type = pyarrow_wrap_data_type(self.tp.type())

    def __repr__(self):
        return """<pyarrow.Tensor>
type: {0}
shape: {1}
strides: {2}""".format(self.type, self.shape, self.strides)

    @staticmethod
    def from_numpy(obj):
        cdef shared_ptr[CTensor] ctensor
        check_status(NdarrayToTensor(c_default_memory_pool(), obj, &ctensor))
        return pyarrow_wrap_tensor(ctensor)

    def to_numpy(self):
        """
        Convert arrow::Tensor to numpy.ndarray with zero copy
        """
        cdef:
            PyObject* out

        check_status(TensorToNdarray(deref(self.tp), self, &out))
        return PyObject_to_object(out)

    def equals(self, Tensor other):
        """
        Return true if the tensors contains exactly equal data
        """
        return self.tp.Equals(deref(other.tp))

    property is_mutable:

        def __get__(self):
            return self.tp.is_mutable()

    property is_contiguous:

        def __get__(self):
            return self.tp.is_contiguous()

    property ndim:

        def __get__(self):
            return self.tp.ndim()

    property size:

        def __get__(self):
            return self.tp.size()

    property shape:

        def __get__(self):
            cdef size_t i
            py_shape = []
            for i in range(self.tp.shape().size()):
                py_shape.append(self.tp.shape()[i])
            return py_shape

    property strides:

        def __get__(self):
            cdef size_t i
            py_strides = []
            for i in range(self.tp.strides().size()):
                py_strides.append(self.tp.strides()[i])
            return py_strides



cdef wrap_array_output(PyObject* output):
    cdef object obj = PyObject_to_object(output)

    if isinstance(obj, dict):
        return Categorical(obj['indices'],
                           categories=obj['dictionary'],
                           fastpath=True)
    else:
        return obj


cdef class NullArray(Array):
    pass


cdef class BooleanArray(Array):
    pass


cdef class NumericArray(Array):
    pass


cdef class IntegerArray(NumericArray):
    pass


cdef class FloatingPointArray(NumericArray):
    pass


cdef class Int8Array(IntegerArray):
    pass


cdef class UInt8Array(IntegerArray):
    pass


cdef class Int16Array(IntegerArray):
    pass


cdef class UInt16Array(IntegerArray):
    pass


cdef class Int32Array(IntegerArray):
    pass


cdef class UInt32Array(IntegerArray):
    pass


cdef class Int64Array(IntegerArray):
    pass


cdef class UInt64Array(IntegerArray):
    pass


cdef class Date32Array(NumericArray):
    pass


cdef class Date64Array(NumericArray):
    pass


cdef class TimestampArray(NumericArray):
    pass


cdef class Time32Array(NumericArray):
    pass


cdef class Time64Array(NumericArray):
    pass


cdef class FloatArray(FloatingPointArray):
    pass


cdef class DoubleArray(FloatingPointArray):
    pass


cdef class FixedSizeBinaryArray(Array):
    pass


cdef class DecimalArray(FixedSizeBinaryArray):
    pass


cdef class ListArray(Array):
    pass


cdef class StringArray(Array):
    pass


cdef class BinaryArray(Array):
    pass


cdef class DictionaryArray(Array):

    cdef getitem(self, int64_t i):
        cdef Array dictionary = self.dictionary
        index = self.indices[i]
        if index is NA:
            return index
        else:
            return box_scalar(dictionary.type, dictionary.sp_array,
                              index.as_py())

    property dictionary:

        def __get__(self):
            cdef CDictionaryArray* darr = <CDictionaryArray*>(self.ap)

            if self._dictionary is None:
                self._dictionary = pyarrow_wrap_array(darr.dictionary())

            return self._dictionary

    property indices:

        def __get__(self):
            cdef CDictionaryArray* darr = <CDictionaryArray*>(self.ap)

            if self._indices is None:
                self._indices = pyarrow_wrap_array(darr.indices())

            return self._indices

    @staticmethod
    def from_arrays(indices, dictionary, mask=None,
                    MemoryPool memory_pool=None):
        """
        Construct Arrow DictionaryArray from array of indices (must be
        non-negative integers) and corresponding array of dictionary values

        Parameters
        ----------
        indices : ndarray or pandas.Series, integer type
        dictionary : ndarray or pandas.Series
        mask : ndarray or pandas.Series, boolean type
            True values indicate that indices are actually null

        Returns
        -------
        dict_array : DictionaryArray
        """
        cdef:
            Array arrow_indices, arrow_dictionary
            DictionaryArray result
            shared_ptr[CDataType] c_type
            shared_ptr[CArray] c_result

        if isinstance(indices, Array):
            if mask is not None:
                raise NotImplementedError(
                    "mask not implemented with Arrow array inputs yet")
            arrow_indices = indices
        else:
            if mask is None:
                mask = indices == -1
            else:
                mask = mask | (indices == -1)
            arrow_indices = Array.from_pandas(indices, mask=mask,
                                              memory_pool=memory_pool)

        if isinstance(dictionary, Array):
            arrow_dictionary = dictionary
        else:
            arrow_dictionary = Array.from_pandas(dictionary,
                                                 memory_pool=memory_pool)

        if not isinstance(arrow_indices, IntegerArray):
            raise ValueError('Indices must be integer type')

        c_type.reset(new CDictionaryType(arrow_indices.type.sp_type,
                                         arrow_dictionary.sp_array))
        c_result.reset(new CDictionaryArray(c_type, arrow_indices.sp_array))

        result = DictionaryArray()
        result.init(c_result)
        return result


cdef dict _array_classes = {
    _Type_NA: NullArray,
    _Type_BOOL: BooleanArray,
    _Type_UINT8: UInt8Array,
    _Type_UINT16: UInt16Array,
    _Type_UINT32: UInt32Array,
    _Type_UINT64: UInt64Array,
    _Type_INT8: Int8Array,
    _Type_INT16: Int16Array,
    _Type_INT32: Int32Array,
    _Type_INT64: Int64Array,
    _Type_DATE32: Date32Array,
    _Type_DATE64: Date64Array,
    _Type_TIMESTAMP: TimestampArray,
    _Type_TIME32: Time32Array,
    _Type_TIME64: Time64Array,
    _Type_FLOAT: FloatArray,
    _Type_DOUBLE: DoubleArray,
    _Type_LIST: ListArray,
    _Type_BINARY: BinaryArray,
    _Type_STRING: StringArray,
    _Type_DICTIONARY: DictionaryArray,
    _Type_FIXED_SIZE_BINARY: FixedSizeBinaryArray,
    _Type_DECIMAL: DecimalArray,
}


cdef object get_series_values(object obj):
    if isinstance(obj, PandasSeries):
        result = obj.values
    elif isinstance(obj, np.ndarray):
        result = obj
    else:
        result = PandasSeries(obj).values

    return result
