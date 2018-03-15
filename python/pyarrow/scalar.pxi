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


NA = None


cdef class NAType(Scalar):
    """
    Null (NA) value singleton
    """
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

    def _check_null(self):
        if self.sp_array.get() == NULL:
            raise ReferenceError(
                'ArrayValue instance not propertly initialized '
                '(references NULL pointer)')

    def __repr__(self):
        self._check_null()
        if hasattr(self, 'as_py'):
            return repr(self.as_py())
        else:
            return super(Scalar, self).__repr__()

    def __eq__(self, other):
        if hasattr(self, 'as_py'):
            if isinstance(other, ArrayValue):
                other = other.as_py()
            return self.as_py() == other
        else:
            raise NotImplementedError(
                "Cannot compare Arrow values that don't support as_py()")


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


cdef class Time32Value(ArrayValue):

    def as_py(self):
        cdef:
            CTime32Array* ap = <CTime32Array*> self.sp_array.get()
            CTime32Type* dtype = <CTime32Type*> ap.type().get()

        if dtype.unit() == TimeUnit_SECOND:
            delta = datetime.timedelta(seconds=ap.Value(self.index))
            return (datetime.datetime(1970, 1, 1) + delta).time()
        else:
            delta = datetime.timedelta(milliseconds=ap.Value(self.index))
            return (datetime.datetime(1970, 1, 1) + delta).time()


cdef class Time64Value(ArrayValue):

    def as_py(self):
        cdef:
            CTime64Array* ap = <CTime64Array*> self.sp_array.get()
            CTime64Type* dtype = <CTime64Type*> ap.type().get()

        cdef int64_t val = ap.Value(self.index)
        if dtype.unit() == TimeUnit_MICRO:
            return (datetime.datetime(1970, 1, 1) +
                    datetime.timedelta(microseconds=val)).time()
        else:
            return (datetime.datetime(1970, 1, 1) +
                    datetime.timedelta(microseconds=val / 1000)).time()


cdef dict DATETIME_CONVERSION_FUNCTIONS

try:
    import pandas as pd
except ImportError:
    DATETIME_CONVERSION_FUNCTIONS = {
        TimeUnit_SECOND: lambda x, tzinfo: (
            datetime.datetime.utcfromtimestamp(x).replace(tzinfo=tzinfo)
        ),
        TimeUnit_MILLI: lambda x, tzinfo: (
            datetime.datetime.utcfromtimestamp(x / 1e3).replace(tzinfo=tzinfo)
        ),
        TimeUnit_MICRO: lambda x, tzinfo: (
            datetime.datetime.utcfromtimestamp(x / 1e6).replace(tzinfo=tzinfo)
        ),
    }
else:
    DATETIME_CONVERSION_FUNCTIONS = {
        TimeUnit_SECOND: lambda x, tzinfo: pd.Timestamp(
            x * 1000000000, tz=tzinfo, unit='ns',
        ),
        TimeUnit_MILLI: lambda x, tzinfo: pd.Timestamp(
            x * 1000000, tz=tzinfo, unit='ns',
        ),
        TimeUnit_MICRO: lambda x, tzinfo: pd.Timestamp(
            x * 1000, tz=tzinfo, unit='ns',
        ),
        TimeUnit_NANO: lambda x, tzinfo: pd.Timestamp(
            x, tz=tzinfo, unit='ns',
        )
    }


cdef class TimestampValue(ArrayValue):

    property value:

        def __get__(self):
            cdef CTimestampArray* ap = <CTimestampArray*> self.sp_array.get()
            cdef CTimestampType* dtype = <CTimestampType*> ap.type().get()
            return ap.Value(self.index)

    def as_py(self):
        cdef CTimestampArray* ap = <CTimestampArray*> self.sp_array.get()
        cdef CTimestampType* dtype = <CTimestampType*> ap.type().get()

        value = self.value

        if not dtype.timezone().empty():
            import pytz
            tzinfo = pytz.timezone(frombytes(dtype.timezone()))
        else:
            tzinfo = None

        try:
            converter = DATETIME_CONVERSION_FUNCTIONS[dtype.unit()]
        except KeyError:
            raise ValueError(
                'Cannot convert nanosecond timestamps without pandas'
            )
        return converter(value, tzinfo=tzinfo)


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
            CDecimal128Array* ap = <CDecimal128Array*> self.sp_array.get()
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


cdef class UnionValue(ArrayValue):

    cdef void _set_array(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array
        self.ap = <CUnionArray*> sp_array.get()

    cdef getitem(self, int64_t i):
        cdef int8_t type_id = self.ap.raw_type_ids()[i]
        cdef shared_ptr[CArray] child = self.ap.child(type_id)
        if self.ap.mode() == _UnionMode_SPARSE:
            return box_scalar(self.type[type_id], child, i)
        else:
            return box_scalar(self.type[type_id], child,
                              self.ap.value_offset(i))

    def as_py(self):
        return self.getitem(self.index).as_py()


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


cdef class StructValue(ArrayValue):

    def as_py(self):
        cdef:
            CStructArray* ap
            vector[shared_ptr[CField]] child_fields = self.type.type.children()

        ap = <CStructArray*> self.sp_array.get()
        wrapped_arrays = [pyarrow_wrap_array(ap.field(i))
                          for i in range(ap.num_fields())]
        child_names = [child.get().name() for child in child_fields]
        # Return the struct as a dict
        return {
            frombytes(name): child_array[self.index].as_py()
            for name, child_array in
            zip(child_names, wrapped_arrays)
        }


cdef dict _scalar_classes = {
    _Type_BOOL: BooleanValue,
    _Type_UINT8: UInt8Value,
    _Type_UINT16: UInt16Value,
    _Type_UINT32: UInt32Value,
    _Type_UINT64: UInt64Value,
    _Type_INT8: Int8Value,
    _Type_INT16: Int16Value,
    _Type_INT32: Int32Value,
    _Type_INT64: Int64Value,
    _Type_DATE32: Date32Value,
    _Type_DATE64: Date64Value,
    _Type_TIME32: Time32Value,
    _Type_TIME64: Time64Value,
    _Type_TIMESTAMP: TimestampValue,
    _Type_FLOAT: FloatValue,
    _Type_DOUBLE: DoubleValue,
    _Type_LIST: ListValue,
    _Type_UNION: UnionValue,
    _Type_BINARY: BinaryValue,
    _Type_STRING: StringValue,
    _Type_FIXED_SIZE_BINARY: FixedSizeBinaryValue,
    _Type_DECIMAL: DecimalValue,
    _Type_STRUCT: StructValue,
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
