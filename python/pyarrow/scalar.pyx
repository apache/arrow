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

from pyarrow.schema cimport DataType, box_data_type

from pyarrow.compat import frombytes
import pyarrow.schema as schema
import decimal
import datetime

cimport cpython as cp

NA = None


cdef _pandas():
    import pandas as pd
    return pd


cdef class NAType(Scalar):

    def __cinit__(self):
        global NA
        if NA is not None:
            raise Exception('Cannot create multiple NAType instances')

        self.type = schema.null()

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
        return decimal.Decimal(s.decode('utf8'))


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
        self.value_type = box_data_type(self.ap.value_type())

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
    Type_BOOL: BooleanValue,
    Type_UINT8: Int8Value,
    Type_UINT16: Int16Value,
    Type_UINT32: Int32Value,
    Type_UINT64: Int64Value,
    Type_INT8: Int8Value,
    Type_INT16: Int16Value,
    Type_INT32: Int32Value,
    Type_INT64: Int64Value,
    Type_DATE32: Date32Value,
    Type_DATE64: Date64Value,
    Type_TIMESTAMP: TimestampValue,
    Type_FLOAT: FloatValue,
    Type_DOUBLE: DoubleValue,
    Type_LIST: ListValue,
    Type_BINARY: BinaryValue,
    Type_STRING: StringValue,
    Type_FIXED_SIZE_BINARY: FixedSizeBinaryValue,
    Type_DECIMAL: DecimalValue,
}

cdef object box_scalar(DataType type, const shared_ptr[CArray]& sp_array,
                       int64_t index):
    cdef ArrayValue val
    if type.type.id() == Type_NA:
        return NA
    elif sp_array.get().IsNull(index):
        return NA
    else:
        val = _scalar_classes[type.type.id()]()
        val.init(type, sp_array, index)
        return val
