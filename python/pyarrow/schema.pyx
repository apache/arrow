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

########################################
# Data types, fields, schemas, and so forth

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from cython.operator cimport dereference as deref

from pyarrow.compat import frombytes, tobytes
from pyarrow.array cimport Array
from pyarrow.error cimport check_status
from pyarrow.includes.libarrow cimport (CDataType, CStructType, CListType,
                                        CFixedSizeBinaryType,
                                        CDecimalType,
                                        TimeUnit_SECOND, TimeUnit_MILLI,
                                        TimeUnit_MICRO, TimeUnit_NANO,
                                        Type, TimeUnit)
cimport pyarrow.includes.pyarrow as pyarrow
cimport pyarrow.includes.libarrow as la

cimport cpython

import six


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
        if op == cpython.Py_EQ:
            return self.type.Equals(deref(other.type))
        elif op == cpython.Py_NE:
            return not self.type.Equals(deref(other.type))
        else:
            raise TypeError('Invalid comparison')


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
            return timeunit_to_string(self.ts_type.unit)

    property tz:

        def __get__(self):
            if self.ts_type.timezone.size() > 0:
                return frombytes(self.ts_type.timezone)
            else:
                return None


cdef class FixedSizeBinaryType(DataType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.fixed_size_binary_type = <const CFixedSizeBinaryType*> type.get()

    property byte_width:

        def __get__(self):
            return self.fixed_size_binary_type.byte_width()


cdef class DecimalType(FixedSizeBinaryType):

    cdef void init(self, const shared_ptr[CDataType]& type):
        DataType.init(self, type)
        self.decimal_type = <const CDecimalType*> type.get()


cdef class Field:

    def __cinit__(self):
        pass

    cdef init(self, const shared_ptr[CField]& field):
        self.sp_field = field
        self.field = field.get()
        self.type = box_data_type(field.get().type)

    @classmethod
    def from_py(cls, object name, DataType type, bint nullable=True):
        cdef Field result = Field()
        result.type = type
        result.sp_field.reset(new CField(tobytes(name), type.sp_type,
                                         nullable))
        result.field = result.sp_field.get()

        return result

    def __repr__(self):
        return 'Field({0!r}, type={1})'.format(self.name, str(self.type))

    property nullable:

        def __get__(self):
            return self.field.nullable

    property name:

        def __get__(self):
            if box_field(self.sp_field) is None:
                raise ReferenceError(
                    'Field not initialized (references NULL pointer)')
            return frombytes(self.field.name)


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
        result.type = box_data_type(result.field.type)

        return result

    cdef init(self, const vector[shared_ptr[CField]]& fields):
        self.schema = new CSchema(fields)
        self.sp_schema.reset(self.schema)

    cdef init_schema(self, const shared_ptr[CSchema]& schema):
        self.schema = schema.get()
        self.sp_schema = schema

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
        return box_field(self.schema.GetFieldByName(tobytes(name)))

    @classmethod
    def from_fields(cls, fields):
        cdef:
            Schema result
            Field field
            vector[shared_ptr[CField]] c_fields

        c_fields.resize(len(fields))

        for i in range(len(fields)):
            field = fields[i]
            c_fields[i] = field.sp_field

        result = Schema()
        result.init(c_fields)

        return result

    def __str__(self):
        return frombytes(self.schema.ToString())

    def __repr__(self):
        return self.__str__()


cdef dict _type_cache = {}


cdef DataType primitive_type(Type type):
    if type in _type_cache:
        return _type_cache[type]

    cdef DataType out = DataType()
    out.init(pyarrow.GetPrimitiveType(type))

    _type_cache[type] = out
    return out

#------------------------------------------------------------
# Type factory functions

def field(name, type, bint nullable=True):
    return Field.from_py(name, type, nullable)


cdef set PRIMITIVE_TYPES = set([
    la.Type_NA, la.Type_BOOL,
    la.Type_UINT8, la.Type_INT8,
    la.Type_UINT16, la.Type_INT16,
    la.Type_UINT32, la.Type_INT32,
    la.Type_UINT64, la.Type_INT64,
    la.Type_TIMESTAMP, la.Type_DATE32,
    la.Type_DATE64,
    la.Type_HALF_FLOAT,
    la.Type_FLOAT,
    la.Type_DOUBLE])


def null():
    return primitive_type(la.Type_NA)


def bool_():
    return primitive_type(la.Type_BOOL)


def uint8():
    return primitive_type(la.Type_UINT8)


def int8():
    return primitive_type(la.Type_INT8)


def uint16():
    return primitive_type(la.Type_UINT16)


def int16():
    return primitive_type(la.Type_INT16)


def uint32():
    return primitive_type(la.Type_UINT32)


def int32():
    return primitive_type(la.Type_INT32)


def uint64():
    return primitive_type(la.Type_UINT64)


def int64():
    return primitive_type(la.Type_INT64)


cdef dict _timestamp_type_cache = {}


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
        raise TypeError('Invalid TimeUnit string')

    cdef TimestampType out = TimestampType()

    if tz is None:
        out.init(la.timestamp(unit))
        if unit in _timestamp_type_cache:
            return _timestamp_type_cache[unit]
        _timestamp_type_cache[unit] = out
    else:
        if not isinstance(tz, six.string_types):
            tz = tz.zone

        c_timezone = tobytes(tz)
        out.init(la.timestamp(unit, c_timezone))

    return out


def date32():
    return primitive_type(la.Type_DATE32)


def date64():
    return primitive_type(la.Type_DATE64)


def float16():
    return primitive_type(la.Type_HALF_FLOAT)


def float32():
    return primitive_type(la.Type_FLOAT)


def float64():
    return primitive_type(la.Type_DOUBLE)


cpdef DataType decimal(int precision, int scale=0):
    cdef shared_ptr[CDataType] decimal_type
    decimal_type.reset(new CDecimalType(precision, scale))
    return box_data_type(decimal_type)


def string():
    """
    UTF8 string
    """
    return primitive_type(la.Type_STRING)


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
        return primitive_type(la.Type_BINARY)

    cdef shared_ptr[CDataType] fixed_size_binary_type
    fixed_size_binary_type.reset(new CFixedSizeBinaryType(length))
    return box_data_type(fixed_size_binary_type)


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
    return Schema.from_fields(fields)


cdef DataType box_data_type(const shared_ptr[CDataType]& type):
    cdef:
        DataType out

    if type.get() == NULL:
        return None

    if type.get().type == la.Type_DICTIONARY:
        out = DictionaryType()
    elif type.get().type == la.Type_TIMESTAMP:
        out = TimestampType()
    elif type.get().type == la.Type_FIXED_SIZE_BINARY:
        out = FixedSizeBinaryType()
    elif type.get().type == la.Type_DECIMAL:
        out = DecimalType()
    else:
        out = DataType()

    out.init(type)
    return out

cdef Field box_field(const shared_ptr[CField]& field):
    if field.get() == NULL:
        return None
    cdef Field out = Field()
    out.init(field)
    return out

cdef Schema box_schema(const shared_ptr[CSchema]& type):
    cdef Schema out = Schema()
    out.init_schema(type)
    return out


def type_from_numpy_dtype(object dtype):
    cdef shared_ptr[CDataType] c_type
    with nogil:
        check_status(pyarrow.NumPyDtypeToArrow(dtype, &c_type))

    return box_data_type(c_type)
