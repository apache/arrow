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

from arrow.compat import frombytes, tobytes
from arrow.includes.arrow cimport *
cimport arrow.includes.pyarrow as pyarrow

cimport cpython

cdef class DataType:

    def __cinit__(self):
        pass

    cdef init(self, const shared_ptr[CDataType]& type):
        self.sp_type = type
        self.type = type.get()

    def __str__(self):
        return frombytes(self.type.ToString())

    def __repr__(self):
        return 'DataType({0})'.format(str(self))

    def __richcmp__(DataType self, DataType other, int op):
        if op == cpython.Py_EQ:
            return self.type.Equals(other.type)
        elif op == cpython.Py_NE:
            return not self.type.Equals(other.type)
        else:
            raise TypeError('Invalid comparison')


cdef class Field:

    def __cinit__(self, object name, DataType type):
        self.type = type
        self.sp_field.reset(new CField(tobytes(name), type.sp_type))
        self.field = self.sp_field.get()

    def __repr__(self):
        return 'Field({0!r}, type={1})'.format(self.name, str(self.type))

    property name:

        def __get__(self):
            return frombytes(self.field.name)

cdef dict _type_cache = {}

cdef DataType primitive_type(LogicalType type, bint nullable=True):
    if (type, nullable) in _type_cache:
        return _type_cache[type, nullable]

    cdef DataType out = DataType()
    out.init(pyarrow.GetPrimitiveType(type, nullable))

    _type_cache[type, nullable] = out
    return out

#------------------------------------------------------------
# Type factory functions

def field(name, type):
    return Field(name, type)

cdef set PRIMITIVE_TYPES = set([
    LogicalType_NA, LogicalType_BOOL,
    LogicalType_UINT8, LogicalType_INT8,
    LogicalType_UINT16, LogicalType_INT16,
    LogicalType_UINT32, LogicalType_INT32,
    LogicalType_UINT64, LogicalType_INT64,
    LogicalType_FLOAT, LogicalType_DOUBLE])

def null():
    return primitive_type(LogicalType_NA)

def bool_(c_bool nullable=True):
    return primitive_type(LogicalType_BOOL, nullable)

def uint8(c_bool nullable=True):
    return primitive_type(LogicalType_UINT8, nullable)

def int8(c_bool nullable=True):
    return primitive_type(LogicalType_INT8, nullable)

def uint16(c_bool nullable=True):
    return primitive_type(LogicalType_UINT16, nullable)

def int16(c_bool nullable=True):
    return primitive_type(LogicalType_INT16, nullable)

def uint32(c_bool nullable=True):
    return primitive_type(LogicalType_UINT32, nullable)

def int32(c_bool nullable=True):
    return primitive_type(LogicalType_INT32, nullable)

def uint64(c_bool nullable=True):
    return primitive_type(LogicalType_UINT64, nullable)

def int64(c_bool nullable=True):
    return primitive_type(LogicalType_INT64, nullable)

def float_(c_bool nullable=True):
    return primitive_type(LogicalType_FLOAT, nullable)

def double(c_bool nullable=True):
    return primitive_type(LogicalType_DOUBLE, nullable)

def string(c_bool nullable=True):
    """
    UTF8 string
    """
    return primitive_type(LogicalType_STRING, nullable)

def list_(DataType value_type, c_bool nullable=True):
    cdef DataType out = DataType()
    out.init(shared_ptr[CDataType](
        new CListType(value_type.sp_type, nullable)))
    return out

def struct(fields, c_bool nullable=True):
    """

    """
    cdef:
        DataType out = DataType()
        Field field
        vector[shared_ptr[CField]] c_fields

    for field in fields:
        c_fields.push_back(field.sp_field)

    out.init(shared_ptr[CDataType](
        new CStructType(c_fields, nullable)))
    return out


cdef DataType box_data_type(const shared_ptr[CDataType]& type):
    cdef DataType out = DataType()
    out.init(type)
    return out
