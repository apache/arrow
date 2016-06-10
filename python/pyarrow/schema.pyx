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

from pyarrow.compat import frombytes, tobytes
from pyarrow.includes.libarrow cimport *
cimport pyarrow.includes.pyarrow as pyarrow

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

    def __cinit__(self):
        pass

    cdef init(self, const shared_ptr[CField]& field):
        self.sp_field = field
        self.field = field.get()

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

    def __repr__(self):
        return frombytes(self.schema.ToString())

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
    Type_NA, Type_BOOL,
    Type_UINT8, Type_INT8,
    Type_UINT16, Type_INT16,
    Type_UINT32, Type_INT32,
    Type_UINT64, Type_INT64,
    Type_FLOAT, Type_DOUBLE])

def null():
    return primitive_type(Type_NA)

def bool_():
    return primitive_type(Type_BOOL)

def uint8():
    return primitive_type(Type_UINT8)

def int8():
    return primitive_type(Type_INT8)

def uint16():
    return primitive_type(Type_UINT16)

def int16():
    return primitive_type(Type_INT16)

def uint32():
    return primitive_type(Type_UINT32)

def int32():
    return primitive_type(Type_INT32)

def uint64():
    return primitive_type(Type_UINT64)

def int64():
    return primitive_type(Type_INT64)

def float_():
    return primitive_type(Type_FLOAT)

def double():
    return primitive_type(Type_DOUBLE)

def string():
    """
    UTF8 string
    """
    return primitive_type(Type_STRING)

def list_(DataType value_type):
    cdef DataType out = DataType()
    cdef shared_ptr[CDataType] list_type
    list_type.reset(new CListType(value_type.sp_type))
    out.init(list_type)
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
    cdef DataType out = DataType()
    out.init(type)
    return out

cdef Schema box_schema(const shared_ptr[CSchema]& type):
    cdef Schema out = Schema()
    out.init_schema(type)
    return out
