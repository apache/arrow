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
"""
Functions to interact with Arrow memory allocated by Arrow Java.

These functions convert the objects holding the metadata, the actual
data is not copied at all.

This will only work with a JVM running in the same process such as provided
through jpype. Modules that talk to a remote JVM like py4j will not work as the
memory addresses reported by them are not reachable in the python process.
"""

from __future__ import absolute_import


import pyarrow as pa


def jvm_buffer(arrowbuf):
    """
    Construct an Arrow buffer from io.netty.buffer.ArrowBuf

    Parameters
    ----------

    arrowbuf: io.netty.buffer.ArrowBuf
        Arrow Buffer representation on the JVM

    Returns
    -------
    pyarrow.Buffer
        Python Buffer that references the JVM memory
    """
    address = arrowbuf.memoryAddress()
    size = arrowbuf.capacity()
    return pa.foreign_buffer(address, size, arrowbuf.asNettyBuffer())


def _from_jvm_int_type(jvm_type):
    """
    Convert a JVM int type to its Python equivalent.

    Parameters
    ----------
    jvm_type: org.apache.arrow.vector.types.pojo.ArrowType$Int

    Returns
    -------
    typ: pyarrow.DataType
    """
    if jvm_type.isSigned:
        if jvm_type.bitWidth == 8:
            return pa.int8()
        elif jvm_type.bitWidth == 16:
            return pa.int16()
        elif jvm_type.bitWidth == 32:
            return pa.int32()
        elif jvm_type.bitWidth == 64:
            return pa.int64()
    else:
        if jvm_type.bitWidth == 8:
            return pa.uint8()
        elif jvm_type.bitWidth == 16:
            return pa.uint16()
        elif jvm_type.bitWidth == 32:
            return pa.uint32()
        elif jvm_type.bitWidth == 64:
            return pa.uint64()


def _from_jvm_float_type(jvm_type):
    """
    Convert a JVM float type to its Python equivalent.

    Parameters
    ----------
    jvm_type: org.apache.arrow.vector.types.pojo.ArrowType$FloatingPoint

    Returns
    -------
    typ: pyarrow.DataType
    """
    precision = jvm_type.getPrecision().toString()
    if precision == 'HALF':
        return pa.float16()
    elif precision == 'SINGLE':
        return pa.float32()
    elif precision == 'DOUBLE':
        return pa.float64()


def _from_jvm_time_type(jvm_type):
    """
    Convert a JVM time type to its Python equivalent.

    Parameters
    ----------
    jvm_type: org.apache.arrow.vector.types.pojo.ArrowType$Time

    Returns
    -------
    typ: pyarrow.DataType
    """
    time_unit = jvm_type.getUnit().toString()
    if time_unit == 'SECOND':
        assert jvm_type.bitWidth == 32
        return pa.time32('s')
    elif time_unit == 'MILLISECOND':
        assert jvm_type.bitWidth == 32
        return pa.time32('ms')
    elif time_unit == 'MICROSECOND':
        assert jvm_type.bitWidth == 64
        return pa.time64('us')
    elif time_unit == 'NANOSECOND':
        assert jvm_type.bitWidth == 64
        return pa.time64('ns')


def _from_jvm_timestamp_type(jvm_type):
    """
    Convert a JVM timestamp type to its Python equivalent.

    Parameters
    ----------
    jvm_type: org.apache.arrow.vector.types.pojo.ArrowType$Timestamp

    Returns
    -------
    typ: pyarrow.DataType
    """
    time_unit = jvm_type.getUnit().toString()
    timezone = jvm_type.getTimezone()
    if time_unit == 'SECOND':
        return pa.timestamp('s', tz=timezone)
    elif time_unit == 'MILLISECOND':
        return pa.timestamp('ms', tz=timezone)
    elif time_unit == 'MICROSECOND':
        return pa.timestamp('us', tz=timezone)
    elif time_unit == 'NANOSECOND':
        return pa.timestamp('ns', tz=timezone)


def _from_jvm_date_type(jvm_type):
    """
    Convert a JVM date type to its Python equivalent

    Parameters
    ----------
    jvm_type: org.apache.arrow.vector.types.pojo.ArrowType$Date

    Returns
    -------
    typ: pyarrow.DataType
    """
    day_unit = jvm_type.getUnit().toString()
    if day_unit == 'DAY':
        return pa.date32()
    elif day_unit == 'MILLISECOND':
        return pa.date64()


def field(jvm_field):
    """
    Construct a Field from a org.apache.arrow.vector.types.pojo.Field
    instance.

    Parameters
    ----------
    jvm_field: org.apache.arrow.vector.types.pojo.Field

    Returns
    -------
    pyarrow.Field
    """
    name = jvm_field.getName()
    jvm_type = jvm_field.getType()

    typ = None
    if not jvm_type.isComplex():
        type_str = jvm_type.getTypeID().toString()
        if type_str == 'Null':
            typ = pa.null()
        elif type_str == 'Int':
            typ = _from_jvm_int_type(jvm_type)
        elif type_str == 'FloatingPoint':
            typ = _from_jvm_float_type(jvm_type)
        elif type_str == 'Utf8':
            typ = pa.string()
        elif type_str == 'Binary':
            typ = pa.binary()
        elif type_str == 'FixedSizeBinary':
            typ = pa.binary(jvm_type.getByteWidth())
        elif type_str == 'Bool':
            typ = pa.bool_()
        elif type_str == 'Time':
            typ = _from_jvm_time_type(jvm_type)
        elif type_str == 'Timestamp':
            typ = _from_jvm_timestamp_type(jvm_type)
        elif type_str == 'Date':
            typ = _from_jvm_date_type(jvm_type)
        elif type_str == 'Decimal':
            typ = pa.decimal128(jvm_type.getPrecision(), jvm_type.getScale())
        else:
            raise NotImplementedError(
                "Unsupported JVM type: {}".format(type_str))
    else:
        # TODO: The following JVM types are not implemented:
        #       Struct, List, FixedSizeList, Union, Dictionary
        raise NotImplementedError(
            "JVM field conversion only implemented for primitive types.")

    nullable = jvm_field.isNullable()
    if jvm_field.getMetadata().isEmpty():
        metadata = None
    else:
        metadata = dict(jvm_field.getMetadata())
    return pa.field(name, typ, nullable, metadata)


def schema(jvm_schema):
    """
    Construct a Schema from a org.apache.arrow.vector.types.pojo.Schema
    instance.

    Parameters
    ----------
    jvm_schema: org.apache.arrow.vector.types.pojo.Schema

    Returns
    -------
    pyarrow.Schema
    """
    fields = jvm_schema.getFields()
    fields = [field(f) for f in fields]
    metadata = jvm_schema.getCustomMetadata()
    if metadata.isEmpty():
        meta = None
    else:
        meta = {k: metadata[k] for k in metadata.keySet()}
    return pa.schema(fields, meta)


def array(jvm_array):
    """
    Construct an (Python) Array from its JVM equivalent.

    Parameters
    ----------
    jvm_array : org.apache.arrow.vector.ValueVector

    Returns
    -------
    array : Array
    """
    if jvm_array.getField().getType().isComplex():
        minor_type_str = jvm_array.getMinorType().toString()
        raise NotImplementedError(
            "Cannot convert JVM Arrow array of type {},"
            " complex types not yet implemented.".format(minor_type_str))
    dtype = field(jvm_array.getField()).type
    length = jvm_array.getValueCount()
    buffers = [jvm_buffer(buf)
               for buf in list(jvm_array.getBuffers(False))]
    null_count = jvm_array.getNullCount()
    return pa.Array.from_buffers(dtype, length, buffers, null_count)


def record_batch(jvm_vector_schema_root):
    """
    Construct a (Python) RecordBatch from a JVM VectorSchemaRoot

    Parameters
    ----------
    jvm_vector_schema_root : org.apache.arrow.vector.VectorSchemaRoot

    Returns
    -------
    record_batch: pyarrow.RecordBatch
    """
    pa_schema = schema(jvm_vector_schema_root.getSchema())

    arrays = []
    for name in pa_schema.names:
        arrays.append(array(jvm_vector_schema_root.getVector(name)))

    return pa.RecordBatch.from_arrays(
        arrays,
        pa_schema.names,
        metadata=pa_schema.metadata
    )
