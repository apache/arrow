# -*- coding: utf-8 -*-
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

import json
import os
import pyarrow as pa
import pyarrow.jvm as pa_jvm
import pytest
import sys
import xml.etree.ElementTree as ET


jpype = pytest.importorskip("jpype")


@pytest.fixture(scope="session")
def root_allocator():
    # This test requires Arrow Java to be built in the same source tree
    pom_path = os.path.join(
        os.path.dirname(__file__), '..', '..', '..',
        'java', 'pom.xml')
    tree = ET.parse(pom_path)
    version = tree.getroot().find(
        'POM:version',
        namespaces={
            'POM': 'http://maven.apache.org/POM/4.0.0'
        }).text
    jar_path = os.path.join(
        os.path.dirname(__file__), '..', '..', '..',
        'java', 'tools', 'target',
        'arrow-tools-{}-jar-with-dependencies.jar'.format(version))
    jar_path = os.getenv("ARROW_TOOLS_JAR", jar_path)
    jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jar_path)
    return jpype.JPackage("org").apache.arrow.memory.RootAllocator(sys.maxsize)


def test_jvm_buffer(root_allocator):
    # Create a buffer
    jvm_buffer = root_allocator.buffer(8)
    for i in range(8):
        jvm_buffer.setByte(i, 8 - i)

    # Convert to Python
    buf = pa_jvm.jvm_buffer(jvm_buffer)

    # Check its content
    assert buf.to_pybytes() == b'\x08\x07\x06\x05\x04\x03\x02\x01'


def _jvm_field(jvm_spec):
    om = jpype.JClass('com.fasterxml.jackson.databind.ObjectMapper')()
    pojo_Field = jpype.JClass('org.apache.arrow.vector.types.pojo.Field')
    return om.readValue(jvm_spec, pojo_Field)


# In the following, we use the JSON serialization of the Field objects in Java.
# This ensures that we neither rely on the exact mechanics on how to construct
# them using Java code as well as enables us to define them as parameters
# without to invoke the JVM.
#
# The specifications were created using:
#
#   om = jpype.JClass('com.fasterxml.jackson.databind.ObjectMapper')()
#   field = …  # Code to instantiate the field
#   jvm_spec = om.writeValueAsString(field)
@pytest.mark.parametrize('typ,jvm_spec', [
    (pa.null(), '{"name":"null"}'),
    (pa.bool_(), '{"name":"bool"}'),
    (pa.int8(), '{"name":"int","bitWidth":8,"isSigned":true}'),
    (pa.int16(), '{"name":"int","bitWidth":16,"isSigned":true}'),
    (pa.int32(), '{"name":"int","bitWidth":32,"isSigned":true}'),
    (pa.int64(), '{"name":"int","bitWidth":64,"isSigned":true}'),
    (pa.uint8(), '{"name":"int","bitWidth":8,"isSigned":false}'),
    (pa.uint16(), '{"name":"int","bitWidth":16,"isSigned":false}'),
    (pa.uint32(), '{"name":"int","bitWidth":32,"isSigned":false}'),
    (pa.uint64(), '{"name":"int","bitWidth":64,"isSigned":false}'),
    (pa.float16(), '{"name":"floatingpoint","precision":"HALF"}'),
    (pa.float32(), '{"name":"floatingpoint","precision":"SINGLE"}'),
    (pa.float64(), '{"name":"floatingpoint","precision":"DOUBLE"}'),
    (pa.time32('s'), '{"name":"time","unit":"SECOND","bitWidth":32}'),
    (pa.time32('ms'), '{"name":"time","unit":"MILLISECOND","bitWidth":32}'),
    (pa.time64('us'), '{"name":"time","unit":"MICROSECOND","bitWidth":64}'),
    (pa.time64('ns'), '{"name":"time","unit":"NANOSECOND","bitWidth":64}'),
    (pa.timestamp('s'), '{"name":"timestamp","unit":"SECOND",'
        '"timezone":null}'),
    (pa.timestamp('ms'), '{"name":"timestamp","unit":"MILLISECOND",'
        '"timezone":null}'),
    (pa.timestamp('us'), '{"name":"timestamp","unit":"MICROSECOND",'
        '"timezone":null}'),
    (pa.timestamp('ns'), '{"name":"timestamp","unit":"NANOSECOND",'
        '"timezone":null}'),
    (pa.timestamp('ns', tz='UTC'), '{"name":"timestamp","unit":"NANOSECOND"'
        ',"timezone":"UTC"}'),
    (pa.timestamp('ns', tz='Europe/Paris'), '{"name":"timestamp",'
        '"unit":"NANOSECOND","timezone":"Europe/Paris"}'),
    (pa.date32(), '{"name":"date","unit":"DAY"}'),
    (pa.date64(), '{"name":"date","unit":"MILLISECOND"}'),
    (pa.decimal128(19, 4), '{"name":"decimal","precision":19,"scale":4}'),
    (pa.string(), '{"name":"utf8"}'),
    (pa.binary(), '{"name":"binary"}'),
    (pa.binary(10), '{"name":"fixedsizebinary","byteWidth":10}'),
    # TODO(ARROW-2609): complex types that have children
    # pa.list_(pa.int32()),
    # pa.struct([pa.field('a', pa.int32()),
    #            pa.field('b', pa.int8()),
    #            pa.field('c', pa.string())]),
    # pa.union([pa.field('a', pa.binary(10)),
    #           pa.field('b', pa.string())], mode=pa.lib.UnionMode_DENSE),
    # pa.union([pa.field('a', pa.binary(10)),
    #           pa.field('b', pa.string())], mode=pa.lib.UnionMode_SPARSE),
    # TODO: DictionaryType requires a vector in the type
    # pa.dictionary(pa.int32(), pa.array(['a', 'b', 'c'])),
])
@pytest.mark.parametrize('nullable', [True, False])
def test_jvm_types(root_allocator, typ, jvm_spec, nullable):
    spec = {
        'name': 'field_name',
        'nullable': nullable,
        'type': json.loads(jvm_spec),
        # TODO: This needs to be set for complex types
        'children': []
    }
    jvm_field = _jvm_field(json.dumps(spec))
    result = pa_jvm.field(jvm_field)
    assert result == pa.field('field_name', typ, nullable=nullable)


# These test parameters mostly use an integer range as an input as this is
# often the only type that is understood by both Python and Java
# implementations of Arrow.
@pytest.mark.parametrize('typ,data,jvm_type', [
    (pa.bool_(), [True, False, True, True], 'BitVector'),
    (pa.uint8(), list(range(128)), 'UInt1Vector'),
    (pa.uint16(), list(range(128)), 'UInt2Vector'),
    (pa.int32(), list(range(128)), 'IntVector'),
    (pa.int64(), list(range(128)), 'BigIntVector'),
    (pa.float32(), list(range(128)), 'Float4Vector'),
    (pa.float64(), list(range(128)), 'Float8Vector'),
    (pa.timestamp('s'), list(range(128)), 'TimeStampSecVector'),
    (pa.timestamp('ms'), list(range(128)), 'TimeStampMilliVector'),
    (pa.timestamp('us'), list(range(128)), 'TimeStampMicroVector'),
    (pa.timestamp('ns'), list(range(128)), 'TimeStampNanoVector'),
    # TODO(ARROW-2605): These types miss a conversion from pure Python objects
    #  * pa.time32('s')
    #  * pa.time32('ms')
    #  * pa.time64('us')
    #  * pa.time64('ns')
    (pa.date32(), list(range(128)), 'DateDayVector'),
    (pa.date64(), list(range(128)), 'DateMilliVector'),
    # TODO(ARROW-2606): pa.decimal128(19, 4)
])
def test_jvm_array(root_allocator, typ, data, jvm_type):
    # Create vector
    cls = "org.apache.arrow.vector.{}".format(jvm_type)
    jvm_vector = jpype.JClass(cls)("vector", root_allocator)
    jvm_vector.allocateNew(len(data))
    for i, val in enumerate(data):
        jvm_vector.setSafe(i, val)
    jvm_vector.setValueCount(len(data))

    py_array = pa.array(data, type=typ)
    jvm_array = pa_jvm.array(jvm_vector)

    assert py_array.equals(jvm_array)


def _string_to_varchar_holder(ra, string):
    nvch_cls = "org.apache.arrow.vector.holders.NullableVarCharHolder"
    holder = jpype.JClass(nvch_cls)()
    if string is None:
        holder.isSet = 0
    else:
        holder.isSet = 1
        value = jpype.JClass("java.lang.String")("string")
        std_charsets = jpype.JClass("java.nio.charset.StandardCharsets")
        bytes_ = value.getBytes(std_charsets.UTF_8)
        holder.buffer = ra.buffer(len(bytes_))
        holder.buffer.setBytes(0, bytes_, 0, len(bytes_))
        holder.start = 0
        holder.end = len(bytes_)
    return holder


# TODO(ARROW-2607)
@pytest.mark.xfail(reason="from_buffers is only supported for "
                          "primitive arrays yet")
def test_jvm_string_array(root_allocator):
    data = [u"string", None, u"töst"]
    cls = "org.apache.arrow.vector.VarCharVector"
    jvm_vector = jpype.JClass(cls)("vector", root_allocator)
    jvm_vector.allocateNew()

    for i, string in enumerate(data):
        holder = _string_to_varchar_holder(root_allocator, "string")
        jvm_vector.setSafe(i, holder)
        jvm_vector.setValueCount(i + 1)

    py_array = pa.array(data, type=pa.string())
    jvm_array = pa_jvm.array(jvm_vector)

    assert py_array.equals(jvm_array)
