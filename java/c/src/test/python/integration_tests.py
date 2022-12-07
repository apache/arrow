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

import decimal
import gc
import os
import sys
import unittest
import xml.etree.ElementTree as ET

import jpype
import pyarrow as pa
from pyarrow.cffi import ffi


def setup_jvm():
    # This test requires Arrow Java to be built in the same source tree
    try:
        arrow_dir = os.environ["ARROW_SOURCE_DIR"]
    except KeyError:
        arrow_dir = os.path.join(os.path.dirname(
            __file__), '..', '..', '..', '..', '..')
    pom_path = os.path.join(arrow_dir, 'java', 'pom.xml')
    tree = ET.parse(pom_path)
    version = tree.getroot().find(
        'POM:version',
        namespaces={
            'POM': 'http://maven.apache.org/POM/4.0.0'
        }).text
    jar_path = os.path.join(
        arrow_dir, 'java', 'tools', 'target',
        'arrow-tools-{}-jar-with-dependencies.jar'.format(version))
    jar_path = os.getenv("ARROW_TOOLS_JAR", jar_path)
    jar_path += ":{}".format(os.path.join(arrow_dir,
                                          "java", "c/target/arrow-c-data-{}.jar".format(version)))
    kwargs = {}
    # This will be the default behaviour in jpype 0.8+
    kwargs['convertStrings'] = False
    jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jar_path, **kwargs)


class Bridge:
    def __init__(self):
        self.java_allocator = jpype.JPackage(
            "org").apache.arrow.memory.RootAllocator(sys.maxsize)
        self.java_c = jpype.JPackage("org").apache.arrow.c

    def java_to_python_field(self, jfield):
        c_schema = ffi.new("struct ArrowSchema*")
        ptr_schema = int(ffi.cast("uintptr_t", c_schema))
        self.java_c.Data.exportField(self.java_allocator, jfield, None,
                                     self.java_c.ArrowSchema.wrap(ptr_schema))
        return pa.Field._import_from_c(ptr_schema)

    def java_to_python_array(self, vector, dictionary_provider=None):
        c_schema = ffi.new("struct ArrowSchema*")
        ptr_schema = int(ffi.cast("uintptr_t", c_schema))
        c_array = ffi.new("struct ArrowArray*")
        ptr_array = int(ffi.cast("uintptr_t", c_array))
        self.java_c.Data.exportVector(self.java_allocator, vector, dictionary_provider, self.java_c.ArrowArray.wrap(
            ptr_array), self.java_c.ArrowSchema.wrap(ptr_schema))
        return pa.Array._import_from_c(ptr_array, ptr_schema)

    def java_to_python_record_batch(self, root):
        c_schema = ffi.new("struct ArrowSchema*")
        ptr_schema = int(ffi.cast("uintptr_t", c_schema))
        c_array = ffi.new("struct ArrowArray*")
        ptr_array = int(ffi.cast("uintptr_t", c_array))
        self.java_c.Data.exportVectorSchemaRoot(self.java_allocator, root, None, self.java_c.ArrowArray.wrap(
            ptr_array), self.java_c.ArrowSchema.wrap(ptr_schema))
        return pa.RecordBatch._import_from_c(ptr_array, ptr_schema)

    def java_to_python_reader(self, reader):
        c_stream = ffi.new("struct ArrowArrayStream*")
        ptr_stream = int(ffi.cast("uintptr_t", c_stream))
        self.java_c.Data.exportArrayStream(self.java_allocator, reader,
                                           self.java_c.ArrowArrayStream.wrap(ptr_stream))
        return pa.RecordBatchReader._import_from_c(ptr_stream)

    def python_to_java_field(self, field):
        c_schema = self.java_c.ArrowSchema.allocateNew(self.java_allocator)
        field._export_to_c(c_schema.memoryAddress())
        return self.java_c.Data.importField(self.java_allocator, c_schema, None)

    def python_to_java_array(self, array, dictionary_provider=None):
        c_schema = self.java_c.ArrowSchema.allocateNew(self.java_allocator)
        c_array = self.java_c.ArrowArray.allocateNew(self.java_allocator)
        array._export_to_c(c_array.memoryAddress(), c_schema.memoryAddress())
        return self.java_c.Data.importVector(self.java_allocator, c_array, c_schema, dictionary_provider)

    def python_to_java_record_batch(self, record_batch):
        c_schema = self.java_c.ArrowSchema.allocateNew(self.java_allocator)
        c_array = self.java_c.ArrowArray.allocateNew(self.java_allocator)
        record_batch._export_to_c(
            c_array.memoryAddress(), c_schema.memoryAddress())
        return self.java_c.Data.importVectorSchemaRoot(self.java_allocator, c_array, c_schema, None)

    def python_to_java_reader(self, reader):
        c_stream = self.java_c.ArrowArrayStream.allocateNew(self.java_allocator)
        reader._export_to_c(c_stream.memoryAddress())
        return self.java_c.Data.importArrayStream(self.java_allocator, c_stream)

    def close(self):
        self.java_allocator.close()


class TestPythonIntegration(unittest.TestCase):
    def setUp(self):
        gc.collect()
        self.old_allocated_python = pa.total_allocated_bytes()
        self.bridge = Bridge()

    def tearDown(self):
        self.bridge.close()
        gc.collect()
        diff_python = pa.total_allocated_bytes() - self.old_allocated_python
        self.assertEqual(
            pa.total_allocated_bytes(), self.old_allocated_python,
            f"PyArrow memory was not adequately released: {diff_python} bytes lost")

    def round_trip_field(self, field_generator):
        original_field = field_generator()
        java_field = self.bridge.python_to_java_field(original_field)
        del original_field
        new_field = self.bridge.java_to_python_field(java_field)
        del java_field

        expected = field_generator()
        self.assertEqual(expected, new_field)

    def round_trip_array(self, array_generator, check_metadata=True):
        original_arr = array_generator()
        with self.bridge.java_c.CDataDictionaryProvider() as dictionary_provider, \
                self.bridge.python_to_java_array(original_arr, dictionary_provider) as vector:
            del original_arr
            new_array = self.bridge.java_to_python_array(vector, dictionary_provider)

        expected = array_generator()

        self.assertEqual(expected, new_array)
        if check_metadata:
            self.assertTrue(new_array.type.equals(expected.type, check_metadata=True))

    def round_trip_record_batch(self, rb_generator):
        original_rb = rb_generator()
        with self.bridge.python_to_java_record_batch(original_rb) as root:
            del original_rb
            new_rb = self.bridge.java_to_python_record_batch(root)

        expected = rb_generator()
        self.assertEqual(expected, new_rb)

    def round_trip_reader(self, schema, batches):
        reader = pa.RecordBatchReader.from_batches(schema, batches)

        java_reader = self.bridge.python_to_java_reader(reader)
        del reader
        py_reader = self.bridge.java_to_python_reader(java_reader)
        del java_reader

        actual = list(py_reader)
        self.assertEqual(batches, actual)

    def test_string_array(self):
        self.round_trip_array(lambda: pa.array([None, "a", "bb", "ccc"]))

    def test_decimal_array(self):
        data = [
            round(decimal.Decimal(722.82), 2),
            round(decimal.Decimal(-934.11), 2),
            None,
        ]
        self.round_trip_array(lambda: pa.array(data, pa.decimal128(5, 2)))

    def test_int_array(self):
        self.round_trip_array(lambda: pa.array([1, 2, 3], type=pa.int32()))

    def test_list_array(self):
        self.round_trip_array(lambda: pa.array(
            [[], [0], [1, 2], [4, 5, 6]], pa.list_(pa.int64())
            # disabled check_metadata since the list internal field name ("item")
            # is not preserved during round trips (it becomes "$data$").
        ), check_metadata=False)
        

    def test_struct_array(self):
        fields = [
            ("f1", pa.int32()),
            ("f2", pa.string()),
        ]
        data = [
            {"f1": 1, "f2": "a"},
            None,
            {"f1": 3, "f2": None},
            {"f1": None, "f2": "d"},
            {"f1": None, "f2": None},
        ]
        self.round_trip_array(lambda: pa.array(data, type=pa.struct(fields)))

    def test_dict(self):
        self.round_trip_array(
            lambda: pa.array(["a", "b", None, "d"], pa.dictionary(pa.int64(), pa.utf8())))

    def test_map(self):
        offsets = [0, None, 2, 6]
        pykeys = [b"a", b"b", b"c", b"d", b"e", b"f"]
        pyitems = [1, 2, 3, None, 4, 5]
        keys = pa.array(pykeys, type="binary")
        items = pa.array(pyitems, type="i4")
        self.round_trip_array(
            lambda: pa.MapArray.from_arrays(offsets, keys, items))

    def test_field(self):
        self.round_trip_field(lambda: pa.field("aa", pa.bool_()))

    def test_field_nested(self):
        self.round_trip_field(lambda: pa.field(
            "test", pa.list_(pa.int32()), nullable=True))

    def test_field_metadata(self):
        self.round_trip_field(lambda: pa.field("aa", pa.bool_(), {"a": "b"}))

    def test_record_batch_with_list(self):
        data = [
            pa.array([[1], [2], [3], [4, 5, 6]]),
            pa.array([1, 2, 3, 4]),
            pa.array(['foo', 'bar', 'baz', None]),
            pa.array([True, None, False, True])
        ]
        self.round_trip_record_batch(
            lambda: pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2', 'f3']))

    def test_reader_roundtrip(self):
        schema = pa.schema([("ints", pa.int64()), ("strs", pa.string())])
        data = [
            pa.record_batch([[1, 2, 3, None],
                             ["a", "bc", None, ""]],
                            schema=schema),
            pa.record_batch([[None, 4, 5, 6],
                             [None, "", "def", "g"]],
                            schema=schema),
        ]
        self.round_trip_reader(schema, data)

    def test_reader_complex_roundtrip(self):
        schema = pa.schema([
            ("str_dict", pa.dictionary(pa.int8(), pa.string())),
            ("int_list", pa.list_(pa.int64())),
        ])
        dictionary = pa.array(["a", "bc", None])
        data = [
            pa.record_batch([pa.DictionaryArray.from_arrays([0, 2], dictionary),
                             [[1, 2, 3], None]],
                            schema=schema),
            pa.record_batch([pa.DictionaryArray.from_arrays([None, 1], dictionary),
                             [[], [4]]],
                            schema=schema),
        ]
        self.round_trip_reader(schema, data)


if __name__ == '__main__':
    setup_jvm()
    unittest.main(verbosity=2)
