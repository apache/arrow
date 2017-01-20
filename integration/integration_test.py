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

from collections import OrderedDict
import argparse
import glob
import itertools
import json
import os
import six
import string
import subprocess
import tempfile
import uuid

import numpy as np

ARROW_HOME = os.path.abspath(__file__).rsplit("/", 2)[0]

# Control for flakiness
np.random.seed(12345)


def guid():
    return uuid.uuid4().hex


# from pandas
RANDS_CHARS = np.array(list(string.ascii_letters + string.digits),
                       dtype=(np.str_, 1))


def rands(nchars):
    """
    Generate one random byte string.

    See `rands_array` if you want to create an array of random strings.

    """
    return ''.join(np.random.choice(RANDS_CHARS, nchars))


def str_from_bytes(x):
    if six.PY2:
        return x
    else:
        return x.decode('utf-8')


# from the merge_arrow_pr.py script
def run_cmd(cmd):
    if isinstance(cmd, six.string_types):
        cmd = cmd.split(' ')

    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        # this avoids hiding the stdout / stderr of failed processes
        print('Command failed: %s' % ' '.join(cmd))
        print('With output:')
        print('--------------')
        print(str_from_bytes(e.output))
        print('--------------')
        raise e

    return str_from_bytes(output)

# ----------------------------------------------------------------------
# Data generation


class DataType(object):

    def __init__(self, name, nullable=True):
        self.name = name
        self.nullable = nullable

    def get_json(self):
        return OrderedDict([
            ('name', self.name),
            ('type', self._get_type()),
            ('nullable', self.nullable),
            ('children', self._get_children()),
            ('typeLayout', self._get_type_layout())
        ])

    def _make_is_valid(self, size):
        if self.nullable:
            return np.random.randint(0, 2, size=size)
        else:
            return np.ones(size)


class Column(object):

    def __init__(self, name, count):
        self.name = name
        self.count = count

    def _get_children(self):
        return []

    def _get_buffers(self):
        return []

    def get_json(self):
        entries = [
            ('name', self.name),
            ('count', self.count)
        ]

        buffers = self._get_buffers()
        entries.extend(buffers)

        children = self._get_children()
        if len(children) > 0:
            entries.append(('children', children))

        return OrderedDict(entries)


class PrimitiveType(DataType):

    def _get_children(self):
        return []

    def _get_type_layout(self):
        return OrderedDict([
            ('vectors',
             [OrderedDict([('type', 'VALIDITY'),
                           ('typeBitWidth', 1)]),
              OrderedDict([('type', 'DATA'),
                           ('typeBitWidth', self.bit_width)])])])


class PrimitiveColumn(Column):

    def __init__(self, name, count, is_valid, values):
        Column.__init__(self, name, count)
        self.is_valid = is_valid
        self.values = values

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid]),
            ('DATA', list(self.values))
        ]


TEST_INT_MIN = - 2**31 + 1
TEST_INT_MAX = 2**31 - 1


class IntegerType(PrimitiveType):

    def __init__(self, name, is_signed, bit_width, nullable=True):
        PrimitiveType.__init__(self, name, nullable=nullable)
        self.is_signed = is_signed
        self.bit_width = bit_width

    @property
    def numpy_type(self):
        return ('int' if self.is_signed else 'uint') + str(self.bit_width)

    def _get_type(self):
        return OrderedDict([
            ('name', 'int'),
            ('isSigned', self.is_signed),
            ('bitWidth', self.bit_width)
        ])

    def generate_column(self, size):
        iinfo = np.iinfo(self.numpy_type)
        values = [int(x) for x in
                  np.random.randint(max(iinfo.min, TEST_INT_MIN),
                                    min(iinfo.max, TEST_INT_MAX),
                                    size=size)]

        is_valid = self._make_is_valid(size)
        return PrimitiveColumn(self.name, size, is_valid, values)


class FloatingPointType(PrimitiveType):

    def __init__(self, name, bit_width, nullable=True):
        PrimitiveType.__init__(self, name, nullable=nullable)

        self.bit_width = bit_width
        self.precision = {
            16: 'HALF',
            32: 'SINGLE',
            64: 'DOUBLE'
        }[self.bit_width]

    @property
    def numpy_type(self):
        return 'float' + str(self.bit_width)

    def _get_type(self):
        return OrderedDict([
            ('name', 'floatingpoint'),
            ('precision', self.precision)
        ])

    def generate_column(self, size):
        values = np.random.randn(size) * 1000
        values = np.round(values, 3)

        is_valid = self._make_is_valid(size)
        return PrimitiveColumn(self.name, size, is_valid, values)


class BooleanType(PrimitiveType):

    bit_width = 1

    def _get_type(self):
        return OrderedDict([('name', 'bool')])

    @property
    def numpy_type(self):
        return 'bool'

    def generate_column(self, size):
        values = list(map(bool, np.random.randint(0, 2, size=size)))
        is_valid = self._make_is_valid(size)
        return PrimitiveColumn(self.name, size, is_valid, values)


class StringType(PrimitiveType):

    @property
    def numpy_type(self):
        return object

    def _get_type(self):
        return OrderedDict([('name', 'utf8')])

    def _get_type_layout(self):
        return OrderedDict([
            ('vectors',
             [OrderedDict([('type', 'VALIDITY'),
                           ('typeBitWidth', 1)]),
              OrderedDict([('type', 'OFFSET'),
                           ('typeBitWidth', 32)]),
              OrderedDict([('type', 'DATA'),
                           ('typeBitWidth', 8)])])])

    def generate_column(self, size):
        K = 7
        is_valid = self._make_is_valid(size)
        values = []

        for i in range(size):
            if is_valid[i]:
                values.append(rands(K))
            else:
                values.append("")

        return StringColumn(self.name, size, is_valid, values)


class JSONSchema(object):

    def __init__(self, fields):
        self.fields = fields

    def get_json(self):
        return OrderedDict([
            ('fields', [field.get_json() for field in self.fields])
        ])


class StringColumn(PrimitiveColumn):

    def _get_buffers(self):
        offset = 0
        offsets = [0]

        data = []
        for i, v in enumerate(self.values):
            if self.is_valid[i]:
                offset += len(v)
            else:
                v = ""

            offsets.append(offset)
            data.append(v)

        return [
            ('VALIDITY', [int(x) for x in self.is_valid]),
            ('OFFSET', offsets),
            ('DATA', data)
        ]


class ListType(DataType):

    def __init__(self, name, value_type, nullable=True):
        DataType.__init__(self, name, nullable=nullable)
        self.value_type = value_type

    def _get_type(self):
        return OrderedDict([
            ('name', 'list')
        ])

    def _get_children(self):
        return [self.value_type.get_json()]

    def _get_type_layout(self):
        return OrderedDict([
            ('vectors',
             [OrderedDict([('type', 'VALIDITY'),
                           ('typeBitWidth', 1)]),
              OrderedDict([('type', 'OFFSET'),
                           ('typeBitWidth', 32)])])])

    def generate_column(self, size):
        MAX_LIST_SIZE = 4

        is_valid = self._make_is_valid(size)
        list_sizes = np.random.randint(0, MAX_LIST_SIZE + 1, size=size)
        offsets = [0]

        offset = 0
        for i in range(size):
            if is_valid[i]:
                offset += int(list_sizes[i])
            offsets.append(offset)

        # The offset now is the total number of elements in the child array
        values = self.value_type.generate_column(offset)

        return ListColumn(self.name, size, is_valid, offsets, values)


class ListColumn(Column):

    def __init__(self, name, count, is_valid, offsets, values):
        Column.__init__(self, name, count)
        self.is_valid = is_valid
        self.offsets = offsets
        self.values = values

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid]),
            ('OFFSET', list(self.offsets))
        ]

    def _get_children(self):
        return [self.values.get_json()]


class StructType(DataType):

    def __init__(self, name, field_types, nullable=True):
        DataType.__init__(self, name, nullable=nullable)
        self.field_types = field_types

    def _get_type(self):
        return OrderedDict([
            ('name', 'struct')
        ])

    def _get_children(self):
        return [type_.get_json() for type_ in self.field_types]

    def _get_type_layout(self):
        return OrderedDict([
            ('vectors',
             [OrderedDict([('type', 'VALIDITY'),
                           ('typeBitWidth', 1)])])])

    def generate_column(self, size):
        is_valid = self._make_is_valid(size)

        field_values = [type_.generate_column(size)
                        for type_ in self.field_types]

        return StructColumn(self.name, size, is_valid, field_values)


class StructColumn(Column):

    def __init__(self, name, count, is_valid, field_values):
        Column.__init__(self, name, count)
        self.is_valid = is_valid
        self.field_values = field_values

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid])
        ]

    def _get_children(self):
        return [field.get_json() for field in self.field_values]


class JSONRecordBatch(object):

    def __init__(self, count, columns):
        self.count = count
        self.columns = columns

    def get_json(self):
        return OrderedDict([
            ('count', self.count),
            ('columns', [col.get_json() for col in self.columns])
        ])


class JSONFile(object):

    def __init__(self, schema, batches):
        self.schema = schema
        self.batches = batches

    def get_json(self):
        return OrderedDict([
            ('schema', self.schema.get_json()),
            ('batches', [batch.get_json() for batch in self.batches])
        ])

    def write(self, path):
        with open(path, 'wb') as f:
            f.write(json.dumps(self.get_json(), indent=2).encode('utf-8'))


def get_field(name, type_, nullable=True):
    if type_ == 'utf8':
        return StringType(name, nullable=nullable)

    dtype = np.dtype(type_)

    if dtype.kind in ('i', 'u'):
        return IntegerType(name, dtype.kind == 'i', dtype.itemsize * 8,
                           nullable=nullable)
    elif dtype.kind == 'f':
        return FloatingPointType(name, dtype.itemsize * 8,
                                 nullable=nullable)
    elif dtype.kind == 'b':
        return BooleanType(name, nullable=nullable)
    else:
        raise TypeError(dtype)


def generate_primitive_case():
    types = ['bool', 'int8', 'int16', 'int32', 'int64',
             'uint8', 'uint16', 'uint32', 'uint64',
             'float32', 'float64', 'utf8']

    fields = []

    for type_ in types:
        fields.append(get_field(type_ + "_nullable", type_, True))
        fields.append(get_field(type_ + "_nonnullable", type_, False))

    schema = JSONSchema(fields)

    batch_sizes = [7, 10]
    batches = []
    for size in batch_sizes:
        columns = []
        for field in fields:
            col = field.generate_column(size)
            columns.append(col)

        batches.append(JSONRecordBatch(size, columns))

    return JSONFile(schema, batches)


def generate_nested_case():
    fields = [
        ListType('list_nullable', get_field('item', 'int32')),
        StructType('struct_nullable', [get_field('f1', 'int32'),
                                       get_field('f2', 'utf8')]),

        # TODO(wesm): this causes segfault
        # ListType('list_nonnullable', get_field('item', 'int32'), False),
    ]

    schema = JSONSchema(fields)

    batch_sizes = [7, 10]
    batches = []
    for size in batch_sizes:
        columns = []
        for field in fields:
            col = field.generate_column(size)
            columns.append(col)

        batches.append(JSONRecordBatch(size, columns))

    return JSONFile(schema, batches)


def get_generated_json_files():
    temp_dir = tempfile.mkdtemp()

    def _temp_path():
        return

    file_objs = []

    K = 10
    for i in range(K):
        file_objs.append(generate_primitive_case())

    file_objs.append(generate_nested_case())

    generated_paths = []
    for file_obj in file_objs:
        out_path = os.path.join(temp_dir, guid() + '.json')
        file_obj.write(out_path)
        generated_paths.append(out_path)

    return generated_paths


# ----------------------------------------------------------------------
# Testing harness


class IntegrationRunner(object):

    def __init__(self, json_files, testers, debug=False):
        self.json_files = json_files
        self.testers = testers
        self.temp_dir = tempfile.mkdtemp()
        self.debug = debug

    def run(self):
        for producer, consumer in itertools.product(self.testers,
                                                    self.testers):
            if producer is consumer:
                continue

            print('-- {0} producing, {1} consuming'.format(producer.name,
                                                           consumer.name))

            for json_path in self.json_files:
                print('Testing with {0}'.format(json_path))

                arrow_path = os.path.join(self.temp_dir, guid())

                producer.json_to_arrow(json_path, arrow_path)
                consumer.validate(json_path, arrow_path)


class Tester(object):

    def __init__(self, debug=False):
        self.debug = debug

    def json_to_arrow(self, json_path, arrow_path):
        raise NotImplementedError

    def validate(self, json_path, arrow_path):
        raise NotImplementedError


class JavaTester(Tester):

    ARROW_TOOLS_JAR = os.environ.get(
        'ARROW_JAVA_INTEGRATION_JAR',
        os.path.join(ARROW_HOME,
                     'java/tools/target/arrow-tools-0.1.1-'
                     'SNAPSHOT-jar-with-dependencies.jar'))

    name = 'Java'

    def _run(self, arrow_path=None, json_path=None, command='VALIDATE'):
        cmd = ['java', '-cp', self.ARROW_TOOLS_JAR,
               'org.apache.arrow.tools.Integration']

        if arrow_path is not None:
            cmd.extend(['-a', arrow_path])

        if json_path is not None:
            cmd.extend(['-j', json_path])

        cmd.extend(['-c', command])

        if self.debug:
            print(' '.join(cmd))

        return run_cmd(cmd)

    def validate(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'VALIDATE')

    def json_to_arrow(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'JSON_TO_ARROW')


class CPPTester(Tester):

    CPP_INTEGRATION_EXE = os.environ.get(
        'ARROW_CPP_TESTER',
        os.path.join(ARROW_HOME,
                     'cpp/test-build/debug/json-integration-test'))

    name = 'C++'

    def _run(self, arrow_path=None, json_path=None, command='VALIDATE'):
        cmd = [self.CPP_INTEGRATION_EXE, '--integration']

        if arrow_path is not None:
            cmd.append('--arrow=' + arrow_path)

        if json_path is not None:
            cmd.append('--json=' + json_path)

        cmd.append('--mode=' + command)

        if self.debug:
            print(' '.join(cmd))

        return run_cmd(cmd)

    def validate(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'VALIDATE')

    def json_to_arrow(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'JSON_TO_ARROW')


def get_static_json_files():
    glob_pattern = os.path.join(ARROW_HOME, 'integration', 'data', '*.json')
    return glob.glob(glob_pattern)


def run_all_tests(debug=False):
    testers = [CPPTester(debug=debug), JavaTester(debug=debug)]
    static_json_files = get_static_json_files()
    generated_json_files = get_generated_json_files()
    json_files = static_json_files + generated_json_files

    runner = IntegrationRunner(json_files, testers, debug=debug)
    runner.run()
    print('-- All tests passed!')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Arrow integration test CLI')
    parser.add_argument('--debug', dest='debug', action='store_true',
                        default=False,
                        help='Run executables in debug mode as relevant')

    args = parser.parse_args()
    run_all_tests(debug=args.debug)
