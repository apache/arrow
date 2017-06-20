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


def load_version_from_pom():
    import xml.etree.ElementTree as ET
    tree = ET.parse(os.path.join(ARROW_HOME, 'java', 'pom.xml'))
    tag_pattern = '{http://maven.apache.org/POM/4.0.0}version'
    version_tag = list(tree.getroot().findall(tag_pattern))[0]
    return version_tag.text


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

    def __init__(self, name, is_signed, bit_width, nullable=True,
                 min_value=TEST_INT_MIN,
                 max_value=TEST_INT_MAX):
        PrimitiveType.__init__(self, name, nullable=nullable)
        self.is_signed = is_signed
        self.bit_width = bit_width
        self.min_value = min_value
        self.max_value = max_value

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
                  np.random.randint(max(iinfo.min, self.min_value),
                                    min(iinfo.max, self.max_value),
                                    size=size)]

        is_valid = self._make_is_valid(size)
        return PrimitiveColumn(self.name, size, is_valid, values)


class DateType(IntegerType):

    DAY = 0
    MILLISECOND = 1

    def __init__(self, name, unit, nullable=True):
        self.unit = unit
        bit_width = 32 if unit == self.DAY else 64
        IntegerType.__init__(self, name, True, bit_width, nullable=nullable)

    def _get_type(self):
        return OrderedDict([
            ('name', 'date'),
            ('unit', 'DAY' if self.unit == self.DAY else 'MILLISECOND')
        ])


TIMEUNIT_NAMES = {
    's': 'SECOND',
    'ms': 'MILLISECOND',
    'us': 'MICROSECOND',
    'ns': 'NANOSECOND'
}


class TimeType(IntegerType):

    BIT_WIDTHS = {
        's': 32,
        'ms': 32,
        'us': 64,
        'ns': 64
    }

    def __init__(self, name, unit='s', nullable=True):
        self.unit = unit
        IntegerType.__init__(self, name, True, self.BIT_WIDTHS[unit],
                             nullable=nullable)

    def _get_type(self):
        return OrderedDict([
            ('name', 'time'),
            ('unit', TIMEUNIT_NAMES[self.unit]),
            ('bitWidth', self.bit_width)
        ])


class TimestampType(IntegerType):

    def __init__(self, name, unit='s', tz=None, nullable=True):
        self.unit = unit
        self.tz = tz
        IntegerType.__init__(self, name, True, 64, nullable=nullable)

    def _get_type(self):
        fields = [
            ('name', 'timestamp'),
            ('unit', TIMEUNIT_NAMES[self.unit])
        ]

        if self.tz is not None:
            fields.append(('timezone', self.tz))

        return OrderedDict(fields)


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


class BinaryType(PrimitiveType):

    @property
    def numpy_type(self):
        return object

    @property
    def column_class(self):
        return BinaryColumn

    def _get_type(self):
        return OrderedDict([('name', 'binary')])

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
                draw = (np.random.randint(0, 255, size=K)
                        .astype(np.uint8)
                        .tostring())
                values.append(draw)
            else:
                values.append("")

        return self.column_class(self.name, size, is_valid, values)


class StringType(BinaryType):

    @property
    def column_class(self):
        return StringColumn

    def _get_type(self):
        return OrderedDict([('name', 'utf8')])

    def generate_column(self, size):
        K = 7
        is_valid = self._make_is_valid(size)
        values = []

        for i in range(size):
            if is_valid[i]:
                values.append(rands(K))
            else:
                values.append("")

        return self.column_class(self.name, size, is_valid, values)


class JSONSchema(object):

    def __init__(self, fields):
        self.fields = fields

    def get_json(self):
        return OrderedDict([
            ('fields', [field.get_json() for field in self.fields])
        ])


class BinaryColumn(PrimitiveColumn):

    def _encode_value(self, x):
        return ''.join('{:02x}'.format(c).upper() for c in x)

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
            data.append(self._encode_value(v))

        return [
            ('VALIDITY', [int(x) for x in self.is_valid]),
            ('OFFSET', offsets),
            ('DATA', data)
        ]


class StringColumn(BinaryColumn):

    def _encode_value(self, x):
        return x


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

    def __init__(self, name, schema, batches):
        self.name = name
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
    if type_ == 'binary':
        return BinaryType(name, nullable=nullable)
    elif type_ == 'utf8':
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


def _generate_file(name, fields, batch_sizes):
    schema = JSONSchema(fields)
    batches = []
    for size in batch_sizes:
        columns = []
        for field in fields:
            col = field.generate_column(size)
            columns.append(col)

        batches.append(JSONRecordBatch(size, columns))

    return JSONFile(name, schema, batches)


def generate_primitive_case(batch_sizes):
    types = ['bool', 'int8', 'int16', 'int32', 'int64',
             'uint8', 'uint16', 'uint32', 'uint64',
             'float32', 'float64', 'binary', 'utf8']

    fields = []

    for type_ in types:
        fields.append(get_field(type_ + "_nullable", type_, True))
        fields.append(get_field(type_ + "_nonnullable", type_, False))

    return _generate_file("primitive", fields, batch_sizes)


def generate_datetime_case():
    fields = [
        DateType('f0', DateType.DAY),
        DateType('f1', DateType.MILLISECOND),
        TimeType('f2', 's'),
        TimeType('f3', 'ms'),
        TimeType('f4', 'us'),
        TimeType('f5', 'ns'),
        TimestampType('f6', 's'),
        TimestampType('f7', 'ms'),
        TimestampType('f8', 'us'),
        TimestampType('f9', 'ns'),
        TimestampType('f10', 'ms', tz=None),
        TimestampType('f11', 's', tz='UTC'),
        TimestampType('f12', 'ms', tz='US/Eastern'),
        TimestampType('f13', 'us', tz='Europe/Paris'),
        TimestampType('f14', 'ns', tz='US/Pacific')
    ]

    batch_sizes = [7, 10]
    return _generate_file("datetime", fields, batch_sizes)


def generate_nested_case():
    fields = [
        ListType('list_nullable', get_field('item', 'int32')),
        StructType('struct_nullable', [get_field('f1', 'int32'),
                                       get_field('f2', 'utf8')]),

        # TODO(wesm): this causes segfault
        # ListType('list_nonnullable', get_field('item', 'int32'), False),
    ]

    batch_sizes = [7, 10]
    return _generate_file("nested", fields, batch_sizes)


def get_generated_json_files():
    temp_dir = tempfile.mkdtemp()

    def _temp_path():
        return

    file_objs = [
        generate_primitive_case([7, 10]),
        generate_primitive_case([0, 0, 0]),
        generate_datetime_case(),
        generate_nested_case()
    ]

    generated_paths = []
    for file_obj in file_objs:
        out_path = os.path.join(temp_dir, 'generated_' + file_obj.name + '.json')
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
            self._compare_implementations(producer, consumer)

    def _compare_implementations(self, producer, consumer):
        print('-- {0} producing, {1} consuming'.format(producer.name,
                                                       consumer.name))

        for json_path in self.json_files:
            print('=====================================================================================')
            print('Testing file {0}'.format(json_path))
            print('=====================================================================================')

            name = os.path.splitext(os.path.basename(json_path))[0]

            # Make the random access file
            print('-- Creating binary inputs')
            producer_file_path = os.path.join(self.temp_dir, guid() + '_' + name + '.json_to_arrow')
            producer.json_to_file(json_path, producer_file_path)

            # Validate the file
            print('-- Validating file')
            consumer.validate(json_path, producer_file_path)

            print('-- Validating stream')
            producer_stream_path = os.path.join(self.temp_dir, guid() + '_' + name + '.arrow_to_stream')
            consumer_file_path = os.path.join(self.temp_dir, guid() + '_' + name + '.stream_to_arrow')
            producer.file_to_stream(producer_file_path,
                                    producer_stream_path)
            consumer.stream_to_file(producer_stream_path,
                                    consumer_file_path)
            consumer.validate(json_path, consumer_file_path)


class Tester(object):

    def __init__(self, debug=False):
        self.debug = debug

    def json_to_file(self, json_path, arrow_path):
        raise NotImplementedError

    def stream_to_file(self, stream_path, file_path):
        raise NotImplementedError

    def file_to_stream(self, file_path, stream_path):
        raise NotImplementedError

    def validate(self, json_path, arrow_path):
        raise NotImplementedError


class JavaTester(Tester):

    _arrow_version = load_version_from_pom()
    ARROW_TOOLS_JAR = os.environ.get(
        'ARROW_JAVA_INTEGRATION_JAR',
        os.path.join(ARROW_HOME,
                     'java/tools/target/arrow-tools-{}-'
                     'jar-with-dependencies.jar'.format(_arrow_version)))

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

        run_cmd(cmd)

    def validate(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'VALIDATE')

    def json_to_file(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'JSON_TO_ARROW')

    def stream_to_file(self, stream_path, file_path):
        cmd = ['java', '-cp', self.ARROW_TOOLS_JAR,
               'org.apache.arrow.tools.StreamToFile',
               stream_path, file_path]
        if self.debug:
            print(' '.join(cmd))
        run_cmd(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = ['java', '-cp', self.ARROW_TOOLS_JAR,
               'org.apache.arrow.tools.FileToStream',
               file_path, stream_path]
        if self.debug:
            print(' '.join(cmd))
        run_cmd(cmd)


class CPPTester(Tester):

    EXE_PATH = os.environ.get(
        'ARROW_CPP_EXE_PATH',
        os.path.join(ARROW_HOME, 'cpp/test-build/debug'))

    CPP_INTEGRATION_EXE = os.path.join(EXE_PATH, 'json-integration-test')
    STREAM_TO_FILE = os.path.join(EXE_PATH, 'stream-to-file')
    FILE_TO_STREAM = os.path.join(EXE_PATH, 'file-to-stream')

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

        run_cmd(cmd)

    def validate(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'VALIDATE')

    def json_to_file(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'JSON_TO_ARROW')

    def stream_to_file(self, stream_path, file_path):
        cmd = ['cat', stream_path, '|', self.STREAM_TO_FILE, '>', file_path]
        cmd = ' '.join(cmd)
        if self.debug:
            print(cmd)
        os.system(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = [self.FILE_TO_STREAM, file_path, '>', stream_path]
        cmd = ' '.join(cmd)
        if self.debug:
            print(cmd)
        os.system(cmd)


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
