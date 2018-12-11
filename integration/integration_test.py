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
import binascii
import glob
import itertools
import json
import os
import random
import six
import string
import subprocess
import tempfile
import uuid
import errno

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


def tobytes(o):
    if isinstance(o, six.text_type):
        return o.encode('utf8')
    return o


def frombytes(o):
    if isinstance(o, six.binary_type):
        return o.decode('utf8')
    return o


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
        print(frombytes(e.output))
        print('--------------')
        raise e

    return frombytes(output)

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
            ('children', self._get_children())
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

    def __len__(self):
        return self.count

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


class PrimitiveColumn(Column):

    def __init__(self, name, count, is_valid, values):
        super(PrimitiveColumn, self).__init__(name, count)
        self.is_valid = is_valid
        self.values = values

    def _encode_value(self, x):
        return x

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid]),
            ('DATA', list([self._encode_value(x) for x in self.values]))
        ]


TEST_INT_MAX = 2 ** 31 - 1
TEST_INT_MIN = ~TEST_INT_MAX


class IntegerType(PrimitiveType):

    def __init__(self, name, is_signed, bit_width, nullable=True,
                 min_value=TEST_INT_MIN,
                 max_value=TEST_INT_MAX):
        super(IntegerType, self).__init__(name, nullable=nullable)
        self.is_signed = is_signed
        self.bit_width = bit_width
        self.min_value = min_value
        self.max_value = max_value

    def _get_generated_data_bounds(self):
        signed_iinfo = np.iinfo('int' + str(self.bit_width))
        if self.is_signed:
            min_value, max_value = signed_iinfo.min, signed_iinfo.max
        else:
            # ARROW-1837 Remove this hack and restore full unsigned integer
            # range
            min_value, max_value = 0, signed_iinfo.max

        lower_bound = max(min_value, self.min_value)
        upper_bound = min(max_value, self.max_value)
        return lower_bound, upper_bound

    def _get_type(self):
        return OrderedDict([
            ('name', 'int'),
            ('isSigned', self.is_signed),
            ('bitWidth', self.bit_width)
        ])

    def generate_column(self, size, name=None):
        lower_bound, upper_bound = self._get_generated_data_bounds()
        return self.generate_range(size, lower_bound, upper_bound, name=name)

    def generate_range(self, size, lower, upper, name=None):
        values = [int(x) for x in
                  np.random.randint(lower, upper, size=size)]

        is_valid = self._make_is_valid(size)

        if name is None:
            name = self.name
        return PrimitiveColumn(name, size, is_valid, values)


class DateType(IntegerType):

    DAY = 0
    MILLISECOND = 1

    # 1/1/1 to 12/31/9999
    _ranges = {
        DAY: [-719162, 2932896],
        MILLISECOND: [-62135596800000, 253402214400000]
    }

    def __init__(self, name, unit, nullable=True):
        bit_width = 32 if unit == self.DAY else 64

        min_value, max_value = self._ranges[unit]
        super(DateType, self).__init__(
            name, True, bit_width, nullable=nullable,
            min_value=min_value, max_value=max_value
        )
        self.unit = unit

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

    _ranges = {
        's': [0, 86400],
        'ms': [0, 86400000],
        'us': [0, 86400000000],
        'ns': [0, 86400000000000]
    }

    def __init__(self, name, unit='s', nullable=True):
        min_val, max_val = self._ranges[unit]
        super(TimeType, self).__init__(name, True, self.BIT_WIDTHS[unit],
                                       nullable=nullable,
                                       min_value=min_val,
                                       max_value=max_val)
        self.unit = unit

    def _get_type(self):
        return OrderedDict([
            ('name', 'time'),
            ('unit', TIMEUNIT_NAMES[self.unit]),
            ('bitWidth', self.bit_width)
        ])


class TimestampType(IntegerType):

    # 1/1/1 to 12/31/9999
    _ranges = {
        's': [-62135596800, 253402214400],
        'ms': [-62135596800000, 253402214400000],
        'us': [-62135596800000000, 253402214400000000],

        # Physical range for int64, ~584 years and change
        'ns': [np.iinfo('int64').min, np.iinfo('int64').max]
    }

    def __init__(self, name, unit='s', tz=None, nullable=True):
        min_val, max_val = self._ranges[unit]
        super(TimestampType, self).__init__(name, True, 64, nullable=nullable,
                                            min_value=min_val,
                                            max_value=max_val)
        self.unit = unit
        self.tz = tz

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
        super(FloatingPointType, self).__init__(name, nullable=nullable)

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

    def generate_column(self, size, name=None):
        values = np.random.randn(size) * 1000
        values = np.round(values, 3)

        is_valid = self._make_is_valid(size)
        if name is None:
            name = self.name
        return PrimitiveColumn(name, size, is_valid, values)


DECIMAL_PRECISION_TO_VALUE = {
    key: (1 << (8 * i - 1)) - 1 for i, key in enumerate(
        [1, 3, 5, 7, 10, 12, 15, 17, 19, 22, 24, 27, 29, 32, 34, 36],
        start=1,
    )
}


def decimal_range_from_precision(precision):
    assert 1 <= precision <= 38
    try:
        max_value = DECIMAL_PRECISION_TO_VALUE[precision]
    except KeyError:
        return decimal_range_from_precision(precision - 1)
    else:
        return ~max_value, max_value


class DecimalType(PrimitiveType):
    def __init__(self, name, precision, scale, bit_width=128, nullable=True):
        super(DecimalType, self).__init__(name, nullable=True)
        self.precision = precision
        self.scale = scale
        self.bit_width = bit_width

    @property
    def numpy_type(self):
        return object

    def _get_type(self):
        return OrderedDict([
            ('name', 'decimal'),
            ('precision', self.precision),
            ('scale', self.scale),
        ])

    def generate_column(self, size, name=None):
        min_value, max_value = decimal_range_from_precision(self.precision)
        values = [random.randint(min_value, max_value) for _ in range(size)]

        is_valid = self._make_is_valid(size)
        if name is None:
            name = self.name
        return DecimalColumn(name, size, is_valid, values, self.bit_width)


class DecimalColumn(PrimitiveColumn):

    def __init__(self, name, count, is_valid, values, bit_width=128):
        super(DecimalColumn, self).__init__(name, count, is_valid, values)
        self.bit_width = bit_width

    def _encode_value(self, x):
        return str(x)


class BooleanType(PrimitiveType):
    bit_width = 1

    def _get_type(self):
        return OrderedDict([('name', 'bool')])

    @property
    def numpy_type(self):
        return 'bool'

    def generate_column(self, size, name=None):
        values = list(map(bool, np.random.randint(0, 2, size=size)))
        is_valid = self._make_is_valid(size)
        if name is None:
            name = self.name
        return PrimitiveColumn(name, size, is_valid, values)


class BinaryType(PrimitiveType):

    @property
    def numpy_type(self):
        return object

    @property
    def column_class(self):
        return BinaryColumn

    def _get_type(self):
        return OrderedDict([('name', 'binary')])

    def generate_column(self, size, name=None):
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
                values.append(b"")

        if name is None:
            name = self.name
        return self.column_class(name, size, is_valid, values)


class FixedSizeBinaryType(PrimitiveType):

    def __init__(self, name, byte_width, nullable=True):
        super(FixedSizeBinaryType, self).__init__(name, nullable=nullable)
        self.byte_width = byte_width

    @property
    def numpy_type(self):
        return object

    @property
    def column_class(self):
        return FixedSizeBinaryColumn

    def _get_type(self):
        return OrderedDict([('name', 'fixedsizebinary'),
                            ('byteWidth', self.byte_width)])

    def _get_type_layout(self):
        return OrderedDict([
            ('vectors',
             [OrderedDict([('type', 'VALIDITY'),
                           ('typeBitWidth', 1)]),
              OrderedDict([('type', 'DATA'),
                           ('typeBitWidth', self.byte_width)])])])

    def generate_column(self, size, name=None):
        is_valid = self._make_is_valid(size)
        values = []

        for i in range(size):
            draw = (np.random.randint(0, 255, size=self.byte_width)
                    .astype(np.uint8)
                    .tostring())
            values.append(draw)

        if name is None:
            name = self.name
        return self.column_class(name, size, is_valid, values)


class StringType(BinaryType):

    @property
    def column_class(self):
        return StringColumn

    def _get_type(self):
        return OrderedDict([('name', 'utf8')])

    def generate_column(self, size, name=None):
        K = 7
        is_valid = self._make_is_valid(size)
        values = []

        for i in range(size):
            if is_valid[i]:
                values.append(tobytes(rands(K)))
            else:
                values.append(b"")

        if name is None:
            name = self.name
        return self.column_class(name, size, is_valid, values)


class JsonSchema(object):

    def __init__(self, fields):
        self.fields = fields

    def get_json(self):
        return OrderedDict([
            ('fields', [field.get_json() for field in self.fields])
        ])


class BinaryColumn(PrimitiveColumn):

    def _encode_value(self, x):
        return frombytes(binascii.hexlify(x).upper())

    def _get_buffers(self):
        offset = 0
        offsets = [0]

        data = []
        for i, v in enumerate(self.values):
            if self.is_valid[i]:
                offset += len(v)
            else:
                v = b""

            offsets.append(offset)
            data.append(self._encode_value(v))

        return [
            ('VALIDITY', [int(x) for x in self.is_valid]),
            ('OFFSET', offsets),
            ('DATA', data)
        ]


class FixedSizeBinaryColumn(PrimitiveColumn):

    def _encode_value(self, x):
        return ''.join('{:02x}'.format(c).upper() for c in x)

    def _get_buffers(self):
        data = []
        for i, v in enumerate(self.values):
            data.append(self._encode_value(v))

        return [
            ('VALIDITY', [int(x) for x in self.is_valid]),
            ('DATA', data)
        ]


class StringColumn(BinaryColumn):

    def _encode_value(self, x):
        return frombytes(x)


class ListType(DataType):

    def __init__(self, name, value_type, nullable=True):
        super(ListType, self).__init__(name, nullable=nullable)
        self.value_type = value_type

    def _get_type(self):
        return OrderedDict([
            ('name', 'list')
        ])

    def _get_children(self):
        return [self.value_type.get_json()]

    def generate_column(self, size, name=None):
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

        if name is None:
            name = self.name
        return ListColumn(name, size, is_valid, offsets, values)


class ListColumn(Column):

    def __init__(self, name, count, is_valid, offsets, values):
        super(ListColumn, self).__init__(name, count)
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
        super(StructType, self).__init__(name, nullable=nullable)
        self.field_types = field_types

    def _get_type(self):
        return OrderedDict([
            ('name', 'struct')
        ])

    def _get_children(self):
        return [type_.get_json() for type_ in self.field_types]

    def generate_column(self, size, name=None):
        is_valid = self._make_is_valid(size)

        field_values = [type_.generate_column(size)
                        for type_ in self.field_types]
        if name is None:
            name = self.name
        return StructColumn(name, size, is_valid, field_values)


class Dictionary(object):

    def __init__(self, id_, field, values, ordered=False):
        self.id_ = id_
        self.field = field
        self.values = values
        self.ordered = ordered

    def __len__(self):
        return len(self.values)

    def get_json(self):
        dummy_batch = JsonRecordBatch(len(self.values), [self.values])
        return OrderedDict([
            ('id', self.id_),
            ('data', dummy_batch.get_json())
        ])


class DictionaryType(DataType):

    def __init__(self, name, index_type, dictionary, nullable=True):
        super(DictionaryType, self).__init__(name, nullable=nullable)
        assert isinstance(index_type, IntegerType)
        assert isinstance(dictionary, Dictionary)

        self.index_type = index_type
        self.dictionary = dictionary

    def get_json(self):
        dict_field = self.dictionary.field
        return OrderedDict([
            ('name', self.name),
            ('type', dict_field._get_type()),
            ('nullable', self.nullable),
            ('children', dict_field._get_children()),
            ('dictionary', OrderedDict([
                ('id', self.dictionary.id_),
                ('indexType', self.index_type._get_type()),
                ('isOrdered', self.dictionary.ordered)
            ]))
        ])

    def generate_column(self, size, name=None):
        if name is None:
            name = self.name
        return self.index_type.generate_range(size, 0, len(self.dictionary),
                                              name=name)


class StructColumn(Column):

    def __init__(self, name, count, is_valid, field_values):
        super(StructColumn, self).__init__(name, count)
        self.is_valid = is_valid
        self.field_values = field_values

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid])
        ]

    def _get_children(self):
        return [field.get_json() for field in self.field_values]


class JsonRecordBatch(object):

    def __init__(self, count, columns):
        self.count = count
        self.columns = columns

    def get_json(self):
        return OrderedDict([
            ('count', self.count),
            ('columns', [col.get_json() for col in self.columns])
        ])


class JsonFile(object):

    def __init__(self, name, schema, batches, dictionaries=None):
        self.name = name
        self.schema = schema
        self.dictionaries = dictionaries or []
        self.batches = batches

    def get_json(self):
        entries = [
            ('schema', self.schema.get_json())
        ]

        if len(self.dictionaries) > 0:
            entries.append(('dictionaries',
                            [dictionary.get_json()
                             for dictionary in self.dictionaries]))

        entries.append(('batches', [batch.get_json()
                                    for batch in self.batches]))
        return OrderedDict(entries)

    def write(self, path):
        with open(path, 'wb') as f:
            f.write(json.dumps(self.get_json(), indent=2).encode('utf-8'))


def get_field(name, type_, nullable=True):
    if type_ == 'binary':
        return BinaryType(name, nullable=nullable)
    elif type_ == 'utf8':
        return StringType(name, nullable=nullable)
    elif type_.startswith('fixedsizebinary_'):
        byte_width = int(type_.split('_')[1])
        return FixedSizeBinaryType(name,
                                   byte_width=byte_width,
                                   nullable=nullable)

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


def _generate_file(name, fields, batch_sizes, dictionaries=None):
    schema = JsonSchema(fields)
    batches = []
    for size in batch_sizes:
        columns = []
        for field in fields:
            col = field.generate_column(size)
            columns.append(col)

        batches.append(JsonRecordBatch(size, columns))

    return JsonFile(name, schema, batches, dictionaries)


def generate_primitive_case(batch_sizes, name='primitive'):
    types = ['bool', 'int8', 'int16', 'int32', 'int64',
             'uint8', 'uint16', 'uint32', 'uint64',
             'float32', 'float64', 'binary', 'utf8',
             'fixedsizebinary_19', 'fixedsizebinary_120']

    fields = []

    for type_ in types:
        fields.append(get_field(type_ + "_nullable", type_, True))
        fields.append(get_field(type_ + "_nonnullable", type_, False))

    return _generate_file(name, fields, batch_sizes)


def generate_decimal_case():
    fields = [
        DecimalType(name='f{}'.format(i), precision=precision, scale=2)
        for i, precision in enumerate(range(3, 39))
    ]

    possible_batch_sizes = 7, 10
    batch_sizes = [possible_batch_sizes[i % 2] for i in range(len(fields))]

    return _generate_file('decimal', fields, batch_sizes)


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


def generate_dictionary_case():
    dict_type1 = StringType('dictionary1')
    dict_type2 = get_field('dictionary2', 'int64')

    dict1 = Dictionary(0, dict_type1,
                       dict_type1.generate_column(10, name='DICT0'))
    dict2 = Dictionary(1, dict_type2,
                       dict_type2.generate_column(50, name='DICT1'))

    fields = [
        DictionaryType('dict1_0', get_field('', 'int8'), dict1),
        DictionaryType('dict1_1', get_field('', 'int32'), dict1),
        DictionaryType('dict2_0', get_field('', 'int16'), dict2)
    ]
    batch_sizes = [7, 10]
    return _generate_file("dictionary", fields, batch_sizes,
                          dictionaries=[dict1, dict2])


def get_generated_json_files():
    temp_dir = tempfile.mkdtemp()

    def _temp_path():
        return

    file_objs = [
        generate_primitive_case([17, 20], name='primitive'),
        generate_primitive_case([0, 0, 0], name='primitive_zerolength'),
        generate_decimal_case(),
        generate_datetime_case(),
        generate_nested_case(),
        generate_dictionary_case()
    ]

    generated_paths = []
    for file_obj in file_objs:
        out_path = os.path.join(temp_dir, 'generated_' +
                                file_obj.name + '.json')
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
        for producer, consumer in itertools.product(
                filter(lambda t: t.PRODUCER, self.testers),
                filter(lambda t: t.CONSUMER, self.testers)):
            self._compare_implementations(producer, consumer)

    def _compare_implementations(self, producer, consumer):
        print('##########################################################')
        print(
            '{0} producing, {1} consuming'.format(producer.name, consumer.name)
        )
        print('##########################################################')

        for json_path in self.json_files:
            print('==========================================================')
            print('Testing file {0}'.format(json_path))
            print('==========================================================')

            name = os.path.splitext(os.path.basename(json_path))[0]

            # Make the random access file
            print('-- Creating binary inputs')
            producer_file_path = os.path.join(self.temp_dir, guid() + '_' +
                                              name + '.json_to_arrow')
            producer.json_to_file(json_path, producer_file_path)

            # Validate the file
            print('-- Validating file')
            consumer.validate(json_path, producer_file_path)

            print('-- Validating stream')
            producer_stream_path = os.path.join(self.temp_dir, guid() + '_' +
                                                name + '.arrow_to_stream')
            consumer_file_path = os.path.join(self.temp_dir, guid() + '_' +
                                              name + '.stream_to_arrow')
            producer.file_to_stream(producer_file_path,
                                    producer_stream_path)
            consumer.stream_to_file(producer_stream_path,
                                    consumer_file_path)
            consumer.validate(json_path, consumer_file_path)


class Tester(object):
    PRODUCER = False
    CONSUMER = False

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
    PRODUCER = True
    CONSUMER = True

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
    PRODUCER = True
    CONSUMER = True

    EXE_PATH = os.environ.get(
        'ARROW_CPP_EXE_PATH',
        os.path.join(ARROW_HOME, 'cpp/build/debug'))

    CPP_INTEGRATION_EXE = os.path.join(EXE_PATH, 'arrow-json-integration-test')
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


class JSTester(Tester):
    PRODUCER = True
    CONSUMER = True

    EXE_PATH = os.path.join(ARROW_HOME, 'js/bin')
    VALIDATE = os.path.join(EXE_PATH, 'integration.js')
    JSON_TO_ARROW = os.path.join(EXE_PATH, 'json-to-arrow.js')
    STREAM_TO_FILE = os.path.join(EXE_PATH, 'stream-to-file.js')
    FILE_TO_STREAM = os.path.join(EXE_PATH, 'file-to-stream.js')

    name = 'JS'

    def _run(self, exe_cmd, arrow_path=None, json_path=None,
             command='VALIDATE'):
        cmd = [exe_cmd]

        if arrow_path is not None:
            cmd.extend(['-a', arrow_path])

        if json_path is not None:
            cmd.extend(['-j', json_path])

        cmd.extend(['--mode', command, '-t', 'es5', '-m', 'umd'])

        if self.debug:
            print(' '.join(cmd))

        run_cmd(cmd)

    def validate(self, json_path, arrow_path):
        return self._run(self.VALIDATE, arrow_path, json_path, 'VALIDATE')

    def json_to_file(self, json_path, arrow_path):
        cmd = ['node',
               '--no-warnings', self.JSON_TO_ARROW,
               '-a', arrow_path,
               '-j', json_path]
        cmd = ' '.join(cmd)
        if self.debug:
            print(cmd)
        os.system(cmd)

    def stream_to_file(self, stream_path, file_path):
        cmd = ['cat', stream_path, '|',
               'node', '--no-warnings', self.STREAM_TO_FILE, '>',
               file_path]
        cmd = ' '.join(cmd)
        if self.debug:
            print(cmd)
        os.system(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = ['cat', file_path, '|',
               'node', '--no-warnings', self.FILE_TO_STREAM, '>',
               stream_path]
        cmd = ' '.join(cmd)
        if self.debug:
            print(cmd)
        os.system(cmd)


def get_static_json_files():
    glob_pattern = os.path.join(ARROW_HOME, 'integration', 'data', '*.json')
    return glob.glob(glob_pattern)


def run_all_tests(debug=False):
    testers = [CPPTester(debug=debug),
               JavaTester(debug=debug),
               JSTester(debug=debug)]
    static_json_files = get_static_json_files()
    generated_json_files = get_generated_json_files()
    json_files = static_json_files + generated_json_files

    runner = IntegrationRunner(json_files, testers, debug=debug)
    runner.run()
    print('-- All tests passed!')


def write_js_test_json(directory):
    generate_nested_case().write(os.path.join(directory, 'nested.json'))
    generate_decimal_case().write(os.path.join(directory, 'decimal.json'))
    generate_datetime_case().write(os.path.join(directory, 'datetime.json'))
    (generate_dictionary_case()
     .write(os.path.join(directory, 'dictionary.json')))
    (generate_primitive_case([7, 10])
     .write(os.path.join(directory, 'primitive.json')))
    (generate_primitive_case([0, 0, 0])
     .write(os.path.join(directory, 'primitive-empty.json')))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Arrow integration test CLI')
    parser.add_argument('--write_generated_json', dest='generated_json_path',
                        action='store', default=False,
                        help='Generate test JSON')
    parser.add_argument('--debug', dest='debug', action='store_true',
                        default=False,
                        help='Run executables in debug mode as relevant')
    args = parser.parse_args()
    if args.generated_json_path:
        try:
            os.makedirs(args.generated_json_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        write_js_test_json(args.generated_json_path)
    else:
        run_all_tests(debug=args.debug)
