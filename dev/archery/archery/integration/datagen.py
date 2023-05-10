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

from collections import namedtuple, OrderedDict
import binascii
import json
import os
import random
import tempfile

import numpy as np

from .util import frombytes, tobytes, random_bytes, random_utf8


def metadata_key_values(pairs):
    return [{'key': k, 'value': v} for k, v in pairs]


class Field(object):

    def __init__(self, name, *, nullable=True, metadata=None):
        self.name = name
        self.nullable = nullable
        self.metadata = metadata or []

    def get_json(self):
        entries = [
            ('name', self.name),
            ('type', self._get_type()),
            ('nullable', self.nullable),
            ('children', self._get_children()),
        ]

        dct = self._get_dictionary()
        if dct:
            entries.append(('dictionary', dct))

        if self.metadata is not None and len(self.metadata) > 0:
            entries.append(('metadata', metadata_key_values(self.metadata)))

        return OrderedDict(entries)

    def _get_dictionary(self):
        return None

    def _make_is_valid(self, size, null_probability=0.4):
        if self.nullable:
            return (np.random.random_sample(size) > null_probability
                    ).astype(np.int8)
        else:
            return np.ones(size, dtype=np.int8)


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


class PrimitiveField(Field):

    def _get_children(self):
        return []


class PrimitiveColumn(Column):

    def __init__(self, name, count, is_valid, values):
        super().__init__(name, count)
        self.is_valid = is_valid
        self.values = values

    def _encode_value(self, x):
        return x

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid]),
            ('DATA', list([self._encode_value(x) for x in self.values]))
        ]


class NullColumn(Column):
    # This subclass is for readability only
    pass


class NullField(PrimitiveField):

    def __init__(self, name, metadata=None):
        super().__init__(name, nullable=True,
                         metadata=metadata)

    def _get_type(self):
        return OrderedDict([('name', 'null')])

    def generate_column(self, size, name=None):
        return NullColumn(name or self.name, size)


TEST_INT_MAX = 2 ** 31 - 1
TEST_INT_MIN = ~TEST_INT_MAX


class IntegerField(PrimitiveField):

    def __init__(self, name, is_signed, bit_width, *, nullable=True,
                 metadata=None,
                 min_value=TEST_INT_MIN,
                 max_value=TEST_INT_MAX):
        super().__init__(name, nullable=nullable,
                         metadata=metadata)
        self.is_signed = is_signed
        self.bit_width = bit_width
        self.min_value = min_value
        self.max_value = max_value

    def _get_generated_data_bounds(self):
        if self.is_signed:
            signed_iinfo = np.iinfo('int' + str(self.bit_width))
            min_value, max_value = signed_iinfo.min, signed_iinfo.max
        else:
            unsigned_iinfo = np.iinfo('uint' + str(self.bit_width))
            min_value, max_value = 0, unsigned_iinfo.max

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
        return self.generate_range(size, lower_bound, upper_bound,
                                   name=name, include_extremes=True)

    def generate_range(self, size, lower, upper, name=None,
                       include_extremes=False):
        values = np.random.randint(lower, upper, size=size, dtype=np.int64)
        if include_extremes and size >= 2:
            values[:2] = [lower, upper]
        values = list(map(int if self.bit_width < 64 else str, values))

        is_valid = self._make_is_valid(size)

        if name is None:
            name = self.name
        return PrimitiveColumn(name, size, is_valid, values)


# Integer field that fulfils the requirements for the run ends field of REE.
# The integers are positive and in a strictly increasing sequence
class RunEndsField(IntegerField):
    # bit_width should only be one of 16/32/64
    def __init__(self, name, bit_width, *, metadata=None):
        super().__init__(name, is_signed=True, bit_width=bit_width,
                         nullable=False, metadata=metadata, min_value=1)

    def generate_range(self, size, lower, upper, name=None,
                       include_extremes=False):
        rng = np.random.default_rng()
        # generate values that are strictly increasing with a min-value of
        # 1, but don't go higher than the max signed value for the given
        # bit width. We sort the values to ensure they are strictly increasing
        # and set replace to False to avoid duplicates, ensuring a valid
        # run-ends array.
        values = rng.choice(2 ** (self.bit_width - 1) - 1, size=size, replace=False)
        values += 1
        values = sorted(values)
        values = list(map(int if self.bit_width < 64 else str, values))
        # RunEnds cannot be null, as such self.nullable == False and this
        # will generate a validity map of all ones.
        is_valid = self._make_is_valid(size)

        if name is None:
            name = self.name
        return PrimitiveColumn(name, size, is_valid, values)


class DateField(IntegerField):

    DAY = 0
    MILLISECOND = 1

    # 1/1/1 to 12/31/9999
    _ranges = {
        DAY: [-719162, 2932896],
        MILLISECOND: [-62135596800000, 253402214400000]
    }

    def __init__(self, name, unit, *, nullable=True, metadata=None):
        bit_width = 32 if unit == self.DAY else 64

        min_value, max_value = self._ranges[unit]
        super().__init__(
            name, True, bit_width,
            nullable=nullable, metadata=metadata,
            min_value=min_value, max_value=max_value
        )
        self.unit = unit

    def _get_type(self):
        return OrderedDict([
            ('name', 'date'),
            ('unit', 'DAY' if self.unit == self.DAY else 'MILLISECOND')
        ])

    def generate_range(self, size, lower, upper, name=None,
                       include_extremes=False):
        if self.unit == self.DAY:
            return super().generate_range(size, lower, upper, name)

        full_day_millis = 1000 * 60 * 60 * 24
        lower = -1 * (abs(lower) // full_day_millis)
        upper //= full_day_millis

        values = [val * full_day_millis for val in np.random.randint(
            lower, upper, size=size, dtype=np.int64)]
        lower *= full_day_millis
        upper *= full_day_millis

        if include_extremes and size >= 2:
            values[:2] = [lower, upper]
        values = list(map(int if self.bit_width < 64 else str, values))

        is_valid = self._make_is_valid(size)

        if name is None:
            name = self.name
        return PrimitiveColumn(name, size, is_valid, values)


TIMEUNIT_NAMES = {
    's': 'SECOND',
    'ms': 'MILLISECOND',
    'us': 'MICROSECOND',
    'ns': 'NANOSECOND'
}


class TimeField(IntegerField):

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

    def __init__(self, name, unit='s', *, nullable=True,
                 metadata=None):
        min_val, max_val = self._ranges[unit]
        super().__init__(name, True, self.BIT_WIDTHS[unit],
                         nullable=nullable, metadata=metadata,
                         min_value=min_val, max_value=max_val)
        self.unit = unit

    def _get_type(self):
        return OrderedDict([
            ('name', 'time'),
            ('unit', TIMEUNIT_NAMES[self.unit]),
            ('bitWidth', self.bit_width)
        ])

    def generate_column(self, size, name=None):
        lower_bound, upper_bound = self._get_generated_data_bounds()
        return self.generate_range(size, lower_bound, upper_bound,
                                   name=name)


class TimestampField(IntegerField):

    # 1/1/1 to 12/31/9999
    _ranges = {
        's': [-62135596800, 253402214400],
        'ms': [-62135596800000, 253402214400000],
        'us': [-62135596800000000, 253402214400000000],

        # Physical range for int64, ~584 years and change
        'ns': [np.iinfo('int64').min, np.iinfo('int64').max]
    }

    def __init__(self, name, unit='s', tz=None, *, nullable=True,
                 metadata=None):
        min_val, max_val = self._ranges[unit]
        super().__init__(name, True, 64,
                         nullable=nullable,
                         metadata=metadata,
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


class DurationIntervalField(IntegerField):

    def __init__(self, name, unit='s', *, nullable=True,
                 metadata=None):
        min_val, max_val = np.iinfo('int64').min, np.iinfo('int64').max,
        super().__init__(
            name, True, 64,
            nullable=nullable, metadata=metadata,
            min_value=min_val, max_value=max_val)
        self.unit = unit

    def _get_type(self):
        fields = [
            ('name', 'duration'),
            ('unit', TIMEUNIT_NAMES[self.unit])
        ]

        return OrderedDict(fields)


class YearMonthIntervalField(IntegerField):
    def __init__(self, name, *, nullable=True, metadata=None):
        min_val, max_val = [-10000*12, 10000*12]  # +/- 10000 years.
        super().__init__(
            name, True, 32,
            nullable=nullable, metadata=metadata,
            min_value=min_val, max_value=max_val)

    def _get_type(self):
        fields = [
            ('name', 'interval'),
            ('unit', 'YEAR_MONTH'),
        ]

        return OrderedDict(fields)


class DayTimeIntervalField(PrimitiveField):
    def __init__(self, name, *, nullable=True, metadata=None):
        super().__init__(name,
                         nullable=True,
                         metadata=metadata)

    @property
    def numpy_type(self):
        return object

    def _get_type(self):

        return OrderedDict([
            ('name', 'interval'),
            ('unit', 'DAY_TIME'),
        ])

    def generate_column(self, size, name=None):
        min_day_value, max_day_value = -10000*366, 10000*366
        values = [{'days': random.randint(min_day_value, max_day_value),
                   'milliseconds': random.randint(-86400000, +86400000)}
                  for _ in range(size)]

        is_valid = self._make_is_valid(size)
        if name is None:
            name = self.name
        return PrimitiveColumn(name, size, is_valid, values)


class MonthDayNanoIntervalField(PrimitiveField):
    def __init__(self, name, *, nullable=True, metadata=None):
        super().__init__(name,
                         nullable=True,
                         metadata=metadata)

    @property
    def numpy_type(self):
        return object

    def _get_type(self):

        return OrderedDict([
            ('name', 'interval'),
            ('unit', 'MONTH_DAY_NANO'),
        ])

    def generate_column(self, size, name=None):
        I32 = 'int32'
        min_int_value, max_int_value = np.iinfo(I32).min, np.iinfo(I32).max
        I64 = 'int64'
        min_nano_val, max_nano_val = np.iinfo(I64).min, np.iinfo(I64).max,
        values = [{'months': random.randint(min_int_value, max_int_value),
                   'days': random.randint(min_int_value, max_int_value),
                   'nanoseconds': random.randint(min_nano_val, max_nano_val)}
                  for _ in range(size)]

        is_valid = self._make_is_valid(size)
        if name is None:
            name = self.name
        return PrimitiveColumn(name, size, is_valid, values)


class FloatingPointField(PrimitiveField):

    def __init__(self, name, bit_width, *, nullable=True,
                 metadata=None):
        super().__init__(name,
                         nullable=nullable,
                         metadata=metadata)

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


def decimal_range_from_precision(precision):
    assert 1 <= precision <= 76
    max_value = (10 ** precision) - 1
    return -max_value, max_value


class DecimalField(PrimitiveField):
    def __init__(self, name, precision, scale, bit_width, *,
                 nullable=True, metadata=None):
        super().__init__(name, nullable=True,
                         metadata=metadata)
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
            ('bitWidth', self.bit_width),
        ])

    def generate_column(self, size, name=None):
        min_value, max_value = decimal_range_from_precision(self.precision)
        values = [random.randint(min_value, max_value) for _ in range(size)]

        is_valid = self._make_is_valid(size)
        if name is None:
            name = self.name
        return DecimalColumn(name, size, is_valid, values, self.bit_width)


class DecimalColumn(PrimitiveColumn):

    def __init__(self, name, count, is_valid, values, bit_width):
        super().__init__(name, count, is_valid, values)
        self.bit_width = bit_width

    def _encode_value(self, x):
        return str(x)


class BooleanField(PrimitiveField):
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


class FixedSizeBinaryField(PrimitiveField):

    def __init__(self, name, byte_width, *, nullable=True,
                 metadata=None):
        super().__init__(name, nullable=nullable,
                         metadata=metadata)
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

    def generate_column(self, size, name=None):
        is_valid = self._make_is_valid(size)
        values = []

        for i in range(size):
            values.append(random_bytes(self.byte_width))

        if name is None:
            name = self.name
        return self.column_class(name, size, is_valid, values)


class BinaryField(PrimitiveField):

    @property
    def numpy_type(self):
        return object

    @property
    def column_class(self):
        return BinaryColumn

    def _get_type(self):
        return OrderedDict([('name', 'binary')])

    def _random_sizes(self, size):
        return np.random.exponential(scale=4, size=size).astype(np.int32)

    def generate_column(self, size, name=None):
        is_valid = self._make_is_valid(size)
        values = []

        sizes = self._random_sizes(size)

        for i, nbytes in enumerate(sizes):
            if is_valid[i]:
                values.append(random_bytes(nbytes))
            else:
                values.append(b"")

        if name is None:
            name = self.name
        return self.column_class(name, size, is_valid, values)


class StringField(BinaryField):

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
                values.append(tobytes(random_utf8(K)))
            else:
                values.append(b"")

        if name is None:
            name = self.name
        return self.column_class(name, size, is_valid, values)


class LargeBinaryField(BinaryField):

    @property
    def column_class(self):
        return LargeBinaryColumn

    def _get_type(self):
        return OrderedDict([('name', 'largebinary')])


class LargeStringField(StringField):

    @property
    def column_class(self):
        return LargeStringColumn

    def _get_type(self):
        return OrderedDict([('name', 'largeutf8')])


class Schema(object):

    def __init__(self, fields, metadata=None):
        self.fields = fields
        self.metadata = metadata

    def get_json(self):
        entries = [
            ('fields', [field.get_json() for field in self.fields])
        ]

        if self.metadata is not None and len(self.metadata) > 0:
            entries.append(('metadata', metadata_key_values(self.metadata)))

        return OrderedDict(entries)


class _NarrowOffsetsMixin:

    def _encode_offsets(self, offsets):
        return list(map(int, offsets))


class _LargeOffsetsMixin:

    def _encode_offsets(self, offsets):
        # 64-bit offsets have to be represented as strings to roundtrip
        # through JSON.
        return list(map(str, offsets))


class _BaseBinaryColumn(PrimitiveColumn):

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
            ('OFFSET', self._encode_offsets(offsets)),
            ('DATA', data)
        ]


class _BaseStringColumn(_BaseBinaryColumn):

    def _encode_value(self, x):
        return frombytes(x)


class BinaryColumn(_BaseBinaryColumn, _NarrowOffsetsMixin):
    pass


class StringColumn(_BaseStringColumn, _NarrowOffsetsMixin):
    pass


class LargeBinaryColumn(_BaseBinaryColumn, _LargeOffsetsMixin):
    pass


class LargeStringColumn(_BaseStringColumn, _LargeOffsetsMixin):
    pass


class FixedSizeBinaryColumn(PrimitiveColumn):

    def _encode_value(self, x):
        return frombytes(binascii.hexlify(x).upper())

    def _get_buffers(self):
        data = []
        for i, v in enumerate(self.values):
            data.append(self._encode_value(v))

        return [
            ('VALIDITY', [int(x) for x in self.is_valid]),
            ('DATA', data)
        ]


class ListField(Field):

    def __init__(self, name, value_field, *, nullable=True,
                 metadata=None):
        super().__init__(name, nullable=nullable,
                         metadata=metadata)
        self.value_field = value_field

    @property
    def column_class(self):
        return ListColumn

    def _get_type(self):
        return OrderedDict([
            ('name', 'list')
        ])

    def _get_children(self):
        return [self.value_field.get_json()]

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
        values = self.value_field.generate_column(offset)

        if name is None:
            name = self.name
        return self.column_class(name, size, is_valid, offsets, values)


class LargeListField(ListField):

    @property
    def column_class(self):
        return LargeListColumn

    def _get_type(self):
        return OrderedDict([
            ('name', 'largelist')
        ])


class _BaseListColumn(Column):

    def __init__(self, name, count, is_valid, offsets, values):
        super().__init__(name, count)
        self.is_valid = is_valid
        self.offsets = offsets
        self.values = values

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid]),
            ('OFFSET', self._encode_offsets(self.offsets))
        ]

    def _get_children(self):
        return [self.values.get_json()]


class ListColumn(_BaseListColumn, _NarrowOffsetsMixin):
    pass


class LargeListColumn(_BaseListColumn, _LargeOffsetsMixin):
    pass


class MapField(Field):

    def __init__(self, name, key_field, item_field, *, nullable=True,
                 metadata=None, keys_sorted=False, entries_name='entries'):
        super().__init__(name, nullable=nullable,
                         metadata=metadata)

        assert not key_field.nullable
        self.key_field = key_field
        self.item_field = item_field
        self.pair_field = StructField(entries_name, [key_field, item_field],
                                      nullable=False)
        self.keys_sorted = keys_sorted

    def _get_type(self):
        return OrderedDict([
            ('name', 'map'),
            ('keysSorted', self.keys_sorted)
        ])

    def _get_children(self):
        return [self.pair_field.get_json()]

    def generate_column(self, size, name=None):
        MAX_MAP_SIZE = 4

        is_valid = self._make_is_valid(size)
        map_sizes = np.random.randint(0, MAX_MAP_SIZE + 1, size=size)
        offsets = [0]

        offset = 0
        for i in range(size):
            if is_valid[i]:
                offset += int(map_sizes[i])
            offsets.append(offset)

        # The offset now is the total number of elements in the child array
        pairs = self.pair_field.generate_column(offset)
        if name is None:
            name = self.name

        return MapColumn(name, size, is_valid, offsets, pairs)


class MapColumn(Column):

    def __init__(self, name, count, is_valid, offsets, pairs):
        super().__init__(name, count)
        self.is_valid = is_valid
        self.offsets = offsets
        self.pairs = pairs

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid]),
            ('OFFSET', list(self.offsets))
        ]

    def _get_children(self):
        return [self.pairs.get_json()]


class FixedSizeListField(Field):

    def __init__(self, name, value_field, list_size, *, nullable=True,
                 metadata=None):
        super().__init__(name, nullable=nullable,
                         metadata=metadata)
        self.value_field = value_field
        self.list_size = list_size

    def _get_type(self):
        return OrderedDict([
            ('name', 'fixedsizelist'),
            ('listSize', self.list_size)
        ])

    def _get_children(self):
        return [self.value_field.get_json()]

    def generate_column(self, size, name=None):
        is_valid = self._make_is_valid(size)
        values = self.value_field.generate_column(size * self.list_size)

        if name is None:
            name = self.name
        return FixedSizeListColumn(name, size, is_valid, values)


class FixedSizeListColumn(Column):

    def __init__(self, name, count, is_valid, values):
        super().__init__(name, count)
        self.is_valid = is_valid
        self.values = values

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid])
        ]

    def _get_children(self):
        return [self.values.get_json()]


class StructField(Field):

    def __init__(self, name, fields, *, nullable=True,
                 metadata=None):
        super().__init__(name, nullable=nullable,
                         metadata=metadata)
        self.fields = fields

    def _get_type(self):
        return OrderedDict([
            ('name', 'struct')
        ])

    def _get_children(self):
        return [field.get_json() for field in self.fields]

    def generate_column(self, size, name=None):
        is_valid = self._make_is_valid(size)

        field_values = [field.generate_column(size) for field in self.fields]
        if name is None:
            name = self.name
        return StructColumn(name, size, is_valid, field_values)


class RunEndEncodedField(Field):

    def __init__(self, name, run_ends_bitwidth, values_field, *, nullable=True,
                 metadata=None):
        super().__init__(name, nullable=nullable, metadata=metadata)
        self.run_ends_field = RunEndsField('run_ends', run_ends_bitwidth)
        self.values_field = values_field

    def _get_type(self):
        return OrderedDict([
            ('name', 'runendencoded')
        ])

    def _get_children(self):
        return [
            self.run_ends_field.get_json(),
            self.values_field.get_json()
        ]

    def generate_column(self, size, name=None):
        values = self.values_field.generate_column(size)
        run_ends = self.run_ends_field.generate_column(size)
        if name is None:
            name = self.name
        return RunEndEncodedColumn(name, size, run_ends, values)


class _BaseUnionField(Field):

    def __init__(self, name, fields, type_ids=None, *, nullable=True,
                 metadata=None):
        super().__init__(name, nullable=nullable, metadata=metadata)
        if type_ids is None:
            type_ids = list(range(fields))
        else:
            assert len(fields) == len(type_ids)
        self.fields = fields
        self.type_ids = type_ids
        assert all(x >= 0 for x in self.type_ids)

    def _get_type(self):
        return OrderedDict([
            ('name', 'union'),
            ('mode', self.mode),
            ('typeIds', self.type_ids),
        ])

    def _get_children(self):
        return [field.get_json() for field in self.fields]

    def _make_type_ids(self, size):
        return np.random.choice(self.type_ids, size)


class SparseUnionField(_BaseUnionField):
    mode = 'SPARSE'

    def generate_column(self, size, name=None):
        array_type_ids = self._make_type_ids(size)
        field_values = [field.generate_column(size) for field in self.fields]

        if name is None:
            name = self.name
        return SparseUnionColumn(name, size, array_type_ids, field_values)


class DenseUnionField(_BaseUnionField):
    mode = 'DENSE'

    def generate_column(self, size, name=None):
        # Reverse mapping {logical type id => physical child id}
        child_ids = [None] * (max(self.type_ids) + 1)
        for i, type_id in enumerate(self.type_ids):
            child_ids[type_id] = i

        array_type_ids = self._make_type_ids(size)
        offsets = []
        child_sizes = [0] * len(self.fields)

        for i in range(size):
            child_id = child_ids[array_type_ids[i]]
            offset = child_sizes[child_id]
            offsets.append(offset)
            child_sizes[child_id] = offset + 1

        field_values = [
            field.generate_column(child_size)
            for field, child_size in zip(self.fields, child_sizes)]

        if name is None:
            name = self.name
        return DenseUnionColumn(name, size, array_type_ids, offsets,
                                field_values)


class Dictionary(object):

    def __init__(self, id_, field, size, name=None, ordered=False):
        self.id_ = id_
        self.field = field
        self.values = field.generate_column(size=size, name=name)
        self.ordered = ordered

    def __len__(self):
        return len(self.values)

    def get_json(self):
        dummy_batch = RecordBatch(len(self.values), [self.values])
        return OrderedDict([
            ('id', self.id_),
            ('data', dummy_batch.get_json())
        ])


class DictionaryField(Field):

    def __init__(self, name, index_field, dictionary, *, nullable=True,
                 metadata=None):
        super().__init__(name, nullable=nullable,
                         metadata=metadata)
        assert index_field.name == ''
        assert isinstance(index_field, IntegerField)
        assert isinstance(dictionary, Dictionary)

        self.index_field = index_field
        self.dictionary = dictionary

    def _get_type(self):
        return self.dictionary.field._get_type()

    def _get_children(self):
        return self.dictionary.field._get_children()

    def _get_dictionary(self):
        return OrderedDict([
            ('id', self.dictionary.id_),
            ('indexType', self.index_field._get_type()),
            ('isOrdered', self.dictionary.ordered)
        ])

    def generate_column(self, size, name=None):
        if name is None:
            name = self.name
        return self.index_field.generate_range(size, 0, len(self.dictionary),
                                               name=name)


ExtensionType = namedtuple(
    'ExtensionType', ['extension_name', 'serialized', 'storage_field'])


class ExtensionField(Field):

    def __init__(self, name, extension_type, *, nullable=True, metadata=None):
        metadata = (metadata or []) + [
            ('ARROW:extension:name', extension_type.extension_name),
            ('ARROW:extension:metadata', extension_type.serialized),
        ]
        super().__init__(name, nullable=nullable, metadata=metadata)
        self.extension_type = extension_type

    def _get_type(self):
        return self.extension_type.storage_field._get_type()

    def _get_children(self):
        return self.extension_type.storage_field._get_children()

    def _get_dictionary(self):
        return self.extension_type.storage_field._get_dictionary()

    def generate_column(self, size, name=None):
        if name is None:
            name = self.name
        return self.extension_type.storage_field.generate_column(size, name)


class StructColumn(Column):

    def __init__(self, name, count, is_valid, field_values):
        super().__init__(name, count)
        self.is_valid = is_valid
        self.field_values = field_values

    def _get_buffers(self):
        return [
            ('VALIDITY', [int(v) for v in self.is_valid])
        ]

    def _get_children(self):
        return [field.get_json() for field in self.field_values]


class RunEndEncodedColumn(Column):

    def __init__(self, name, count, run_ends_field, values_field):
        super().__init__(name, count)
        self.run_ends = run_ends_field
        self.values = values_field

    def _get_buffers(self):
        return []

    def _get_children(self):
        return [self.run_ends.get_json(), self.values.get_json()]


class SparseUnionColumn(Column):

    def __init__(self, name, count, type_ids, field_values):
        super().__init__(name, count)
        self.type_ids = type_ids
        self.field_values = field_values

    def _get_buffers(self):
        return [
            ('TYPE_ID', [int(v) for v in self.type_ids])
        ]

    def _get_children(self):
        return [field.get_json() for field in self.field_values]


class DenseUnionColumn(Column):

    def __init__(self, name, count, type_ids, offsets, field_values):
        super().__init__(name, count)
        self.type_ids = type_ids
        self.offsets = offsets
        self.field_values = field_values

    def _get_buffers(self):
        return [
            ('TYPE_ID', [int(v) for v in self.type_ids]),
            ('OFFSET', [int(v) for v in self.offsets]),
        ]

    def _get_children(self):
        return [field.get_json() for field in self.field_values]


class RecordBatch(object):

    def __init__(self, count, columns):
        self.count = count
        self.columns = columns

    def get_json(self):
        return OrderedDict([
            ('count', self.count),
            ('columns', [col.get_json() for col in self.columns])
        ])


class File(object):

    def __init__(self, name, schema, batches, dictionaries=None,
                 skip=None, path=None, quirks=None):
        self.name = name
        self.schema = schema
        self.dictionaries = dictionaries or []
        self.batches = batches
        self.skip = set()
        self.path = path
        if skip:
            self.skip.update(skip)
        # For tracking flags like whether to validate decimal values
        # fit into the given precision (ARROW-13558).
        self.quirks = set()
        if quirks:
            self.quirks.update(quirks)

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
        self.path = path

    def skip_category(self, category):
        """Skip this test for the given category.

        Category should be SKIP_ARROW or SKIP_FLIGHT.
        """
        self.skip.add(category)
        return self


def get_field(name, type_, **kwargs):
    if type_ == 'binary':
        return BinaryField(name, **kwargs)
    elif type_ == 'utf8':
        return StringField(name, **kwargs)
    elif type_ == 'largebinary':
        return LargeBinaryField(name, **kwargs)
    elif type_ == 'largeutf8':
        return LargeStringField(name, **kwargs)
    elif type_.startswith('fixedsizebinary_'):
        byte_width = int(type_.split('_')[1])
        return FixedSizeBinaryField(name, byte_width=byte_width, **kwargs)

    dtype = np.dtype(type_)

    if dtype.kind in ('i', 'u'):
        signed = dtype.kind == 'i'
        bit_width = dtype.itemsize * 8
        return IntegerField(name, signed, bit_width, **kwargs)
    elif dtype.kind == 'f':
        bit_width = dtype.itemsize * 8
        return FloatingPointField(name, bit_width, **kwargs)
    elif dtype.kind == 'b':
        return BooleanField(name, **kwargs)
    else:
        raise TypeError(dtype)


def _generate_file(name, fields, batch_sizes, dictionaries=None, skip=None,
                   metadata=None):
    schema = Schema(fields, metadata=metadata)
    batches = []
    for size in batch_sizes:
        columns = []
        for field in fields:
            col = field.generate_column(size)
            columns.append(col)

        batches.append(RecordBatch(size, columns))

    return File(name, schema, batches, dictionaries, skip=skip)


def generate_custom_metadata_case():
    def meta(items):
        # Generate a simple block of metadata where each value is '{}'.
        # Keys are delimited by whitespace in `items`.
        return [(k, '{}') for k in items.split()]

    fields = [
        get_field('sort_of_pandas', 'int8', metadata=meta('pandas')),

        get_field('lots_of_meta', 'int8', metadata=meta('a b c d .. w x y z')),

        get_field(
            'unregistered_extension', 'int8',
            metadata=[
                ('ARROW:extension:name', '!nonexistent'),
                ('ARROW:extension:metadata', ''),
                ('ARROW:integration:allow_unregistered_extension', 'true'),
            ]),

        ListField('list_with_odd_values',
                  get_field('item', 'int32', metadata=meta('odd_values'))),
    ]

    batch_sizes = [1]
    return _generate_file('custom_metadata', fields, batch_sizes,
                          metadata=meta('schema_custom_0 schema_custom_1'))


def generate_duplicate_fieldnames_case():
    fields = [
        get_field('ints', 'int8'),
        get_field('ints', 'int32'),

        StructField('struct', [get_field('', 'int32'), get_field('', 'utf8')]),
    ]

    batch_sizes = [1]
    return _generate_file('duplicate_fieldnames', fields, batch_sizes)


def generate_primitive_case(batch_sizes, name='primitive'):
    types = ['bool', 'int8', 'int16', 'int32', 'int64',
             'uint8', 'uint16', 'uint32', 'uint64',
             'float32', 'float64', 'binary', 'utf8',
             'fixedsizebinary_19', 'fixedsizebinary_120']

    fields = []

    for type_ in types:
        fields.append(get_field(type_ + "_nullable", type_, nullable=True))
        fields.append(get_field(type_ + "_nonnullable", type_, nullable=False))

    return _generate_file(name, fields, batch_sizes)


def generate_primitive_large_offsets_case(batch_sizes):
    types = ['largebinary', 'largeutf8']

    fields = []

    for type_ in types:
        fields.append(get_field(type_ + "_nullable", type_, nullable=True))
        fields.append(get_field(type_ + "_nonnullable", type_, nullable=False))

    return _generate_file('primitive_large_offsets', fields, batch_sizes)


def generate_null_case(batch_sizes):
    # Interleave null with non-null types to ensure the appropriate number of
    # buffers (0) is read and written
    fields = [
        NullField(name='f0'),
        get_field('f1', 'int32'),
        NullField(name='f2'),
        get_field('f3', 'float64'),
        NullField(name='f4')
    ]
    return _generate_file('null', fields, batch_sizes)


def generate_null_trivial_case(batch_sizes):
    # Generate a case with no buffers
    fields = [
        NullField(name='f0'),
    ]
    return _generate_file('null_trivial', fields, batch_sizes)


def generate_decimal128_case():
    fields = [
        DecimalField(name='f{}'.format(i), precision=precision, scale=2,
                     bit_width=128)
        for i, precision in enumerate(range(3, 39))
    ]

    possible_batch_sizes = 7, 10
    batch_sizes = [possible_batch_sizes[i % 2] for i in range(len(fields))]
    # 'decimal' is the original name for the test, and it must match
    # provide "gold" files that test backwards compatibility, so they
    # can be appropriately skipped.
    return _generate_file('decimal', fields, batch_sizes)


def generate_decimal256_case():
    fields = [
        DecimalField(name='f{}'.format(i), precision=precision, scale=5,
                     bit_width=256)
        for i, precision in enumerate(range(37, 70))
    ]

    possible_batch_sizes = 7, 10
    batch_sizes = [possible_batch_sizes[i % 2] for i in range(len(fields))]
    return _generate_file('decimal256', fields, batch_sizes)


def generate_datetime_case():
    fields = [
        DateField('f0', DateField.DAY),
        DateField('f1', DateField.MILLISECOND),
        TimeField('f2', 's'),
        TimeField('f3', 'ms'),
        TimeField('f4', 'us'),
        TimeField('f5', 'ns'),
        TimestampField('f6', 's'),
        TimestampField('f7', 'ms'),
        TimestampField('f8', 'us'),
        TimestampField('f9', 'ns'),
        TimestampField('f10', 'ms', tz=None),
        TimestampField('f11', 's', tz='UTC'),
        TimestampField('f12', 'ms', tz='US/Eastern'),
        TimestampField('f13', 'us', tz='Europe/Paris'),
        TimestampField('f14', 'ns', tz='US/Pacific'),
    ]

    batch_sizes = [7, 10]
    return _generate_file("datetime", fields, batch_sizes)


def generate_duration_case():
    fields = [
        DurationIntervalField('f1', 's'),
        DurationIntervalField('f2', 'ms'),
        DurationIntervalField('f3', 'us'),
        DurationIntervalField('f4', 'ns'),
    ]

    batch_sizes = [7, 10]
    return _generate_file("duration", fields, batch_sizes)


def generate_interval_case():
    fields = [
        YearMonthIntervalField('f5'),
        DayTimeIntervalField('f6'),
    ]

    batch_sizes = [7, 10]
    return _generate_file("interval", fields, batch_sizes)


def generate_month_day_nano_interval_case():
    fields = [
        MonthDayNanoIntervalField('f1'),
    ]

    batch_sizes = [7, 10]
    return _generate_file("interval_mdn", fields, batch_sizes)


def generate_map_case():
    fields = [
        MapField('map_nullable', get_field('key', 'utf8', nullable=False),
                 get_field('value', 'int32')),
    ]

    batch_sizes = [7, 10]
    return _generate_file("map", fields, batch_sizes)


def generate_non_canonical_map_case():
    fields = [
        MapField('map_other_names',
                 get_field('some_key', 'utf8', nullable=False),
                 get_field('some_value', 'int32'),
                 entries_name='some_entries'),
    ]

    batch_sizes = [7]
    return _generate_file("map_non_canonical", fields, batch_sizes)


def generate_nested_case():
    fields = [
        ListField('list_nullable', get_field('item', 'int32')),
        FixedSizeListField('fixedsizelist_nullable',
                           get_field('item', 'int32'), 4),
        StructField('struct_nullable', [get_field('f1', 'int32'),
                                        get_field('f2', 'utf8')]),
        # Fails on Go (ARROW-8452)
        # ListField('list_nonnullable', get_field('item', 'int32'),
        #           nullable=False),
    ]

    batch_sizes = [7, 10]
    return _generate_file("nested", fields, batch_sizes)


def generate_recursive_nested_case():
    fields = [
        ListField('lists_list',
                  ListField('inner_list', get_field('item', 'int16'))),
        ListField('structs_list',
                  StructField('inner_struct',
                              [get_field('f1', 'int32'),
                               get_field('f2', 'utf8')])),
    ]

    batch_sizes = [7, 10]
    return _generate_file("recursive_nested", fields, batch_sizes)


def generate_run_end_encoded_case():
    fields = [
        RunEndEncodedField('ree16', 16, get_field('values', 'int32')),
        RunEndEncodedField('ree32', 32, get_field('values', 'utf8')),
        RunEndEncodedField('ree64', 64, get_field('values', 'float32')),
    ]
    batch_sizes = [0, 7, 10]
    return _generate_file("run_end_encoded", fields, batch_sizes)


def generate_nested_large_offsets_case():
    fields = [
        LargeListField('large_list_nullable', get_field('item', 'int32')),
        LargeListField('large_list_nonnullable',
                       get_field('item', 'int32'), nullable=False),
        LargeListField('large_list_nested',
                       ListField('inner_list', get_field('item', 'int16'))),
    ]

    batch_sizes = [0, 13]
    return _generate_file("nested_large_offsets", fields, batch_sizes)


def generate_unions_case():
    fields = [
        SparseUnionField('sparse', [get_field('f1', 'int32'),
                                    get_field('f2', 'utf8')],
                         type_ids=[5, 7]),
        DenseUnionField('dense', [get_field('f1', 'int16'),
                                  get_field('f2', 'binary')],
                        type_ids=[10, 20]),
        SparseUnionField('sparse', [get_field('f1', 'float32', nullable=False),
                                    get_field('f2', 'bool')],
                         type_ids=[5, 7], nullable=False),
        DenseUnionField('dense', [get_field('f1', 'uint8', nullable=False),
                                  get_field('f2', 'uint16'),
                                  NullField('f3')],
                        type_ids=[42, 43, 44], nullable=False),
    ]

    batch_sizes = [0, 11]
    return _generate_file("union", fields, batch_sizes)


def generate_dictionary_case():
    dict0 = Dictionary(0, StringField('dictionary1'), size=10, name='DICT0')
    dict1 = Dictionary(1, StringField('dictionary1'), size=5, name='DICT1')
    dict2 = Dictionary(2, get_field('dictionary2', 'int64'),
                       size=50, name='DICT2')

    fields = [
        DictionaryField('dict0', get_field('', 'int8'), dict0),
        DictionaryField('dict1', get_field('', 'int32'), dict1),
        DictionaryField('dict2', get_field('', 'int16'), dict2)
    ]
    batch_sizes = [7, 10]
    return _generate_file("dictionary", fields, batch_sizes,
                          dictionaries=[dict0, dict1, dict2])


def generate_dictionary_unsigned_case():
    dict0 = Dictionary(0, StringField('dictionary0'), size=5, name='DICT0')
    dict1 = Dictionary(1, StringField('dictionary1'), size=5, name='DICT1')
    dict2 = Dictionary(2, StringField('dictionary2'), size=5, name='DICT2')

    # TODO: JavaScript does not support uint64 dictionary indices, so disabled
    # for now

    # dict3 = Dictionary(3, StringField('dictionary3'), size=5, name='DICT3')
    fields = [
        DictionaryField('f0', get_field('', 'uint8'), dict0),
        DictionaryField('f1', get_field('', 'uint16'), dict1),
        DictionaryField('f2', get_field('', 'uint32'), dict2),
        # DictionaryField('f3', get_field('', 'uint64'), dict3)
    ]
    batch_sizes = [7, 10]
    return _generate_file("dictionary_unsigned", fields, batch_sizes,
                          dictionaries=[dict0, dict1, dict2])


def generate_nested_dictionary_case():
    dict0 = Dictionary(0, StringField('str'), size=10, name='DICT0')

    list_of_dict = ListField(
        'list',
        DictionaryField('str_dict', get_field('', 'int8'), dict0))
    dict1 = Dictionary(1, list_of_dict, size=30, name='DICT1')

    struct_of_dict = StructField('struct', [
        DictionaryField('str_dict_a', get_field('', 'int8'), dict0),
        DictionaryField('str_dict_b', get_field('', 'int8'), dict0)
    ])
    dict2 = Dictionary(2, struct_of_dict, size=30, name='DICT2')

    fields = [
        DictionaryField('list_dict', get_field('', 'int8'), dict1),
        DictionaryField('struct_dict', get_field('', 'int8'), dict2)
    ]

    batch_sizes = [10, 13]
    return _generate_file("nested_dictionary", fields, batch_sizes,
                          dictionaries=[dict0, dict1, dict2])


def generate_extension_case():
    dict0 = Dictionary(0, StringField('dictionary0'), size=5, name='DICT0')

    uuid_type = ExtensionType('uuid', 'uuid-serialized',
                              FixedSizeBinaryField('', 16))
    dict_ext_type = ExtensionType(
        'dict-extension', 'dict-extension-serialized',
        DictionaryField('str_dict', get_field('', 'int8'), dict0))

    fields = [
        ExtensionField('uuids', uuid_type),
        ExtensionField('dict_exts', dict_ext_type),
    ]

    batch_sizes = [0, 13]
    return _generate_file("extension", fields, batch_sizes,
                          dictionaries=[dict0])


def get_generated_json_files(tempdir=None):
    tempdir = tempdir or tempfile.mkdtemp(prefix='arrow-integration-')

    def _temp_path():
        return

    file_objs = [
        generate_primitive_case([], name='primitive_no_batches'),
        generate_primitive_case([17, 20], name='primitive'),
        generate_primitive_case([0, 0, 0], name='primitive_zerolength'),

        generate_primitive_large_offsets_case([17, 20])
        .skip_category('C#')
        .skip_category('JS'),

        generate_null_case([10, 0])
        .skip_category('JS'),   # TODO(ARROW-7900)

        generate_null_trivial_case([0, 0])
        .skip_category('JS'),   # TODO(ARROW-7900)

        generate_decimal128_case(),

        generate_decimal256_case()
        .skip_category('JS'),

        generate_datetime_case(),

        generate_duration_case()
        .skip_category('C#')
        .skip_category('JS'),  # TODO(ARROW-5239): Intervals + JS

        generate_interval_case()
        .skip_category('C#')
        .skip_category('JS'),  # TODO(ARROW-5239): Intervals + JS

        generate_month_day_nano_interval_case()
        .skip_category('C#')
        .skip_category('JS'),

        generate_map_case()
        .skip_category('C#'),

        generate_non_canonical_map_case()
        .skip_category('C#')
        .skip_category('Java')   # TODO(ARROW-8715)
        .skip_category('JS'),     # TODO(ARROW-8716)

        generate_nested_case()
        .skip_category('C#'),

        generate_recursive_nested_case()
        .skip_category('C#'),

        generate_nested_large_offsets_case()
        .skip_category('C#')
        .skip_category('JS'),

        generate_unions_case()
        .skip_category('C#')
        .skip_category('JS'),

        generate_custom_metadata_case()
        .skip_category('C#')
        .skip_category('JS'),

        generate_duplicate_fieldnames_case()
        .skip_category('C#')
        .skip_category('Go')
        .skip_category('JS'),

        generate_dictionary_case()
        .skip_category('C#'),

        generate_dictionary_unsigned_case()
        .skip_category('C#')
        .skip_category('Java'),  # TODO(ARROW-9377)

        generate_nested_dictionary_case()
        .skip_category('C#')
        .skip_category('Java')  # TODO(ARROW-7779)
        .skip_category('JS'),

        generate_run_end_encoded_case()
        .skip_category('C#')
        .skip_category('Java')
        .skip_category('JS')
        .skip_category('Rust'),

        generate_extension_case()
        .skip_category('C#')
        .skip_category('JS'),
    ]

    generated_paths = []
    for file_obj in file_objs:
        out_path = os.path.join(tempdir, 'generated_' +
                                file_obj.name + '.json')
        file_obj.write(out_path)
        generated_paths.append(file_obj)

    return generated_paths
