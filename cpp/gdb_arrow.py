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

from collections import namedtuple
from collections.abc import Sequence
import datetime
import decimal
import enum
from functools import lru_cache, partial
import itertools
import math
import operator
import struct
import sys
import warnings

import gdb
from gdb.types import get_basic_type


assert sys.version_info[0] >= 3, "Arrow GDB extension needs Python 3+"


# gdb API docs at https://sourceware.org/gdb/onlinedocs/gdb/Python-API.html#Python-API


_type_ids = [
    'NA', 'BOOL', 'UINT8', 'INT8', 'UINT16', 'INT16', 'UINT32', 'INT32',
    'UINT64', 'INT64', 'HALF_FLOAT', 'FLOAT', 'DOUBLE', 'STRING', 'BINARY',
    'FIXED_SIZE_BINARY', 'DATE32', 'DATE64', 'TIMESTAMP', 'TIME32', 'TIME64',
    'INTERVAL_MONTHS', 'INTERVAL_DAY_TIME', 'DECIMAL128', 'DECIMAL256',
    'LIST', 'STRUCT', 'SPARSE_UNION', 'DENSE_UNION', 'DICTIONARY', 'MAP',
    'EXTENSION', 'FIXED_SIZE_LIST', 'DURATION', 'LARGE_STRING',
    'LARGE_BINARY', 'LARGE_LIST', 'INTERVAL_MONTH_DAY_NANO']

# Mirror the C++ Type::type enum
Type = enum.IntEnum('Type', _type_ids, start=0)

# Mirror the C++ TimeUnit::type enum
TimeUnit = enum.IntEnum('TimeUnit', ['SECOND', 'MILLI', 'MICRO', 'NANO'],
                        start=0)

type_id_to_struct_code = {
    Type.INT8: 'b',
    Type.INT16: 'h',
    Type.INT32: 'i',
    Type.INT64: 'q',
    Type.UINT8: 'B',
    Type.UINT16: 'H',
    Type.UINT32: 'I',
    Type.UINT64: 'Q',
    Type.HALF_FLOAT: 'e',
    Type.FLOAT: 'f',
    Type.DOUBLE: 'd',
    Type.DATE32: 'i',
    Type.DATE64: 'q',
    Type.TIME32: 'i',
    Type.TIME64: 'q',
    Type.INTERVAL_DAY_TIME: 'ii',
    Type.INTERVAL_MONTHS: 'i',
    Type.INTERVAL_MONTH_DAY_NANO: 'iiq',
    Type.DURATION: 'q',
    Type.TIMESTAMP: 'q',
}

TimeUnitTraits = namedtuple('TimeUnitTraits', ('multiplier',
                                               'fractional_digits'))

time_unit_traits = {
    TimeUnit.SECOND: TimeUnitTraits(1, 0),
    TimeUnit.MILLI: TimeUnitTraits(1_000, 3),
    TimeUnit.MICRO: TimeUnitTraits(1_000_000, 6),
    TimeUnit.NANO: TimeUnitTraits(1_000_000_000, 9),
}


def identity(v):
    return v


def has_null_bitmap(type_id):
    return type_id not in (Type.NA, Type.SPARSE_UNION, Type.DENSE_UNION)


@lru_cache()
def byte_order():
    """
    Get the target program (not the GDB host's) endianness.
    """
    s = gdb.execute("show endian", to_string=True).strip()
    if 'big' in s:
        return 'big'
    elif 'little' in s:
        return 'little'
    warnings.warn('Could not determine target endianness '
                  f'from GDB\'s response:\n"""{s}"""')
    # Fall back to host endianness
    return sys.byteorder


def for_evaluation(val, ty=None):
    """
    Return a parsable form of gdb.Value `val`, optionally with gdb.Type `ty`.
    """
    if ty is None:
        ty = get_basic_type(val.type)
    typename = str(ty)  # `ty.name` is sometimes None...
    if '::' in typename and not typename.startswith('::'):
        # ARROW-15652: expressions evaluated by GDB are evaluated in the
        # scope of the C++ namespace of the currently selected frame.
        # When inside a Parquet frame, `arrow::<some type>` would be looked
        # up as `parquet::arrow::<some type>` and fail.
        # Therefore, force the lookup to happen in the global namespace scope.
        typename = f"::{typename}"
    if ty.code == gdb.TYPE_CODE_PTR:
        # It's already a pointer, can represent it directly
        return f"(({typename}) ({val}))"
    if val.address is None:
        raise ValueError(f"Cannot further evaluate rvalue: {val}")
    return f"(* ({typename}*) ({val.address}))"


def is_char_star(ty):
    # Note that "const char*" can have TYPE_CODE_INT as target type...
    ty = get_basic_type(ty)
    return (ty.code == gdb.TYPE_CODE_PTR and
            get_basic_type(ty.target()).code
                in (gdb.TYPE_CODE_CHAR, gdb.TYPE_CODE_INT))


def deref(val):
    """
    Dereference a raw or smart pointer.
    """
    ty = get_basic_type(val.type)
    if ty.code == gdb.TYPE_CODE_PTR:
        return val.dereference()
    if ty.name.startswith('std::'):
        if "shared" in ty.name:
            return SharedPtr(val).value
        if "unique" in ty.name:
            return UniquePtr(val).value
    raise TypeError(f"Cannot dereference value of type '{ty.name}'")


_string_literal_mapping = {
    ord('\\'): r'\\',
    ord('\n'): r'\n',
    ord('\r'): r'\r',
    ord('\t'): r'\t',
    ord('"'): r'\"',
}

for c in range(0, 32):
    if c not in _string_literal_mapping:
        _string_literal_mapping[c] = f"\\x{c:02x}"


def string_literal(s):
    """
    Format a Python string or gdb.Value for display as a literal.
    """
    max_len = 50
    if isinstance(s, gdb.Value):
        s = s.string()
    if len(s) > max_len:
        s = s[:max_len]
        return '"' + s.translate(_string_literal_mapping) + '" [continued]'
    else:
        return '"' + s.translate(_string_literal_mapping) + '"'


def bytes_literal(val, size=None):
    """
    Format a gdb.Value for display as a literal containing possibly
    unprintable characters.
    """
    return val.lazy_string(length=size).value()


def utf8_literal(val, size=None):
    """
    Format a gdb.Value for display as a utf-8 literal.
    """
    if size is None:
        s = val.string(encoding='utf8', errors='backslashreplace')
    elif size != 0:
        s = val.string(encoding='utf8', errors='backslashreplace', length=size)
    else:
        s = ""
    return string_literal(s)


def half_float_value(val):
    """
    Return a Python float of the given half-float (represented as a uint64_t
    gdb.Value).
    """
    buf = gdb.selected_inferior().read_memory(val.address, 2)
    return struct.unpack("e", buf)[0]


def load_atomic(val):
    """
    Load a std::atomic<T>'s value.
    """
    valty = val.type.template_argument(0)
    # XXX This assumes std::atomic<T> has the same layout as a raw T.
    return val.address.reinterpret_cast(valty.pointer()).dereference()


def load_null_count(val):
    """
    Load a null count from a gdb.Value of an integer (either atomic or not).
    """
    if get_basic_type(val.type).code != gdb.TYPE_CODE_INT:
        val = load_atomic(val)
    return val


def format_null_count(val):
    """
    Format a null count value.
    """
    if not isinstance(val, int):
        null_count = int(load_null_count(val))
    return (f"null count {null_count}" if null_count != -1
            else "unknown null count")


def short_time_unit(val):
    return ['s', 'ms', 'us', 'ns'][int(val)]


def format_month_interval(val):
    """
    Format a MonthInterval value.
    """
    return f"{int(val)}M"


def format_days_milliseconds(days, milliseconds):
    return f"{days}d{milliseconds}ms"


def format_months_days_nanos(months, days, nanos):
    return f"{months}M{days}d{nanos}ns"


_date_base = datetime.date(1970, 1, 1).toordinal()


def format_date32(val):
    """
    Format a date32 value.
    """
    val = int(val)
    try:
        decoded = datetime.date.fromordinal(val + _date_base)
    except ValueError:  # "ordinal must be >= 1"
        return f"{val}d [year <= 0]"
    else:
        return f"{val}d [{decoded}]"


def format_date64(val):
    """
    Format a date64 value.
    """
    val = int(val)
    days, remainder = divmod(val, 86400 * 1000)
    if remainder:
        return f"{val}ms [non-multiple of 86400000]"
    try:
        decoded = datetime.date.fromordinal(days + _date_base)
    except ValueError:  # "ordinal must be >= 1"
        return f"{val}ms [year <= 0]"
    else:
        return f"{val}ms [{decoded}]"


def format_timestamp(val, unit):
    """
    Format a timestamp value.
    """
    val = int(val)
    unit = int(unit)
    short_unit = short_time_unit(unit)
    traits = time_unit_traits[unit]
    seconds, subseconds = divmod(val, traits.multiplier)
    try:
        dt = datetime.datetime.utcfromtimestamp(seconds)
    except (ValueError, OSError, OverflowError):
        # value out of range for datetime.datetime
        pretty = "too large to represent"
    else:
        pretty = dt.isoformat().replace('T', ' ')
        if traits.fractional_digits > 0:
            pretty += f".{subseconds:0{traits.fractional_digits}d}"
    return f"{val}{short_unit} [{pretty}]"


def cast_to_concrete(val, ty):
    return (val.reference_value().reinterpret_cast(ty.reference())
            .referenced_value())


def scalar_class_from_type(name):
    """
    Given a DataTypeClass class name (such as "BooleanType"), return the
    corresponding Scalar class name.
    """
    assert name.endswith("Type")
    return name[:-4] + "Scalar"


def array_class_from_type(name):
    """
    Given a DataTypeClass class name (such as "BooleanType"), return the
    corresponding Array class name.
    """
    assert name.endswith("Type")
    return name[:-4] + "Array"


class CString:
    """
    A `const char*` or similar value.
    """

    def __init__(self, val):
        self.val = val

    def __bool__(self):
        return int(data) != 0 and int(data[0]) != 0

    @property
    def data(self):
        return self.val

    def bytes_literal(self):
        return self.val.lazy_string().value()

    def string_literal(self):
        # XXX use lazy_string() as well?
        return string_literal(self.val)

    def string(self):
        return self.val.string()

    def __format__(self, fmt):
        return str(self.bytes_literal())


# NOTE: gdb.parse_and_eval() is *slow* and calling it multiple times
# may add noticeable latencies.  For standard C++ classes, we therefore
# try to fetch their properties from libstdc++ internals (which hopefully
# are stable), before falling back on calling the public API methods.

class SharedPtr:
    """
    A `std::shared_ptr<T>` value.
    """

    def __init__(self, val):
        self.val = val
        try:
            # libstdc++ internals
            self._ptr = val['_M_ptr']
        except gdb.error:
            # fallback for other C++ standard libraries
            self._ptr = gdb.parse_and_eval(f"{for_evaluation(val)}.get()")

    def get(self):
        """
        Return the underlying pointer (a T*).
        """
        return self._ptr

    @property
    def value(self):
        """
        The underlying value (a T).
        """
        return self._ptr.dereference()


class UniquePtr:
    """
    A `std::unique_ptr<T>` value.
    """

    def __init__(self, val):
        self.val = val
        ty = self.val.type.template_argument(0)
        # XXX This assumes that the embedded T* pointer lies at the start
        # of std::unique_ptr<T>.
        self._ptr = self.val.address.reinterpret_cast(ty.pointer().pointer())

    def get(self):
        """
        Return the underlying pointer (a T*).
        """
        return self._ptr

    @property
    def value(self):
        """
        The underlying value (a T).
        """
        return self._ptr.dereference()


class Variant:
    """
    A `std::variant<...>`.
    """

    def __init__(self, val):
        self.val = val
        try:
            # libstdc++ internals
            self.index = val['_M_index']
        except gdb.error:
            # fallback for other C++ standard libraries
            self.index = gdb.parse_and_eval(f"{for_evaluation(val)}.index()")
        try:
            self.value_type = self.val.type.template_argument(self.index)
        except RuntimeError:
            # Index out of bounds
            self.value_type = None

    @property
    def value(self):
        if self.value_type is None:
            return None
        ptr = self.val.address
        if ptr is not None:
            return ptr.reinterpret_cast(self.value_type.pointer()
                                        ).dereference()
        return None


class StdString:
    """
    A `std::string` (or possibly `std::string_view`) value.
    """

    def __init__(self, val):
        self.val = val
        try:
            # libstdc++ internals
            self._data = val['_M_dataplus']['_M_p']
            self._size = val['_M_string_length']
        except gdb.error:
            # fallback for other C++ standard libraries
            self._data = gdb.parse_and_eval(f"{for_evaluation(val)}.c_str()")
            self._size = gdb.parse_and_eval(f"{for_evaluation(val)}.size()")

    def __bool__(self):
        return self._size != 0

    @property
    def data(self):
        return self._data

    @property
    def size(self):
        return self._size

    def bytes_literal(self):
        return self._data.lazy_string(length=self._size).value()

    def string_literal(self):
        # XXX use lazy_string() as well?
        return string_literal(self._data)

    def string(self):
        return self._data.string()

    def __format__(self, fmt):
        return str(self.bytes_literal())


class StdVector(Sequence):
    """
    A `std::vector<T>` value.
    """

    def __init__(self, val):
        self.val = val
        try:
            # libstdc++ internals
            impl = self.val['_M_impl']
            self._data = impl['_M_start']
            self._size = int(impl['_M_finish'] - self._data)
        except gdb.error:
            # fallback for other C++ standard libraries
            self._data = int(gdb.parse_and_eval(
                f"{for_evaluation(self.val)}.data()"))
            self._size = int(gdb.parse_and_eval(
                f"{for_evaluation(self.val)}.size()"))

    def _check_index(self, index):
        if index < 0 or index >= self._size:
            raise IndexError(
                f"Index {index} out of bounds (should be in [0, {self._size - 1}])")

    def __len__(self):
        return self._size

    def __getitem__(self, index):
        self._check_index(index)
        return self._data[index]

    def eval_at(self, index, eval_format):
        """
        Run `eval_format` with the value at `index`.

        For example, if `eval_format` is "{}.get()", this will evaluate
        "{self[index]}.get()".
        """
        self._check_index(index)
        return gdb.parse_and_eval(
            eval_format.format(for_evaluation(self._data[index])))

    def iter_eval(self, eval_format):
        data_eval = for_evaluation(self._data)
        for i in range(self._size):
            yield gdb.parse_and_eval(
                eval_format.format(f"{data_eval}[{i}]"))

    @property
    def size(self):
        return self._size


class StdPtrVector(StdVector):

    def __getitem__(self, index):
        return deref(super().__getitem__(index))


class FieldVector(StdVector):

    def __getitem__(self, index):
        """
        Dereference the Field object at this index.
        """
        return Field(deref(super().__getitem__(index)))

    def __str__(self):
        l = [str(self[i]) for i in range(len(self))]
        return "{" + ", ".join(l) + "}"


class Field:
    """
    A arrow::Field value.
    """

    def __init__(self, val):
        self.val = val

    @property
    def name(self):
        return StdString(self.val['name_'])

    @property
    def type(self):
        return deref(self.val['type_'])

    @property
    def nullable(self):
        return bool(self.val['nullable_'])

    def __str__(self):
        return str(self.val)


class FieldPtr(Field):
    """
    A std::shared_ptr<arrow::Field> value.
    """

    def __init__(self, val):
        super().__init__(deref(val))


class Buffer:
    """
    A arrow::Buffer value.
    """

    def __init__(self, val):
        self.val = val
        self.size = int(val['size_'])

    @property
    def data(self):
        return self.val['data_']

    def bytes_literal(self):
        if self.size > 0:
            return self.val['data_'].lazy_string(length=self.size).value()
        else:
            return '""'

    def bytes_view(self, offset=0, length=None):
        """
        Return a view over the bytes of this buffer.
        """
        if self.size > 0:
            if length is None:
                length = self.size
            mem = gdb.selected_inferior().read_memory(
                self.val['data_'] + offset, self.size)
        else:
            mem = memoryview(b"")
        # Read individual bytes as unsigned integers rather than
        # Python bytes objects
        return mem.cast('B')

    view = bytes_view


class BufferPtr:
    """
    A arrow::Buffer* value (possibly null).
    """

    def __init__(self, val):
        self.val = val
        ptr = int(self.val)
        self.buf = Buffer(val.dereference()) if ptr != 0 else None

    @property
    def data(self):
        if self.buf is None:
            return None
        return self.buf.data

    @property
    def size(self):
        if self.buf is None:
            return None
        return self.buf.size

    def bytes_literal(self):
        if self.buf is None:
            return None
        return self.buf.bytes_literal()


class TypedBuffer(Buffer):
    """
    A buffer containing values of a given a struct format code.
    """
    _boolean_format = object()

    def __init__(self, val, mem_format):
        super().__init__(val)
        self.mem_format = mem_format
        if not self.is_boolean:
            self.byte_width = struct.calcsize('=' + self.mem_format)

    @classmethod
    def from_type_id(cls, val, type_id):
        assert isinstance(type_id, int)
        if type_id == Type.BOOL:
            mem_format = cls._boolean_format
        else:
            mem_format = type_id_to_struct_code[type_id]
        return cls(val, mem_format)

    def view(self, offset=0, length=None):
        """
        Return a view over the primitive values in this buffer.

        The optional `offset` and `length` are expressed in primitive values,
        not bytes.
        """
        if self.is_boolean:
            return Bitmap.from_buffer(self, offset, length)

        byte_offset = offset * self.byte_width
        if length is not None:
            mem = self.bytes_view(byte_offset, length * self.byte_width)
        else:
            mem = self.bytes_view(byte_offset)
        return TypedView(mem, self.mem_format)

    @property
    def is_boolean(self):
        return self.mem_format is self._boolean_format


class TypedView(Sequence):
    """
    View a bytes-compatible object as a sequence of objects described
    by a struct format code.
    """

    def __init__(self, mem, mem_format):
        assert isinstance(mem, memoryview)
        self.mem = mem
        self.mem_format = mem_format
        self.byte_width = struct.calcsize('=' + mem_format)
        self.length = mem.nbytes // self.byte_width

    def _check_index(self, index):
        if not 0 <= index < self.length:
            raise IndexError("Wrong index for bitmap")

    def __len__(self):
        return self.length

    def __getitem__(self, index):
        self._check_index(index)
        w = self.byte_width
        # Cannot use memoryview.cast() because the 'e' format for half-floats
        # is poorly supported.
        mem = self.mem[index * w:(index + 1) * w]
        return struct.unpack('=' + self.mem_format, mem)


class Bitmap(Sequence):
    """
    View a bytes-compatible object as a sequence of bools.
    """
    _masks = [0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80]

    def __init__(self, view, offset, length):
        self.view = view
        self.offset = offset
        self.length = length

    def _check_index(self, index):
        if not 0 <= index < self.length:
            raise IndexError("Wrong index for bitmap")

    def __len__(self):
        return self.length

    def __getitem__(self, index):
        self._check_index(index)
        index += self.offset
        byte_index, bit_index = divmod(index, 8)
        byte = self.view[byte_index]
        return byte & self._masks[bit_index] != 0

    @classmethod
    def from_buffer(cls, buf, offset, length):
        assert isinstance(buf, Buffer)
        byte_offset, bit_offset = divmod(offset, 8)
        byte_length = math.ceil(length + offset / 8) - byte_offset
        return cls(buf.bytes_view(byte_offset, byte_length),
                   bit_offset, length)


class MappedView(Sequence):

    def __init__(self, func, view):
        self.view = view
        self.func = func

    def __len__(self):
        return len(self.view)

    def __getitem__(self, index):
        return self.func(self.view[index])


class StarMappedView(Sequence):

    def __init__(self, func, view):
        self.view = view
        self.func = func

    def __len__(self):
        return len(self.view)

    def __getitem__(self, index):
        return self.func(*self.view[index])


class NullBitmap(Bitmap):

    def __getitem__(self, index):
        self._check_index(index)
        if self.view is None:
            return True
        return super().__getitem__(index)

    @classmethod
    def from_buffer(cls, buf, offset, length):
        """
        Create a null bitmap from a Buffer (or None if missing,
        in which case all values are True).
        """
        if buf is None:
            return cls(buf, offset, length)
        return super().from_buffer(buf, offset, length)


KeyValue = namedtuple('KeyValue', ('key', 'value'))


class Metadata(Sequence):
    """
    A arrow::KeyValueMetadata value.
    """

    def __init__(self, val):
        self.val = val
        self.keys = StdVector(self.val['keys_'])
        self.values = StdVector(self.val['values_'])

    def __len__(self):
        return len(self.keys)

    def __getitem__(self, i):
        return KeyValue(StdString(self.keys[i]), StdString(self.values[i]))


class MetadataPtr(Sequence):
    """
    A shared_ptr<arrow::KeyValueMetadata> value, possibly null.
    """

    def __init__(self, val):
        self.ptr = SharedPtr(val).get()
        self.is_null = int(self.ptr) == 0
        self.md = None if self.is_null else Metadata(self.ptr.dereference())

    def __len__(self):
        return 0 if self.is_null else len(self.md)

    def __getitem__(self, i):
        if self.is_null:
            raise IndexError
        return self.md[i]


DecimalTraits = namedtuple('DecimalTraits', ('bit_width', 'struct_format_le'))

decimal_traits = {
    128: DecimalTraits(128, 'Qq'),
    256: DecimalTraits(256, 'QQQq'),
}

class BaseDecimal:
    """
    Base class for arrow::BasicDecimal{128,256...} values.
    """

    def __init__(self, address):
        self.address = address

    @classmethod
    def from_value(cls, val):
        """
        Create a decimal from a gdb.Value representing the corresponding
        arrow::BasicDecimal{128,256...}.
        """
        return cls(val['array_'].address)

    @classmethod
    def from_address(cls, address):
        """
        Create a decimal from a gdb.Value representing the address of the
        raw decimal storage.
        """
        return cls(address)

    @property
    def words(self):
        """
        The decimal words, from least to most significant.
        """
        mem = gdb.selected_inferior().read_memory(self.address,
                                                  self.traits.bit_width // 8)
        fmt = self.traits.struct_format_le
        if byte_order() == 'big':
            fmt = fmt[::-1]
        words = struct.unpack(f"={fmt}", mem)
        if byte_order() == 'big':
            words = words[::-1]
        return words

    def __int__(self):
        """
        The underlying bigint value.
        """
        v = 0
        words = self.words
        bits_per_word = self.traits.bit_width // len(words)
        for w in reversed(words):
            v = (v << bits_per_word) + w
        return v

    def format(self, precision, scale):
        """
        Format as a decimal number with the given precision and scale.
        """
        v = int(self)
        with decimal.localcontext() as ctx:
            ctx.prec = precision
            ctx.capitals = False
            return str(decimal.Decimal(v).scaleb(-scale))


class Decimal128(BaseDecimal):
    traits = decimal_traits[128]


class Decimal256(BaseDecimal):
    traits = decimal_traits[256]


decimal_bits_to_class = {
    128: Decimal128,
    256: Decimal256,
}

decimal_type_to_class = {
    f"Decimal{bits}Type": cls
    for (bits, cls) in decimal_bits_to_class.items()
}


class ExtensionType:
    """
    A arrow::ExtensionType.
    """

    def __init__(self, val):
        self.val = val

    @property
    def storage_type(self):
        return deref(self.val['storage_type_'])

    def to_string(self):
        """
        The result of calling ToString(show_metadata=True).
        """
        # XXX `show_metadata` is an optional argument, but gdb doesn't allow
        # omitting it.
        return StdString(gdb.parse_and_eval(
            f"{for_evaluation(self.val)}.ToString(true)"))


class Schema:
    """
    A arrow::Schema.
    """

    def __init__(self, val):
        self.val = val
        impl = deref(self.val['impl_'])
        self.fields = FieldVector(impl['fields_'])
        self.metadata = MetadataPtr(impl['metadata_'])


class RecordBatch:
    """
    A arrow::RecordBatch.
    """

    def __init__(self, val):
        # XXX this relies on RecordBatch always being a SimpleRecordBatch
        # under the hood. What if users create their own RecordBatch
        # implementation?
        self.val = cast_to_concrete(val,
                                    gdb.lookup_type("arrow::SimpleRecordBatch"))
        self.schema = Schema(deref(self.val['schema_']))
        self.columns = StdPtrVector(self.val['columns_'])

    @property
    def num_rows(self):
        return self.val['num_rows_']


class Table:
    """
    A arrow::Table.
    """

    def __init__(self, val):
        # XXX this relies on Table always being a SimpleTable under the hood.
        # What if users create their own Table implementation?
        self.val = cast_to_concrete(val,
                                    gdb.lookup_type("arrow::SimpleTable"))
        self.schema = Schema(deref(self.val['schema_']))
        self.columns = StdPtrVector(self.val['columns_'])

    @property
    def num_rows(self):
        return self.val['num_rows_']


type_reprs = {
    'NullType': 'null',
    'BooleanType': 'boolean',
    'UInt8Type': 'uint8',
    'Int8Type': 'int8',
    'UInt16Type': 'uint16',
    'Int16Type': 'int16',
    'UInt32Type': 'uint32',
    'Int32Type': 'int32',
    'UInt64Type': 'uint64',
    'Int64Type': 'int64',
    'HalfFloatType': 'float16',
    'FloatType': 'float32',
    'DoubleType': 'float64',
    'Date32Type': 'date32',
    'Date64Type': 'date64',
    'Time32Type': 'time32',
    'Time64Type': 'time64',
    'TimestampType': 'timestamp',
    'MonthIntervalType': 'month_interval',
    'DayTimeIntervalType': 'day_time_interval',
    'MonthDayNanoIntervalType': 'month_day_nano_interval',
    'DurationType': 'duration',
    'Decimal128Type': 'decimal128',
    'Decimal256Type': 'decimal256',
    'StringType': 'utf8',
    'LargeStringType': 'large_utf8',
    'BinaryType': 'binary',
    'LargeBinaryType': 'large_binary',
    'FixedSizeBinaryType': 'fixed_size_binary',
    'ListType': 'list',
    'LargeListType': 'large_list',
    'FixedSizeListType': 'fixed_size_list',
    'MapType': 'map',
    'StructType': 'struct_',
    'SparseUnionType': 'sparse_union',
    'DenseUnionType': 'dense_union',
    'DictionaryType': 'dictionary',
    }


class TypePrinter:
    """
    Pretty-printer for arrow::DataTypeClass and subclasses.
    """

    def __init__(self, name, val):
        self.name = name
        # Cast to concrete type class to access all derived methods
        # and properties.
        self.type = gdb.lookup_type(f"arrow::{name}")
        self.val = cast_to_concrete(val, self.type)

    @property
    def fields(self):
        return FieldVector(self.val['children_'])

    def _format_type(self):
        r = type_reprs.get(self.name, self.name)
        return f"arrow::{r}"

    def _for_evaluation(self):
        return for_evaluation(self.val, self.type)


class PrimitiveTypePrinter(TypePrinter):
    """
    Pretty-printer for non-parametric types.
    """

    def to_string(self):
        return f"{self._format_type()}()"


class TimeTypePrinter(TypePrinter):
    """
    Pretty-printer for time and duration types.
    """

    def _get_unit(self):
        return self.val['unit_']

    def to_string(self):
        return f"{self._format_type()}({self._get_unit()})"


class TimestampTypePrinter(TimeTypePrinter):
    """
    Pretty-printer for timestamp types.
    """

    def to_string(self):
        tz = StdString(self.val['timezone_'])
        if tz:
            return f'{self._format_type()}({self._get_unit()}, {tz})'
        else:
            return f'{self._format_type()}({self._get_unit()})'


class FixedSizeBinaryTypePrinter(TypePrinter):
    """
    Pretty-printer for fixed-size binary types.
    """

    def to_string(self):
        width = int(self.val['byte_width_'])
        return f"{self._format_type()}({width})"


class DecimalTypePrinter(TypePrinter):
    """
    Pretty-printer for decimal types.
    """

    def to_string(self):
        precision = int(self.val['precision_'])
        scale = int(self.val['scale_'])
        return f"{self._format_type()}({precision}, {scale})"


class ListTypePrinter(TypePrinter):
    """
    Pretty-printer for list types.
    """

    def _get_value_type(self):
        fields = self.fields
        if len(fields) != 1:
            return None
        return fields[0].type

    def to_string(self):
        child = self._get_value_type()
        if child is None:
            return f"{self._format_type()}<uninitialized or corrupt>"
        else:
            return f"{self._format_type()}({child})"


class FixedSizeListTypePrinter(ListTypePrinter):
    """
    Pretty-printer for fixed-size list type.
    """

    def to_string(self):
        child = self._get_value_type()
        if child is None:
            return f"{self._format_type()}<uninitialized or corrupt>"
        list_size = int(self.val['list_size_'])
        return f"{self._format_type()}({child}, {list_size})"


class MapTypePrinter(ListTypePrinter):
    """
    Pretty-printer for map types.
    """

    def to_string(self):
        struct_type = self._get_value_type()
        if struct_type is None:
            return f"{self._format_type()}<uninitialized or corrupt>"
        struct_children = FieldVector(struct_type['children_'])
        if len(struct_children) != 2:
            return f"{self._format_type()}<uninitialized or corrupt>"
        key_type = struct_children[0].type
        item_type = struct_children[1].type
        return (f"{self._format_type()}({key_type}, {item_type}, "
                f"keys_sorted={self.val['keys_sorted_']})")


class DictionaryTypePrinter(TypePrinter):
    """
    Pretty-printer for dictionary types.
    """

    def to_string(self):
        index_type = deref(self.val['index_type_'])
        value_type = deref(self.val['value_type_'])
        ordered = self.val['ordered_']
        return (f"{self._format_type()}({index_type}, {value_type}, "
                f"ordered={ordered})")


class StructTypePrinter(TypePrinter):
    """
    Pretty-printer for struct types.
    """

    def to_string(self):
        return f"{self._format_type()}({self.fields})"


class UnionTypePrinter(TypePrinter):
    """
    Pretty-printer for union types.
    """

    def to_string(self):
        type_codes = StdVector(self.val['type_codes_'])
        type_codes = "{" + ", ".join(str(x.cast(gdb.lookup_type('int')))
                                     for x in type_codes) + "}"
        return f"{self._format_type()}(fields={self.fields}, type_codes={type_codes})"


class ExtensionTypePrinter(TypePrinter):
    """
    Pretty-printer for extension types.
    """

    def to_string(self):
        ext_type = ExtensionType(self.val)
        return (f"{self._format_type()} {ext_type.to_string().string_literal()} "
                f"with storage type {ext_type.storage_type}")


class ScalarPrinter:
    """
    Pretty-printer for arrow::Scalar and subclasses.
    """

    def __new__(cls, val):
        # Lookup actual (derived) class to instantiate
        type_id = int(deref(val['type'])['id_'])
        type_class = lookup_type_class(type_id)
        if type_class is not None:
            cls = type_class.scalar_printer
            assert issubclass(cls, ScalarPrinter)
        self = object.__new__(cls)
        self.type_class = type_class
        self.type_name = type_class.name
        self.name = scalar_class_from_type(self.type_name)
        self.type_id = type_id
        # Cast to concrete Scalar class to access derived attributes.
        concrete_type = gdb.lookup_type(f"arrow::{self.name}")
        self.val = cast_to_concrete(val, concrete_type)
        self.is_valid = bool(self.val['is_valid'])
        return self

    @property
    def type(self):
        """
        The concrete DataTypeClass instance.
        """
        concrete_type = gdb.lookup_type(f"arrow::{self.type_name}")
        return cast_to_concrete(deref(self.val['type']),
                                concrete_type)

    def _format_type(self):
        return f"arrow::{self.name}"

    def _format_null(self):
        if self.type_class.is_parametric:
            return f"{self._format_type()} of type {self.type}, null value"
        else:
            return f"{self._format_type()} of null value"

    def _for_evaluation(self):
        return for_evaluation(self.val)


class NullScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::NullScalar.
    """

    def to_string(self):
        return self._format_type()


class NumericScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for numeric Arrow scalars.
    """

    def to_string(self):
        if not self.is_valid:
            return self._format_null()
        value = self.val['value']
        if self.type_name == "HalfFloatType":
            return (f"{self._format_type()} "
                    f"of value {half_float_value(value)} [{value}]")
        if self.type_name in ("UInt8Type", "Int8Type"):
            value = value.cast(gdb.lookup_type('int'))
        return f"{self._format_type()} of value {value}"


class TimeScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for Arrow time-like scalars.
    """

    def to_string(self):
        unit = short_time_unit(self.type['unit_'])
        if not self.is_valid:
            return f"{self._format_type()} of null value [{unit}]"
        value = self.val['value']
        return f"{self._format_type()} of value {value}{unit}"


class Date32ScalarPrinter(TimeScalarPrinter):
    """
    Pretty-printer for arrow::Date32Scalar.
    """

    def to_string(self):
        if not self.is_valid:
            return self._format_null()
        value = self.val['value']
        return f"{self._format_type()} of value {format_date32(value)}"


class Date64ScalarPrinter(TimeScalarPrinter):
    """
    Pretty-printer for arrow::Date64Scalar.
    """

    def to_string(self):
        if not self.is_valid:
            return self._format_null()
        value = self.val['value']
        return f"{self._format_type()} of value {format_date64(value)}"


class TimestampScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::TimestampScalar.
    """

    def to_string(self):
        unit = short_time_unit(self.type['unit_'])
        tz = StdString(self.type['timezone_'])
        tz = tz.string_literal() if tz.size != 0 else "no timezone"
        if not self.is_valid:
            return f"{self._format_type()} of null value [{unit}, {tz}]"
        value = self.val['value']
        return f"{self._format_type()} of value {value}{unit} [{tz}]"


class MonthIntervalScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::MonthIntervalScalarPrinter.
    """

    def to_string(self):
        if not self.is_valid:
            return self._format_null()
        value = self.val['value']
        return f"{self._format_type()} of value {format_month_interval(value)}"


class DecimalScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::DecimalScalar and subclasses.
    """

    @property
    def decimal_class(self):
        return decimal_type_to_class[self.type_name]

    def to_string(self):
        ty = self.type
        precision = int(ty['precision_'])
        scale = int(ty['scale_'])
        suffix = f"[precision={precision}, scale={scale}]"
        if not self.is_valid:
            return f"{self._format_type()} of null value {suffix}"
        value = self.decimal_class.from_value(self.val['value']
                                              ).format(precision, scale)
        return f"{self._format_type()} of value {value} {suffix}"


class BaseBinaryScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::BaseBinaryScalar and subclasses.
    """

    def _format_buf(self, bufptr):
        if 'String' in self.type_name:
            return utf8_literal(bufptr.data, bufptr.size)
        else:
            return bufptr.bytes_literal()

    def to_string(self):
        if not self.is_valid:
            return self._format_null()
        bufptr = BufferPtr(SharedPtr(self.val['value']).get())
        size = bufptr.size
        if size is None:
            return f"{self._format_type()} of value <unallocated>"
        return (f"{self._format_type()} of size {size}, "
                f"value {self._format_buf(bufptr)}")


class FixedSizeBinaryScalarPrinter(BaseBinaryScalarPrinter):
    """
    Pretty-printer for arrow::FixedSizeBinaryScalar.
    """

    def to_string(self):
        size = self.type['byte_width_']
        bufptr = BufferPtr(SharedPtr(self.val['value']).get())
        if bufptr.data is None:
            return f"{self._format_type()} of size {size}, <unallocated>"
        nullness = '' if self.is_valid else 'null with '
        return (f"{self._format_type()} of size {size}, "
                f"{nullness}value {self._format_buf(bufptr)}")


class DictionaryScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::DictionaryScalar.
    """

    def to_string(self):
        if not self.is_valid:
            return self._format_null()
        index = deref(self.val['value']['index'])
        dictionary = deref(self.val['value']['dictionary'])
        return (f"{self._format_type()} of index {index}, "
                f"dictionary {dictionary}")


class BaseListScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::BaseListScalar and subclasses.
    """

    def to_string(self):
        if not self.is_valid:
            return self._format_null()
        value = deref(self.val['value'])
        return f"{self._format_type()} of value {value}"


class StructScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::StructScalar.
    """

    def display_hint(self):
        return 'map'

    def children(self):
        if not self.is_valid:
            return None
        eval_fields = StdVector(self.type['children_'])
        eval_values = StdVector(self.val['value'])
        for field, value in zip(eval_fields, eval_values):
            name = StdString(deref(field)['name_']).string_literal()
            yield ("name", name)
            yield ("value", deref(value))

    def to_string(self):
        if not self.is_valid:
            return self._format_null()
        return f"{self._format_type()}"


class SparseUnionScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::UnionScalar and subclasses.
    """

    def to_string(self):
        type_code = self.val['type_code'].cast(gdb.lookup_type('int'))
        if not self.is_valid:
            return (f"{self._format_type()} of type {self.type}, "
                    f"type code {type_code}, null value")
        eval_values = StdVector(self.val['value'])
        child_id = self.val['child_id'].cast(gdb.lookup_type('int'))
        return (f"{self._format_type()} of type code {type_code}, "
                f"value {deref(eval_values[child_id])}")



class DenseUnionScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::UnionScalar and subclasses.
    """

    def to_string(self):
        type_code = self.val['type_code'].cast(gdb.lookup_type('int'))
        if not self.is_valid:
            return (f"{self._format_type()} of type {self.type}, "
                    f"type code {type_code}, null value")
        value = deref(self.val['value'])
        return (f"{self._format_type()} of type code {type_code}, "
                f"value {value}")


class MapScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::MapScalar.
    """

    def to_string(self):
        if not self.is_valid:
            return self._format_null()

        array = deref(self.val['value'])
        data = deref(array['data_'])
        data_printer = ArrayDataPrinter("arrow::ArrayData", data)
        return (f"{self._format_type()} of type {self.type}, "
                f"value {data_printer._format_contents()}")


class ExtensionScalarPrinter(ScalarPrinter):
    """
    Pretty-printer for arrow::ExtensionScalar.
    """

    def to_string(self):
        ext_type = ExtensionType(self.type)
        if not self.is_valid:
            return (f"{self._format_type()} of type "
                    f"{ext_type.to_string().string_literal()}, null value")
        value = deref(self.val['value'])
        return (f"{self._format_type()} of type "
                f"{ext_type.to_string().string_literal()}, value {value}")


class ArrayDataPrinter:
    """
    Pretty-printer for arrow::ArrayData.
    """

    def __new__(cls, name, val):
        # Lookup actual (derived) class to instantiate
        type_id = int(deref(val['type'])['id_'])
        type_class = lookup_type_class(type_id)
        if type_class is not None:
            cls = type_class.array_data_printer
            assert issubclass(cls, ArrayDataPrinter)
        self = object.__new__(cls)
        self.name = name
        self.val = val
        self.type_class = type_class
        self.type_name = type_class.name
        self.type_id = type_id
        self.offset = int(self.val['offset'])
        self.length = int(self.val['length'])
        return self

    @property
    def type(self):
        """
        The concrete DataTypeClass instance.
        """
        concrete_type = gdb.lookup_type(f"arrow::{self.type_name}")
        return cast_to_concrete(deref(self.val['type']), concrete_type)

    def _format_contents(self):
        return (f"length {self.length}, "
                f"offset {self.offset}, "
                f"{format_null_count(self.val['null_count'])}")

    def _buffer(self, index, type_id=None):
        buffers = StdVector(self.val['buffers'])
        bufptr = SharedPtr(buffers[index]).get()
        if int(bufptr) == 0:
            return None
        if type_id is not None:
            return TypedBuffer.from_type_id(bufptr.dereference(), type_id)
        else:
            return Buffer(bufptr.dereference())

    def _buffer_values(self, index, type_id, length=None):
        """
        Return a typed view of values in the buffer with the given index.

        Values are returned as tuples since some types may decode to
        multiple values (for example day_time_interval).
        """
        buf = self._buffer(index, type_id)
        if buf is None:
            return None
        if length is None:
            length = self.length
        return buf.view(self.offset, length)

    def _unpacked_buffer_values(self, index, type_id, length=None):
        """
        Like _buffer_values(), but assumes values are 1-tuples
        and returns them unpacked.
        """
        return StarMappedView(identity,
                              self._buffer_values(index, type_id, length))

    def _null_bitmap(self):
        buf = self._buffer(0) if has_null_bitmap(self.type_id) else None
        return NullBitmap.from_buffer(buf, self.offset, self.length)

    def _null_child(self, i):
        return str(i), "null"

    def _valid_child(self, i, value):
        return str(i), value

    def display_hint(self):
        return None

    def children(self):
        return ()

    def to_string(self):
        ty = self.type
        return (f"{self.name} of type {ty}, "
                f"{self._format_contents()}")


class NumericArrayDataPrinter(ArrayDataPrinter):
    """
    ArrayDataPrinter specialization for numeric data types.
    """
    _format_value = staticmethod(identity)

    def _values_view(self):
        return StarMappedView(self._format_value,
                              self._buffer_values(1, self.type_id))

    def display_hint(self):
        return "array"

    def children(self):
        if self.length == 0:
            return
        values = self._values_view()
        null_bits = self._null_bitmap()
        for i, (valid, value) in enumerate(zip(null_bits, values)):
            if valid:
                yield self._valid_child(i, str(value))
            else:
                yield self._null_child(i)


class BooleanArrayDataPrinter(NumericArrayDataPrinter):
    """
    ArrayDataPrinter specialization for boolean.
    """

    def _format_value(self, v):
        return str(v).lower()

    def _values_view(self):
        return MappedView(self._format_value,
                          self._buffer_values(1, self.type_id))


class Date32ArrayDataPrinter(NumericArrayDataPrinter):
    """
    ArrayDataPrinter specialization for date32.
    """
    _format_value = staticmethod(format_date32)


class Date64ArrayDataPrinter(NumericArrayDataPrinter):
    """
    ArrayDataPrinter specialization for date64.
    """
    _format_value = staticmethod(format_date64)


class TimeArrayDataPrinter(NumericArrayDataPrinter):
    """
    ArrayDataPrinter specialization for time32 and time64.
    """

    def __init__(self, name, val):
        self.unit = self.type['unit_']
        self.unit_string = short_time_unit(self.unit)

    def _format_value(self, val):
        return f"{val}{self.unit_string}"


class TimestampArrayDataPrinter(NumericArrayDataPrinter):
    """
    ArrayDataPrinter specialization for timestamp.
    """

    def __init__(self, name, val):
        self.unit = self.type['unit_']

    def _format_value(self, val):
        return format_timestamp(val, self.unit)


class MonthIntervalArrayDataPrinter(NumericArrayDataPrinter):
    """
    ArrayDataPrinter specialization for month_interval.
    """
    _format_value = staticmethod(format_month_interval)


class DayTimeIntervalArrayDataPrinter(NumericArrayDataPrinter):
    """
    ArrayDataPrinter specialization for day_time_interval.
    """
    _format_value = staticmethod(format_days_milliseconds)


class MonthDayNanoIntervalArrayDataPrinter(NumericArrayDataPrinter):
    """
    ArrayDataPrinter specialization for day_time_interval.
    """
    _format_value = staticmethod(format_months_days_nanos)


class DecimalArrayDataPrinter(ArrayDataPrinter):
    """
    ArrayDataPrinter specialization for decimals.
    """

    def __init__(self, name, val):
        ty = self.type
        self.precision = int(ty['precision_'])
        self.scale = int(ty['scale_'])
        self.decimal_class = decimal_type_to_class[self.type_name]
        self.byte_width = self.decimal_class.traits.bit_width // 8

    def display_hint(self):
        return "array"

    def children(self):
        if self.length == 0:
            return
        null_bits = self._null_bitmap()
        address = self._buffer(1).data + self.offset * self.byte_width
        for i, valid in enumerate(null_bits):
            if valid:
                dec = self.decimal_class.from_address(address)
                yield self._valid_child(
                    i, dec.format(self.precision, self.scale))
            else:
                yield self._null_child(i)
            address += self.byte_width


class FixedSizeBinaryArrayDataPrinter(ArrayDataPrinter):
    """
    ArrayDataPrinter specialization for fixed_size_binary.
    """

    def __init__(self, name, val):
        self.byte_width = self.type['byte_width_']

    def display_hint(self):
        return "array"

    def children(self):
        if self.length == 0:
            return
        null_bits = self._null_bitmap()
        address = self._buffer(1).data + self.offset * self.byte_width
        for i, valid in enumerate(null_bits):
            if valid:
                if self.byte_width:
                    yield self._valid_child(
                        i, bytes_literal(address, self.byte_width))
                else:
                    yield self._valid_child(i, '""')
            else:
                yield self._null_child(i)
            address += self.byte_width


class BinaryArrayDataPrinter(ArrayDataPrinter):
    """
    ArrayDataPrinter specialization for variable-sized binary.
    """

    def __init__(self, name, val):
        self.is_large = self.type_id in (Type.LARGE_BINARY, Type.LARGE_STRING)
        self.is_utf8 = self.type_id in (Type.STRING, Type.LARGE_STRING)
        self.format_string = utf8_literal if self.is_utf8 else bytes_literal

    def display_hint(self):
        return "array"

    def children(self):
        if self.length == 0:
            return
        null_bits = self._null_bitmap()
        offsets = self._unpacked_buffer_values(
            1, Type.INT64 if self.is_large else Type.INT32,
            length=self.length + 1)
        values = self._buffer(2).data
        for i, valid in enumerate(null_bits):
            if valid:
                start = offsets[i]
                size = offsets[i + 1] - start
                if size:
                    yield self._valid_child(
                        i, self.format_string(values + start, size))
                else:
                    yield self._valid_child(i, '""')
            else:
                yield self._null_child(i)


class ArrayPrinter:
    """
    Pretty-printer for arrow::Array and subclasses.
    """

    def __init__(self, val):
        data = deref(val['data_'])
        self.data_printer = ArrayDataPrinter("arrow::ArrayData", data)
        self.name = array_class_from_type(self.data_printer.type_name)

    def _format_contents(self):
        return self.data_printer._format_contents()

    def to_string(self):
        if self.data_printer.type_class.is_parametric:
            ty = self.data_printer.type
            return f"arrow::{self.name} of type {ty}, {self._format_contents()}"
        else:
            return f"arrow::{self.name} of {self._format_contents()}"

    def display_hint(self):
        return self.data_printer.display_hint()

    def children(self):
        return self.data_printer.children()


class ChunkedArrayPrinter:
    """
    Pretty-printer for arrow::ChunkedArray.
    """

    def __init__(self, name, val):
        self.name = name
        self.val = val
        self.chunks = StdVector(self.val['chunks_'])

    def display_hint(self):
        return "array"

    def children(self):
        for i, chunk in enumerate(self.chunks):
            printer = ArrayPrinter(deref(chunk))
            yield str(i), printer._format_contents()

    def to_string(self):
        ty = deref(self.val['type_'])
        return (f"{self.name} of type {ty}, length {self.val['length_']}, "
                f"{format_null_count(self.val['null_count_'])} "
                f"with {len(self.chunks)} chunks")


class DataTypeClass:
    array_data_printer = ArrayDataPrinter

    def __init__(self, name):
        self.name = name


class NullTypeClass(DataTypeClass):
    is_parametric = False
    type_printer = PrimitiveTypePrinter
    scalar_printer = NullScalarPrinter


class NumericTypeClass(DataTypeClass):
    is_parametric = False
    type_printer = PrimitiveTypePrinter
    scalar_printer = NumericScalarPrinter
    array_data_printer = NumericArrayDataPrinter


class BooleanTypeClass(DataTypeClass):
    is_parametric = False
    type_printer = PrimitiveTypePrinter
    scalar_printer = NumericScalarPrinter
    array_data_printer = BooleanArrayDataPrinter


class Date32TypeClass(DataTypeClass):
    is_parametric = False
    type_printer = PrimitiveTypePrinter
    scalar_printer = Date32ScalarPrinter
    array_data_printer = Date32ArrayDataPrinter


class Date64TypeClass(DataTypeClass):
    is_parametric = False
    type_printer = PrimitiveTypePrinter
    scalar_printer = Date64ScalarPrinter
    array_data_printer = Date64ArrayDataPrinter


class TimeTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = TimeTypePrinter
    scalar_printer = TimeScalarPrinter
    array_data_printer = TimeArrayDataPrinter


class TimestampTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = TimestampTypePrinter
    scalar_printer = TimestampScalarPrinter
    array_data_printer = TimestampArrayDataPrinter


class DurationTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = TimeTypePrinter
    scalar_printer = TimeScalarPrinter
    array_data_printer = TimeArrayDataPrinter


class MonthIntervalTypeClass(DataTypeClass):
    is_parametric = False
    type_printer = PrimitiveTypePrinter
    scalar_printer = MonthIntervalScalarPrinter
    array_data_printer = MonthIntervalArrayDataPrinter


class DayTimeIntervalTypeClass(DataTypeClass):
    is_parametric = False
    type_printer = PrimitiveTypePrinter
    scalar_printer = NumericScalarPrinter
    array_data_printer = DayTimeIntervalArrayDataPrinter


class MonthDayNanoIntervalTypeClass(DataTypeClass):
    is_parametric = False
    type_printer = PrimitiveTypePrinter
    scalar_printer = NumericScalarPrinter
    array_data_printer = MonthDayNanoIntervalArrayDataPrinter


class DecimalTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = DecimalTypePrinter
    scalar_printer = DecimalScalarPrinter
    array_data_printer = DecimalArrayDataPrinter


class BaseBinaryTypeClass(DataTypeClass):
    is_parametric = False
    type_printer = PrimitiveTypePrinter
    scalar_printer = BaseBinaryScalarPrinter
    array_data_printer = BinaryArrayDataPrinter


class FixedSizeBinaryTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = FixedSizeBinaryTypePrinter
    scalar_printer = FixedSizeBinaryScalarPrinter
    array_data_printer = FixedSizeBinaryArrayDataPrinter


class BaseListTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = ListTypePrinter
    scalar_printer = BaseListScalarPrinter


class FixedSizeListTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = FixedSizeListTypePrinter
    scalar_printer = BaseListScalarPrinter


class MapTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = MapTypePrinter
    scalar_printer = MapScalarPrinter


class StructTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = StructTypePrinter
    scalar_printer = StructScalarPrinter


class DenseUnionTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = UnionTypePrinter
    scalar_printer = DenseUnionScalarPrinter


class SparseUnionTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = UnionTypePrinter
    scalar_printer = SparseUnionScalarPrinter


class DictionaryTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = DictionaryTypePrinter
    scalar_printer = DictionaryScalarPrinter


class ExtensionTypeClass(DataTypeClass):
    is_parametric = True
    type_printer = ExtensionTypePrinter
    scalar_printer = ExtensionScalarPrinter


DataTypeTraits = namedtuple('DataTypeTraits', ('factory', 'name'))


type_traits_by_id = {
    Type.NA: DataTypeTraits(NullTypeClass, 'NullType'),

    Type.BOOL: DataTypeTraits(BooleanTypeClass, 'BooleanType'),

    Type.UINT8: DataTypeTraits(NumericTypeClass, 'UInt8Type'),
    Type.INT8: DataTypeTraits(NumericTypeClass, 'Int8Type'),
    Type.UINT16: DataTypeTraits(NumericTypeClass, 'UInt16Type'),
    Type.INT16: DataTypeTraits(NumericTypeClass, 'Int16Type'),
    Type.UINT32: DataTypeTraits(NumericTypeClass, 'UInt32Type'),
    Type.INT32: DataTypeTraits(NumericTypeClass, 'Int32Type'),
    Type.UINT64: DataTypeTraits(NumericTypeClass, 'UInt64Type'),
    Type.INT64: DataTypeTraits(NumericTypeClass, 'Int64Type'),
    Type.HALF_FLOAT: DataTypeTraits(NumericTypeClass, 'HalfFloatType'),
    Type.FLOAT: DataTypeTraits(NumericTypeClass, 'FloatType'),
    Type.DOUBLE: DataTypeTraits(NumericTypeClass, 'DoubleType'),

    Type.STRING: DataTypeTraits(BaseBinaryTypeClass, 'StringType'),
    Type.BINARY: DataTypeTraits(BaseBinaryTypeClass, 'BinaryType'),
    Type.LARGE_STRING: DataTypeTraits(BaseBinaryTypeClass, 'LargeStringType'),
    Type.LARGE_BINARY: DataTypeTraits(BaseBinaryTypeClass, 'LargeBinaryType'),

    Type.FIXED_SIZE_BINARY: DataTypeTraits(FixedSizeBinaryTypeClass,
                                           'FixedSizeBinaryType'),

    Type.DATE32: DataTypeTraits(Date32TypeClass, 'Date32Type'),
    Type.DATE64: DataTypeTraits(Date64TypeClass, 'Date64Type'),
    Type.TIMESTAMP: DataTypeTraits(TimestampTypeClass, 'TimestampType'),
    Type.TIME32: DataTypeTraits(TimeTypeClass, 'Time32Type'),
    Type.TIME64: DataTypeTraits(TimeTypeClass, 'Time64Type'),
    Type.DURATION: DataTypeTraits(DurationTypeClass, 'DurationType'),
    Type.INTERVAL_MONTHS: DataTypeTraits(MonthIntervalTypeClass,
                                         'MonthIntervalType'),
    Type.INTERVAL_DAY_TIME: DataTypeTraits(DayTimeIntervalTypeClass,
                                           'DayTimeIntervalType'),
    Type.INTERVAL_MONTH_DAY_NANO: DataTypeTraits(MonthDayNanoIntervalTypeClass,
                                                 'MonthDayNanoIntervalType'),

    Type.DECIMAL128: DataTypeTraits(DecimalTypeClass, 'Decimal128Type'),
    Type.DECIMAL256: DataTypeTraits(DecimalTypeClass, 'Decimal256Type'),

    Type.LIST: DataTypeTraits(BaseListTypeClass, 'ListType'),
    Type.LARGE_LIST: DataTypeTraits(BaseListTypeClass, 'LargeListType'),
    Type.FIXED_SIZE_LIST: DataTypeTraits(FixedSizeListTypeClass,
                                         'FixedSizeListType'),
    Type.MAP: DataTypeTraits(MapTypeClass, 'MapType'),

    Type.STRUCT: DataTypeTraits(StructTypeClass, 'StructType'),
    Type.SPARSE_UNION: DataTypeTraits(SparseUnionTypeClass, 'SparseUnionType'),
    Type.DENSE_UNION: DataTypeTraits(DenseUnionTypeClass, 'DenseUnionType'),

    Type.DICTIONARY: DataTypeTraits(DictionaryTypeClass, 'DictionaryType'),
    Type.EXTENSION: DataTypeTraits(ExtensionTypeClass, 'ExtensionType'),
}

max_type_id = len(type_traits_by_id) - 1


def lookup_type_class(type_id):
    """
    Lookup a type class (an instance of DataTypeClass) by its type id.
    """
    traits = type_traits_by_id.get(type_id)
    if traits is not None:
        return traits.factory(traits.name)
    return None


class StatusPrinter:
    """
    Pretty-printer for arrow::Status.
    """
    _status_codes_by_id = {
        0: 'OK',
        1: 'OutOfMemory',
        2: 'KeyError',
        3: 'TypeError',
        4: 'Invalid',
        5: 'IOError',
        6: 'CapacityError',
        7: 'IndexError',
        8: 'Cancelled',
        9: 'UnknownError',
        10: 'NotImplemented',
        11: 'SerializationError',
        13: 'RError',
        40: 'CodeGenError',
        41: 'ExpressionValidationError',
        42: 'ExecutionError',
        45: 'AlreadyExists',
    }

    def __init__(self, name, val):
        self.val = val

    def _format_detail(self, state):
        detail_ptr = SharedPtr(state['detail']).get()
        if int(detail_ptr) == 0:
            return None
        detail_id = CString(gdb.parse_and_eval(
            f"{for_evaluation(detail_ptr)}->type_id()"))
        # Cannot use StdString as ToString() returns a rvalue
        detail_msg = CString(gdb.parse_and_eval(
            f"{for_evaluation(detail_ptr)}->ToString().c_str()"))
        return f"[{detail_id.string()}] {detail_msg.string_literal()}"

    def _format_error(self, state):
        code = int(state['code'])
        codename = self._status_codes_by_id.get(code)
        if codename is not None:
            s = f"arrow::Status::{codename}("
        else:
            s = f"arrow::Status(<unknown code {code}>, "
        s += StdString(state['msg']).string_literal()
        detail_msg = self._format_detail(state)
        if detail_msg is not None:
            return s + f", detail={detail_msg})"
        else:
            return s + ")"

    def to_string(self):
        state_ptr = self.val['state_']
        if int(state_ptr) == 0:
            return "arrow::Status::OK()"
        return self._format_error(state_ptr.dereference())


class ResultPrinter(StatusPrinter):
    """
    Pretty-printer for arrow::Result<T>.
    """

    def to_string(self):
        data_type = self.val.type.template_argument(0)
        state_ptr = self.val['status_']['state_']
        if int(state_ptr) != 0:
            inner = self._format_error(state_ptr)
        else:
            data_ptr = self.val['storage_']['data_'].address
            assert data_ptr
            inner = data_ptr.reinterpret_cast(
                data_type.pointer()).dereference()
        return f"arrow::Result<{data_type}>({inner})"


class FieldPrinter:
    """
    Pretty-printer for arrow::Field.
    """

    def __init__(self, name, val):
        self.val = val

    def to_string(self):
        f = Field(self.val)
        nullable = f.nullable
        if nullable:
            return f'arrow::field({f.name}, {f.type})'
        else:
            return f'arrow::field({f.name}, {f.type}, nullable=false)'


class MetadataPrinter:
    """
    Pretty-printer for arrow::KeyValueMetadata.
    """

    def __init__(self, name, val):
        self.val = val
        self.metadata = Metadata(self.val)

    def display_hint(self):
        return 'map'

    def children(self):
        for k, v in self.metadata:
            yield ("key", k.bytes_literal())
            yield ("value", v.bytes_literal())

    def to_string(self):
        return f"arrow::KeyValueMetadata of size {len(self.metadata)}"


class SchemaPrinter:
    """
    Pretty-printer for arrow::Schema.
    """

    def __init__(self, name, val):
        self.val = val
        self.schema = Schema(val)
        # TODO endianness

    def display_hint(self):
        return 'map'

    def children(self):
        for field in self.schema.fields:
            yield ("name", field.name.string_literal())
            yield ("type", field.type)

    def to_string(self):
        num_fields = len(self.schema.fields)
        md_items = len(self.schema.metadata)
        if md_items > 0:
            return (f"arrow::Schema with {num_fields} fields "
                    f"and {md_items} metadata items")
        else:
            return f"arrow::Schema with {num_fields} fields"


class BaseColumnarPrinter:

    def __init__(self, name, val, columnar):
        self.name = name
        self.val = val
        self.columnar = columnar
        self.schema = self.columnar.schema

    def display_hint(self):
        return 'map'

    def children(self):
        for field, col in zip(self.schema.fields,
                              self.columnar.columns):
            yield ("name", field.name.string_literal())
            yield ("value", col)

    def to_string(self):
        num_fields = len(self.schema.fields)
        num_rows = self.columnar.num_rows
        md_items = len(self.schema.metadata)
        if md_items > 0:
            return (f"arrow::{self.name} with {num_fields} columns, "
                    f"{num_rows} rows, {md_items} metadata items")
        else:
            return (f"arrow::{self.name} with {num_fields} columns, "
                    f"{num_rows} rows")


class RecordBatchPrinter(BaseColumnarPrinter):
    """
    Pretty-printer for arrow::RecordBatch.
    """

    def __init__(self, name, val):
        BaseColumnarPrinter.__init__(self, "RecordBatch", val, RecordBatch(val))


class TablePrinter(BaseColumnarPrinter):
    """
    Pretty-printer for arrow::Table.
    """

    def __init__(self, name, val):
        BaseColumnarPrinter.__init__(self, "Table", val, Table(val))


class DatumPrinter:
    """
    Pretty-printer for arrow::Datum.
    """

    def __init__(self, name, val):
        self.val = val
        self.variant = Variant(val['value'])

    def to_string(self):
        if self.variant.index == 0:
            # Datum::NONE
            return "arrow::Datum (empty)"
        if self.variant.value_type is None:
            return "arrow::Datum (uninitialized or corrupt?)"
        # All non-empty Datums contain a shared_ptr<T>
        value = deref(self.variant.value)
        return f"arrow::Datum of value {value}"


class BufferPrinter:
    """
    Pretty-printer for arrow::Buffer and subclasses.
    """

    def __init__(self, name, val):
        self.name = name
        self.val = val

    def to_string(self):
        if bool(self.val['is_mutable_']):
            mutable = 'mutable'
        else:
            mutable = 'read-only'
        size = int(self.val['size_'])
        if size == 0:
            return f"arrow::{self.name} of size 0, {mutable}"
        if not self.val['is_cpu_']:
            return f"arrow::{self.name} of size {size}, {mutable}, not on CPU"
        data = bytes_literal(self.val['data_'], size)
        return f"arrow::{self.name} of size {size}, {mutable}, {data}"


class DayMillisecondsPrinter:
    """
    Pretty-printer for arrow::DayTimeIntervalType::DayMilliseconds.
    """

    def __init__(self, name, val):
        self.val = val

    def to_string(self):
        return format_days_milliseconds(self.val['days'],
                                        self.val['milliseconds'])


class MonthDayNanosPrinter:
    """
    Pretty-printer for arrow::MonthDayNanoIntervalType::MonthDayNanos.
    """

    def __init__(self, name, val):
        self.val = val

    def to_string(self):
        return format_months_days_nanos(self.val['months'],
                                        self.val['days'],
                                        self.val['nanoseconds'])


class DecimalPrinter:
    """
    Pretty-printer for Arrow decimal values.
    """

    def __init__(self, bit_width, name, val):
        self.name = name
        self.val = val
        self.bit_width = bit_width

    def to_string(self):
        dec = decimal_bits_to_class[self.bit_width].from_value(self.val)
        return f"{self.name}({int(dec)})"


printers = {
    "arrow::ArrayData": ArrayDataPrinter,
    "arrow::BasicDecimal128": partial(DecimalPrinter, 128),
    "arrow::BasicDecimal256": partial(DecimalPrinter, 256),
    "arrow::ChunkedArray": ChunkedArrayPrinter,
    "arrow::Datum": DatumPrinter,
    "arrow::DayTimeIntervalType::DayMilliseconds": DayMillisecondsPrinter,
    "arrow::Decimal128": partial(DecimalPrinter, 128),
    "arrow::Decimal256": partial(DecimalPrinter, 256),
    "arrow::MonthDayNanoIntervalType::MonthDayNanos": MonthDayNanosPrinter,
    "arrow::Field": FieldPrinter,
    "arrow::KeyValueMetadata": MetadataPrinter,
    "arrow::RecordBatch": RecordBatchPrinter,
    "arrow::Result": ResultPrinter,
    "arrow::Schema": SchemaPrinter,
    "arrow::SimpleRecordBatch": RecordBatchPrinter,
    "arrow::SimpleTable": TablePrinter,
    "arrow::Status": StatusPrinter,
    "arrow::Table": TablePrinter,
}


def arrow_pretty_print(val):
    name = val.type.strip_typedefs().name
    if name is None:
        return
    name = name.partition('<')[0]  # Remove template parameters
    printer = printers.get(name)
    if printer is not None:
        return printer(name, val)

    if not name.startswith("arrow::"):
        return
    arrow_name = name[len("arrow::"):]

    if arrow_name.endswith("Buffer"):
        try:
            val['data_']
        except Exception:
            # Not a Buffer?
            pass
        else:
            return BufferPrinter(arrow_name, val)

    elif arrow_name.endswith("Type"):
        # Look up dynamic type, as it may be hidden behind a DataTypeClass
        # pointer or reference.
        try:
            type_id = int(val['id_'])
        except Exception:
            # Not a DataTypeClass?
            pass
        else:
            type_class = lookup_type_class(type_id)
            if type_class is not None:
                return type_class.type_printer(type_class.name, val)

    elif arrow_name.endswith("Array"):
        return ArrayPrinter(val)

    elif arrow_name.endswith("Scalar"):
        try:
            val['is_valid']
        except Exception:
            # Not a Scalar?
            pass
        else:
            return ScalarPrinter(val)


def main():
    # This pattern allows for two modes of use:
    # - manual loading using `source gdb-arrow.py`: current_objfile()
    #   will be None;
    # - automatic loading from the GDB `scripts-directory`: current_objfile()
    #   will be tied to the inferior being debugged.
    objfile = gdb.current_objfile()
    if objfile is None:
        objfile = gdb

    objfile.pretty_printers.append(arrow_pretty_print)


if __name__ == '__main__':
    main()
