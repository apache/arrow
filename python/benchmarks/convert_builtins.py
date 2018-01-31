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

from functools import partial
import itertools

import numpy as np
import pyarrow as pa

from . import common


DEFAULT_NONE_PROB = 0.3


# TODO:
# - test dates and times
# - test decimals

class BuiltinsGenerator(object):

    def __init__(self, seed=42):
        self.rnd = np.random.RandomState(seed)

    def sprinkle_nones(self, lst, prob):
        """
        Sprinkle None entries in list *lst* with likelihood *prob*.
        """
        for i, p in enumerate(self.rnd.random_sample(size=len(lst))):
            if p < prob:
                lst[i] = None

    def generate_int_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of Python ints with *none_prob* probability of
        an entry being None.
        """
        data = list(range(n))
        self.sprinkle_nones(data, none_prob)
        return data

    def generate_float_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of Python floats with *none_prob* probability of
        an entry being None.
        """
        # Make sure we get Python floats, not np.float64
        data = list(map(float, self.rnd.uniform(0.0, 1.0, n)))
        assert len(data) == n
        self.sprinkle_nones(data, none_prob)
        return data

    def generate_bool_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of Python bools with *none_prob* probability of
        an entry being None.
        """
        # Make sure we get Python bools, not np.bool_
        data = [bool(x >= 0.5) for x in self.rnd.uniform(0.0, 1.0, n)]
        assert len(data) == n
        self.sprinkle_nones(data, none_prob)
        return data

    def _generate_varying_sequences(self, random_factory, n, min_size, max_size, none_prob):
        """
        Generate a list of *n* sequences of varying size between *min_size*
        and *max_size*, with *none_prob* probability of an entry being None.
        The base material for each sequence is obtained by calling
        `random_factory(<some size>)`
        """
        base_size = 10000
        base = random_factory(base_size + max_size)
        data = []
        for i in range(n):
            off = self.rnd.randint(base_size)
            if min_size == max_size:
                size = min_size
            else:
                size = self.rnd.randint(min_size, max_size + 1)
            data.append(base[off:off + size])
        self.sprinkle_nones(data, none_prob)
        assert len(data) == n
        return data

    def generate_fixed_binary_list(self, n, size, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of bytestrings with a fixed *size*.
        """
        return self._generate_varying_sequences(common.get_random_bytes, n,
                                                size, size, none_prob)


    def generate_varying_binary_list(self, n, min_size, max_size,
                                     none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of bytestrings with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(common.get_random_bytes, n,
                                                min_size, max_size, none_prob)


    def generate_ascii_string_list(self, n, min_size, max_size,
                                   none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of ASCII strings with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(common.get_random_ascii, n,
                                                min_size, max_size, none_prob)


    def generate_unicode_string_list(self, n, min_size, max_size,
                                     none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of unicode strings with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(common.get_random_unicode, n,
                                                min_size, max_size, none_prob)


    def generate_int_list_list(self, n, min_size, max_size,
                               none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of lists of Python ints with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(
            partial(self.generate_int_list, none_prob=none_prob),
            n, min_size, max_size, none_prob)


    def generate_dict_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of dicts with a random size between *min_size* and
        *max_size*.
        Each dict has the form `{'u': int value, 'v': float value, 'w': bool value}`
        """
        ints = self.generate_int_list(n, none_prob=none_prob)
        floats = self.generate_float_list(n, none_prob=none_prob)
        bools = self.generate_bool_list(n, none_prob=none_prob)
        dicts = []
        # Keep half the Nones, omit the other half
        keep_nones = itertools.cycle([True, False])
        for u, v, w in zip(ints, floats, bools):
            d = {}
            if u is not None or next(keep_nones):
                d['u'] = u
            if v is not None or next(keep_nones):
                d['v'] = v
            if w is not None or next(keep_nones):
                d['w'] = w
            dicts.append(d)
        self.sprinkle_nones(dicts, none_prob)
        assert len(dicts) == n
        return dicts

    def get_type_and_builtins(self, n, type_name):
        """
        Return a `(arrow type, list)` tuple where the arrow type
        corresponds to the given logical *type_name*, and the list
        is a list of *n* random-generated Python objects compatible
        with the arrow type.
        """
        size = None

        if type_name in ('bool', 'ascii', 'unicode', 'int64 list', 'struct'):
            kind = type_name
        elif type_name.startswith(('int', 'uint')):
            kind = 'int'
        elif type_name.startswith('float'):
            kind = 'float'
        elif type_name == 'binary':
            kind = 'varying binary'
        elif type_name.startswith('binary'):
            kind = 'fixed binary'
            size = int(type_name[6:])
            assert size > 0
        else:
            raise ValueError("unrecognized type %r" % (type_name,))

        if kind in ('int', 'float'):
            ty = getattr(pa, type_name)()
        elif kind == 'bool':
            ty = pa.bool_()
        elif kind == 'fixed binary':
            ty = pa.binary(size)
        elif kind == 'varying binary':
            ty = pa.binary()
        elif kind in ('ascii', 'unicode'):
            ty = pa.string()
        elif kind == 'int64 list':
            ty = pa.list_(pa.int64())
        elif kind == 'struct':
            ty = pa.struct([pa.field('u', pa.int64()),
                            pa.field('v', pa.float64()),
                            pa.field('w', pa.bool_())])

        factories = {
            'int': self.generate_int_list,
            'float': self.generate_float_list,
            'bool': self.generate_bool_list,
            'fixed binary': partial(self.generate_fixed_binary_list,
                                    size=size),
            'varying binary': partial(self.generate_varying_binary_list,
                                      min_size=3, max_size=40),
            'ascii': partial(self.generate_ascii_string_list,
                             min_size=3, max_size=40),
            'unicode': partial(self.generate_unicode_string_list,
                               min_size=3, max_size=40),
            'int64 list': partial(self.generate_int_list_list,
                                  min_size=0, max_size=20),
            'struct': self.generate_dict_list,
        }
        data = factories[kind](n)
        return ty, data


class ConvertPyListToArray(object):
    """
    Benchmark pa.array(list of values, type=...)
    """
    size = 10 ** 5
    types = ('int32', 'uint32', 'int64', 'uint64',
             'float32', 'float64', 'bool',
             'binary', 'binary10', 'ascii', 'unicode',
             'int64 list', 'struct')

    param_names = ['type']
    params = [types]

    def setup(self, type_name):
        gen = BuiltinsGenerator()
        self.ty, self.data = gen.get_type_and_builtins(self.size, type_name)

    def time_convert(self, *args):
        pa.array(self.data, type=self.ty)


class InferPyListToArray(object):
    """
    Benchmark pa.array(list of values) with type inference
    """
    size = 10 ** 5
    types = ('int64', 'float64', 'bool', 'binary', 'ascii', 'unicode',
             'int64 list')
    # TODO add 'struct' when supported

    param_names = ['type']
    params = [types]

    def setup(self, type_name):
        gen = BuiltinsGenerator()
        self.ty, self.data = gen.get_type_and_builtins(self.size, type_name)

    def time_infer(self, *args):
        arr = pa.array(self.data)
        assert arr.type == self.ty


class ConvertArrayToPyList(object):
    """
    Benchmark pa.array.to_pylist()
    """
    size = 10 ** 5
    types = ('int32', 'uint32', 'int64', 'uint64',
             'float32', 'float64', 'bool',
             'binary', 'binary10', 'ascii', 'unicode',
             'int64 list', 'struct')

    param_names = ['type']
    params = [types]

    def setup(self, type_name):
        gen = BuiltinsGenerator()
        self.ty, self.data = gen.get_type_and_builtins(self.size, type_name)
        self.arr = pa.array(self.data, type=self.ty)

    def time_convert(self, *args):
        self.arr.to_pylist()
