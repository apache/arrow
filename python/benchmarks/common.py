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

import codecs
import os
import sys
import unicodedata

import numpy as np


def _multiplicate_sequence(base, target_size):
    q, r = divmod(target_size, len(base))
    return [base] * q + [base[:r]]


def get_random_bytes(n):
    rnd = np.random.RandomState(42)
    # Computing a huge random bytestring can be costly, so we get at most
    # 100KB and duplicate the result as needed
    base_size = 100003
    q, r = divmod(n, base_size)
    if q == 0:
        result = rnd.bytes(r)
    else:
        base = rnd.bytes(base_size)
        result = b''.join(_multiplicate_sequence(base, n))
    assert len(result) == n
    return result


def get_random_ascii(n):
    arr = np.frombuffer(get_random_bytes(n), dtype=np.int8) & 0x7f
    result, _ = codecs.ascii_decode(arr)
    assert isinstance(result, str)
    assert len(result) == n
    return result


def _random_unicode_letters(n):
    """
    Generate a string of random unicode letters (slow).
    """
    def _get_more_candidates():
        return rnd.randint(0, sys.maxunicode, size=n).tolist()

    rnd = np.random.RandomState(42)
    out = []
    candidates = []

    while len(out) < n:
        if not candidates:
            candidates = _get_more_candidates()
        ch = chr(candidates.pop())
        # XXX Do we actually care that the code points are valid?
        if unicodedata.category(ch)[0] == 'L':
            out.append(ch)
    return out


_1024_random_unicode_letters = _random_unicode_letters(1024)


def get_random_unicode(n):
    indices = np.frombuffer(get_random_bytes(n * 2), dtype=np.int16) & 1023
    unicode_arr = np.array(_1024_random_unicode_letters)[indices]

    result = ''.join(unicode_arr.tolist())
    assert len(result) == n, (len(result), len(unicode_arr))
    return result
