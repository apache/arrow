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

import os
import six
import string
import subprocess
import uuid

import numpy as np


def guid():
    return uuid.uuid4().hex


RANDS_CHARS = np.array(list(string.ascii_letters + string.digits),
                       dtype=(np.str_, 1))

# SKIP categories
SKIP_ARROW = 'arrow'
SKIP_FLIGHT = 'flight'

ARROW_ROOT_DEFAULT = os.environ.get(
    'ARROW_ROOT',
    os.path.abspath(__file__).rsplit("/", 4)[0]
)


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
