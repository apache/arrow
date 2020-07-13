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

import contextlib
import io
import os
import random
import socket
import subprocess
import sys
import threading
import uuid

import numpy as np


def guid():
    return uuid.uuid4().hex


# SKIP categories
SKIP_ARROW = 'arrow'
SKIP_FLIGHT = 'flight'

ARROW_ROOT_DEFAULT = os.environ.get(
    'ARROW_ROOT',
    os.path.abspath(__file__).rsplit("/", 5)[0]
)


class _Printer:
    """
    A print()-providing object that can override the stream output on
    a per-thread basis.
    """

    def __init__(self):
        self._tls = threading.local()

    def _get_stdout(self):
        try:
            return self._tls.stdout
        except AttributeError:
            self._tls.stdout = sys.stdout
            self._tls.corked = False
            return self._tls.stdout

    def print(self, *args, **kwargs):
        """
        A variant of print() that writes to a thread-local stream.
        """
        print(*args, file=self._get_stdout(), **kwargs)

    @property
    def stdout(self):
        """
        A thread-local stdout wrapper that may be temporarily buffered
        using `cork()`.
        """
        return self._get_stdout()

    @contextlib.contextmanager
    def cork(self):
        """
        Temporarily buffer this thread's stream and write out its contents
        at the end of the context manager.  Useful to avoid interleaved
        output when multiple threads output progress information.
        """
        outer_stdout = self._get_stdout()
        assert not self._tls.corked, "reentrant call"
        inner_stdout = self._tls.stdout = io.StringIO()
        self._tls.corked = True
        try:
            yield
        finally:
            self._tls.stdout = outer_stdout
            self._tls.corked = False
            outer_stdout.write(inner_stdout.getvalue())
            outer_stdout.flush()


printer = _Printer()
log = printer.print


_RAND_CHARS = np.array(list("abcdefghijklmnop123456Ârrôwµ£°€矢"), dtype="U")


def random_utf8(nchars):
    """
    Generate one random UTF8 string.
    """
    return ''.join(np.random.choice(_RAND_CHARS, nchars))


def random_bytes(nbytes):
    """
    Generate one random binary string.
    """
    # NOTE getrandbits(0) fails
    if nbytes > 0:
        return random.getrandbits(nbytes * 8).to_bytes(nbytes,
                                                       byteorder='little')
    else:
        return b""


def tobytes(o):
    if isinstance(o, str):
        return o.encode('utf8')
    return o


def frombytes(o):
    if isinstance(o, bytes):
        return o.decode('utf8')
    return o


def run_cmd(cmd):
    if isinstance(cmd, str):
        cmd = cmd.split(' ')

    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        # this avoids hiding the stdout / stderr of failed processes
        sio = io.StringIO()
        print('Command failed:', " ".join(cmd), file=sio)
        print('With output:', file=sio)
        print('--------------', file=sio)
        print(frombytes(e.output), file=sio)
        print('--------------', file=sio)
        raise RuntimeError(sio.getvalue())

    return frombytes(output)


# Adapted from CPython
def find_unused_port(family=socket.AF_INET, socktype=socket.SOCK_STREAM):
    """Returns an unused port that should be suitable for binding.  This is
    achieved by creating a temporary socket with the same family and type as
    the 'sock' parameter (default is AF_INET, SOCK_STREAM), and binding it to
    the specified host address (defaults to 0.0.0.0) with the port set to 0,
    eliciting an unused ephemeral port from the OS.  The temporary socket is
    then closed and deleted, and the ephemeral port is returned.
    """
    with socket.socket(family, socktype) as tempsock:
        tempsock.bind(('', 0))
        port = tempsock.getsockname()[1]
    del tempsock
    return port
