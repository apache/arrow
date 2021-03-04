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

"""
Utility functions for testing
"""

import contextlib
import decimal
import gc
import numpy as np
import os
import random
import string
import subprocess
import sys

import pyarrow as pa


def randsign():
    """Randomly choose either 1 or -1.

    Returns
    -------
    sign : int
    """
    return random.choice((-1, 1))


@contextlib.contextmanager
def random_seed(seed):
    """Set the random seed inside of a context manager.

    Parameters
    ----------
    seed : int
        The seed to set

    Notes
    -----
    This function is useful when you want to set a random seed but not affect
    the random state of other functions using the random module.
    """
    original_state = random.getstate()
    random.seed(seed)
    try:
        yield
    finally:
        random.setstate(original_state)


def randdecimal(precision, scale):
    """Generate a random decimal value with specified precision and scale.

    Parameters
    ----------
    precision : int
        The maximum number of digits to generate. Must be an integer between 1
        and 38 inclusive.
    scale : int
        The maximum number of digits following the decimal point.  Must be an
        integer greater than or equal to 0.

    Returns
    -------
    decimal_value : decimal.Decimal
        A random decimal.Decimal object with the specified precision and scale.
    """
    assert 1 <= precision <= 38, 'precision must be between 1 and 38 inclusive'
    if scale < 0:
        raise ValueError(
            'randdecimal does not yet support generating decimals with '
            'negative scale'
        )
    max_whole_value = 10 ** (precision - scale) - 1
    whole = random.randint(-max_whole_value, max_whole_value)

    if not scale:
        return decimal.Decimal(whole)

    max_fractional_value = 10 ** scale - 1
    fractional = random.randint(0, max_fractional_value)

    return decimal.Decimal(
        '{}.{}'.format(whole, str(fractional).rjust(scale, '0'))
    )


def random_ascii(length):
    return bytes(np.random.randint(65, 123, size=length, dtype='i1'))


def rands(nchars):
    """
    Generate one random string.
    """
    RANDS_CHARS = np.array(
        list(string.ascii_letters + string.digits), dtype=(np.str_, 1))
    return "".join(np.random.choice(RANDS_CHARS, nchars))


def make_dataframe():
    import pandas as pd

    N = 30
    df = pd.DataFrame(
        {col: np.random.randn(N) for col in string.ascii_uppercase[:4]},
        index=pd.Index([rands(10) for _ in range(N)])
    )
    return df


def memory_leak_check(f, metric='rss', threshold=1 << 17, iterations=10,
                      check_interval=1):
    """
    Execute the function and try to detect a clear memory leak either internal
    to Arrow or caused by a reference counting problem in the Python binding
    implementation. Raises exception if a leak detected

    Parameters
    ----------
    f : callable
        Function to invoke on each iteration
    metric : {'rss', 'vms', 'shared'}, default 'rss'
        Attribute of psutil.Process.memory_info to use for determining current
        memory use
    threshold : int, default 128K
        Threshold in number of bytes to consider a leak
    iterations : int, default 10
        Total number of invocations of f
    check_interval : int, default 1
        Number of invocations of f in between each memory use check
    """
    import psutil
    proc = psutil.Process()

    def _get_use():
        gc.collect()
        return getattr(proc.memory_info(), metric)

    baseline_use = _get_use()

    def _leak_check():
        current_use = _get_use()
        if current_use - baseline_use > threshold:
            raise Exception("Memory leak detected. "
                            "Departure from baseline {} after {} iterations"
                            .format(current_use - baseline_use, i))

    for i in range(iterations):
        f()
        if i % check_interval == 0:
            _leak_check()


def get_modified_env_with_pythonpath():
    # Prepend pyarrow root directory to PYTHONPATH
    env = os.environ.copy()
    existing_pythonpath = env.get('PYTHONPATH', '')

    module_path = os.path.abspath(
        os.path.dirname(os.path.dirname(pa.__file__)))

    if existing_pythonpath:
        new_pythonpath = os.pathsep.join((module_path, existing_pythonpath))
    else:
        new_pythonpath = module_path
    env['PYTHONPATH'] = new_pythonpath
    return env


def invoke_script(script_name, *args):
    subprocess_env = get_modified_env_with_pythonpath()

    dir_path = os.path.dirname(os.path.realpath(__file__))
    python_file = os.path.join(dir_path, script_name)

    cmd = [sys.executable, python_file]
    cmd.extend(args)

    subprocess.check_call(cmd, env=subprocess_env)


@contextlib.contextmanager
def changed_environ(name, value):
    """
    Temporarily set environment variable *name* to *value*.
    """
    orig_value = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if orig_value is None:
            del os.environ[name]
        else:
            os.environ[name] = orig_value


@contextlib.contextmanager
def change_cwd(path):
    curdir = os.getcwd()
    os.chdir(str(path))
    try:
        yield
    finally:
        os.chdir(curdir)


def _filesystem_uri(path):
    # URIs on Windows must follow 'file:///C:...' or 'file:/C:...' patterns.
    if os.name == 'nt':
        uri = 'file:///{}'.format(path)
    else:
        uri = 'file://{}'.format(path)
    return uri
