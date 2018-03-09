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
import os
import random

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
        A random decimal.Decimal object with the specifed precision and scale.
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


def get_modified_env_with_pythonpath():
    # Prepend pyarrow root directory to PYTHONPATH
    env = os.environ.copy()
    existing_pythonpath = env.get('PYTHONPATH', '')

    module_path = os.path.abspath(
        os.path.dirname(os.path.dirname(pa.__file__)))

    env['PYTHONPATH'] = os.pathsep.join((module_path, existing_pythonpath))
    return env
