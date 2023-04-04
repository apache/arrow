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

import gc
import signal
import sys
import textwrap
import weakref

import pytest

from pyarrow.util import _break_traceback_cycle_from_frame, doc
from pyarrow.tests.util import disabled_gc


@doc(method="cumsum", operation="sum")
def cumsum(whatever):
    """
    This is the {method} method.

    It computes the cumulative {operation}.
    """


@doc(
    cumsum,
    textwrap.dedent(
        """
        Examples
        --------

        >>> cumavg([1, 2, 3])
        2
        """
    ),
    method="cumavg",
    operation="average",
)
def cumavg(whatever):
    pass


@doc(
    cumsum,
    method="cumprod",
    operation="product",
)
def cumprod(whatever):
    """
    Examples
    --------

    >>> cumprod([1, 2, 3])
    2
    """
    pass


@doc(cumsum, method="cummax", operation="maximum")
def cummax(whatever):
    pass


@doc(cummax, method="cummin", operation="minimum")
def cummin(whatever):
    pass


def test_docstring_formatting():
    docstr = textwrap.dedent(
        """
        This is the cumsum method.

        It computes the cumulative sum.
        """
    )
    assert cumsum.__doc__ == docstr


def test_docstrings():
    docstr = textwrap.dedent(
        """
        This is the cumavg method.

        It computes the cumulative average.

        Examples
        --------

        >>> cumavg([1, 2, 3])
        2
        """
    )
    assert cumavg.__doc__ == docstr


def test_docstring_append():
    docstr = textwrap.dedent(
        """
        This is the cumprod method.

        It computes the cumulative product.

        Examples
        --------

        >>> cumprod([1, 2, 3])
        2
        """
    )
    assert cumprod.__doc__ == docstr


def test_doc_template_from_func():
    docstr = textwrap.dedent(
        """
        This is the cummax method.

        It computes the cumulative maximum.
        """
    )
    assert cummax.__doc__ == docstr


def test_inherit_doc_template():
    docstr = textwrap.dedent(
        """
        This is the cummin method.

        It computes the cumulative minimum.
        """
    )
    assert cummin.__doc__ == docstr


def exhibit_signal_refcycle():
    # Put an object in the frame locals and return a weakref to it.
    # If `signal.getsignal` has a bug where it creates a reference cycle
    # keeping alive the current execution frames, `obj` will not be
    # destroyed immediately when this function returns.
    obj = set()
    signal.getsignal(signal.SIGINT)
    return weakref.ref(obj)


def test_signal_refcycle():
    # Test possible workaround for https://bugs.python.org/issue42248
    with disabled_gc():
        wr = exhibit_signal_refcycle()
        if wr() is None:
            pytest.skip(
                "Python version does not have the bug we're testing for")

    gc.collect()
    with disabled_gc():
        wr = exhibit_signal_refcycle()
        assert wr() is not None
        _break_traceback_cycle_from_frame(sys._getframe(0))
        assert wr() is None
