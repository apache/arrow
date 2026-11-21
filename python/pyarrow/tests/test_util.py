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
import shutil
import sys
import textwrap

import pytest

from pyarrow.util import doc, download_tzdata_on_windows


@doc(method="func_a", operation="A")
def func_a(whatever):
    """
    This is the {method} method.

    It computes {operation}.
    """
    pass


@doc(
    func_a,
    textwrap.dedent(
        """
        Examples
        --------

        >>> func_b()
        B
        """
    ),
    method="func_b",
    operation="B",
)
def func_b(whatever):
    pass


@doc(
    func_a,
    method="func_c",
    operation="C",
)
def func_c(whatever):
    """
    Examples
    --------

    >>> func_c()
    C
    """
    pass


@doc(func_a, method="func_d", operation="D")
def func_d(whatever):
    pass


@doc(func_d, method="func_e", operation="E")
def func_e(whatever):
    pass


@doc(method="func_f")
def func_f(whatever):
    """
    This is the {method} method.

    {{ We can escape curly braces like this. }}

    Examples
    --------
    We should replace curly brace usage in doctests.

    >>> dict(x = "x", y = "y")
    >>> set((1, 2, 3))
    """
    pass


def test_docstring_formatting():
    docstr = textwrap.dedent(
        """
        This is the func_a method.

        It computes A.
        """
    )
    assert func_a.__doc__ == docstr


def test_docstring_concatenation():
    docstr = textwrap.dedent(
        """
        This is the func_b method.

        It computes B.

        Examples
        --------

        >>> func_b()
        B
        """
    )
    assert func_b.__doc__ == docstr


def test_docstring_append():
    docstr = textwrap.dedent(
        """
        This is the func_c method.

        It computes C.

        Examples
        --------

        >>> func_c()
        C
        """
    )
    assert func_c.__doc__ == docstr


def test_docstring_template_from_callable():
    docstr = textwrap.dedent(
        """
        This is the func_d method.

        It computes D.
        """
    )
    assert func_d.__doc__ == docstr


def test_inherit_docstring_template_from_callable():
    docstr = textwrap.dedent(
        """
        This is the func_e method.

        It computes E.
        """
    )
    assert func_e.__doc__ == docstr


def test_escaping_in_docstring():
    docstr = textwrap.dedent(
        """
        This is the func_f method.

        { We can escape curly braces like this. }

        Examples
        --------
        We should replace curly brace usage in doctests.

        >>> dict(x = "x", y = "y")
        >>> set((1, 2, 3))
        """
    )
    assert func_f.__doc__ == docstr


@pytest.mark.skipif(sys.platform != "win32",
                    reason="Timezone database is already provided.")
def test_download_tzdata_on_windows():
    tzdata_path = os.path.expandvars(r"%USERPROFILE%\Downloads\tzdata")

    # Download timezone database and remove data in case it already exists
    if (os.path.exists(tzdata_path)):
        shutil.rmtree(tzdata_path)
    with pytest.warns(FutureWarning, match="deprecated"):
        download_tzdata_on_windows()

    # Inspect the folder
    assert os.path.exists(tzdata_path)
    assert os.path.exists(os.path.join(tzdata_path, "windowsZones.xml"))
    assert os.path.exists(os.path.join(tzdata_path, "europe"))
    assert 'version' in os.listdir(tzdata_path)
