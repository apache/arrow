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

"""Helpers for the type annotation tests in this directory.

Each test pairs a static and a runtime check of the same expression::

    check(assert_type(pa.lib.list_(pa.lib.int32()), pa.lib.ListType),
          pa.lib.ListType)

``assert_type`` is verified by every type checker we run in CI (mypy,
pyright and ty) and is a no-op at runtime. ``check`` runs under pytest and
verifies that the value really is an instance of the given class, so the
stubs can't drift away from what pyarrow actually returns.

Code that type checkers should *reject* goes under
``if TYPE_CHECKING_INVALID_USAGE:`` with a ``# type: ignore[<mypy code>]``
comment. The block never runs, and mypy and pyright are configured to
report unused ignores in these tests, so the test fails if the usage stops
being an error.
"""

import sys
from typing import Any, Final, TYPE_CHECKING, TypeVar

if sys.version_info >= (3, 11):
    from typing import assert_type
else:
    from typing_extensions import assert_type

__all__ = ["TYPE_CHECKING_INVALID_USAGE", "assert_type", "check"]

TYPE_CHECKING_INVALID_USAGE: Final = TYPE_CHECKING

_T = TypeVar("_T")


def check(actual: _T, klass: type[Any] | tuple[type[Any], ...]) -> _T:
    """Assert that ``actual`` is an instance of ``klass`` and return it."""
    if not isinstance(actual, klass):
        raise AssertionError(
            f"Expected an instance of {klass}, got {type(actual)}: {actual!r}"
        )
    return actual
