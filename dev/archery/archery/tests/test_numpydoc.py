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

import pytest

from archery.lang.python import (
    _convert_typehint,
    _signature_determinable,
    _strip_defaults,
    inspect_signature,
    NumpyDoc,
)


def _tokenize(s):
    from archery.lang.python import _tokenize_signature
    return _tokenize_signature(s)


def _token_strings(tokens):
    # ignore the empty trailing ENDMARKER/NEWLINE tokens
    return [string for _, string in tokens if string.strip()]


class FakeCythonCallable:
    """Object whose signature can only be parsed from the docstring."""

    def __init__(self, doc):
        self.__doc__ = doc


def test_convert_typehint_cython_typehints():
    # `bool argument` style cython typehints are dropped
    tokens = _convert_typehint(_tokenize("(bool anonymous=False, region=None)"))
    assert _token_strings(tokens) == [
        '(', 'anonymous', '=', 'False', ',', 'region', '=', 'None', ')'
    ]


def test_convert_typehint_annotation_with_default():
    # annotations are not supported by _signature_fromstr, so they are
    # dropped while keeping the default value
    tokens = _convert_typehint(_tokenize(
        "(retry_strategy: S3RetryStrategy = AwsStandardS3RetryStrategy("
        "max_attempts=3), force_virtual_addressing=False)"))
    assert _token_strings(tokens) == [
        '(', 'retry_strategy', '=', 'AwsStandardS3RetryStrategy', '(',
        'max_attempts', '=', '3', ')', ',', 'force_virtual_addressing', '=',
        'False', ')'
    ]


def test_convert_typehint_annotation_without_default():
    tokens = _convert_typehint(_tokenize("(a: SomeType, b=None)"))
    assert _token_strings(tokens) == [
        '(', 'a', ',', 'b', '=', 'None', ')'
    ]


def test_convert_typehint_trailing_keyword_only_marker():
    # a trailing bare `*` is not valid python syntax and is dropped, while a
    # keyword-only marker followed by more parameters is kept
    tokens = _convert_typehint(_tokenize("(use_mmap=False, *)"))
    assert _token_strings(tokens) == [
        '(', 'use_mmap', '=', 'False', ',', ')'
    ]
    tokens = _convert_typehint(_tokenize("(access_key=None, *, secret_key=None)"))
    assert _token_strings(tokens) == [
        '(', 'access_key', '=', 'None', ',', '*', ',', 'secret_key', '=',
        'None', ')'
    ]


def test_strip_defaults():
    tokens = list(_convert_typehint(_tokenize(
        "(a=1, b=SomeClass(2), *, c='x')")))
    assert _token_strings(_strip_defaults(tokens)) == [
        '(', 'a', ',', 'b', ',', '*', ',', 'c', ')'
    ]


def test_inspect_signature_with_non_constant_default():
    # _signature_fromstr can only evaluate constant defaults, so parsing
    # falls back to a signature without the default values
    obj = FakeCythonCallable(
        "func(a, b=S3RetryStrategy, retry_strategy: SomeStrategy = "
        "AwsStandardS3RetryStrategy(max_attempts=3))\n\n    Docstring.")
    signature = inspect_signature(obj)
    assert list(signature.parameters) == ['a', 'b', 'retry_strategy']


def test_inspect_signature_preserves_defaults_when_evaluable():
    obj = FakeCythonCallable(
        "func(a=1, b='x')\n\n    Docstring.")
    signature = inspect_signature(obj)
    assert list(signature.parameters) == ['a', 'b']
    assert signature.parameters['a'].default == 1


def test_signature_determinable():
    doc = NumpyDoc()

    with doc._apply_patches():
        obj = FakeCythonCallable("func(a, b=None)\n\n    Docstring.")
        assert _signature_determinable(obj)

        # empty signature line, the signature cannot be determined
        obj = FakeCythonCallable("\n    Docstring without a signature.")
        assert not _signature_determinable(obj)


def test_validate_skips_parameter_rules_without_signature():
    # numpydoc compares documented parameters against an empty signature
    # when the signature cannot be determined, which would report every
    # documented parameter as unknown (PR02)
    from numpydoc.validate import validate as numpydoc_validate

    doc = NumpyDoc()
    obj = FakeCythonCallable(
        "\n    Parameters\n    ----------\n    a : int\n        A param.\n")

    with doc._apply_patches():
        assert not _signature_determinable(obj)
        result = numpydoc_validate(obj)
        errors = []
        for errcode, errmsg in result['errors']:
            # same filtering as NumpyDoc.validate applies
            if errcode in ('PR01', 'PR02', 'PR03') and \
                    not _signature_determinable(obj):
                continue
            errors.append(errcode)

    assert 'PR02' in [code for code, _ in result['errors']]
    assert 'PR02' not in errors
