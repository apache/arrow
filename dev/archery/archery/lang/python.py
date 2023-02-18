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

from contextlib import contextmanager
import inspect
import tokenize

try:
    from numpydoc.validate import Docstring, validate
except ImportError:
    have_numpydoc = False
else:
    have_numpydoc = True

from ..compat import _get_module
from ..utils.logger import logger
from ..utils.command import Command, capture_stdout, default_bin


class Flake8(Command):
    def __init__(self, flake8_bin=None):
        self.bin = default_bin(flake8_bin, "flake8")


class CythonLint(Command):
    def __init__(self, cython_lint_bin=None):
        self.bin = default_bin(cython_lint_bin, "cython-lint")


class Autopep8(Command):
    def __init__(self, autopep8_bin=None):
        self.bin = default_bin(autopep8_bin, "autopep8")

    @capture_stdout()
    def run_captured(self, *args, **kwargs):
        return self.run(*args, **kwargs)


def _tokenize_signature(s):
    lines = s.encode('ascii').splitlines()
    generator = iter(lines).__next__
    return tokenize.tokenize(generator)


def _convert_typehint(tokens):
    names = []
    opening_bracket_reached = False
    for token in tokens:
        # omit the tokens before the opening bracket
        if not opening_bracket_reached:
            if token.string == '(':
                opening_bracket_reached = True
            else:
                continue

        if token.type == 1:  # type 1 means NAME token
            names.append(token)
        else:
            if len(names) == 1:
                yield (names[0].type, names[0].string)
            elif len(names) == 2:
                # two "NAME" tokens follow each other which means a cython
                # typehint like `bool argument`, so remove the typehint
                # note that we could convert it to python typehints, but hints
                # are not supported by _signature_fromstr
                yield (names[1].type, names[1].string)
            elif len(names) > 2:
                raise ValueError('More than two NAME tokens follow each other')
            names = []
            yield (token.type, token.string)


def inspect_signature(obj):
    """
    Custom signature inspection primarily for cython generated callables.

    Cython puts the signatures to the first line of the docstrings, which we
    can reuse to parse the python signature from, but some gymnastics are
    required, like removing the cython typehints.

    It converts the cython signature:
        array(obj, type=None, mask=None, size=None, from_pandas=None,
              bool safe=True, MemoryPool memory_pool=None)
    To:
        <Signature (obj, type=None, mask=None, size=None, from_pandas=None,
                    safe=True, memory_pool=None)>
    """
    cython_signature = obj.__doc__.splitlines()[0]
    cython_tokens = _tokenize_signature(cython_signature)
    python_tokens = _convert_typehint(cython_tokens)
    python_signature = tokenize.untokenize(python_tokens)
    return inspect._signature_fromstr(inspect.Signature, obj, python_signature)


class NumpyDoc:

    def __init__(self, symbols=None):
        if not have_numpydoc:
            raise RuntimeError(
                'Numpydoc is not available, install with command: '
                'pip install numpydoc==1.1.0'
            )
        self.symbols = set(symbols or {'pyarrow'})

    def traverse(self, fn, obj, from_package):
        """Apply a function on publicly exposed API components.

        Recursively iterates over the members of the passed object. It omits
        any '_' prefixed and thirdparty (non pyarrow) symbols.

        Parameters
        ----------
        fn : callable
            A function to apply on all traversed objects.
        obj : Any
            The object to start from.
        from_package : string
            Predicate to only consider objects from this package.
        """
        todo = [obj]
        seen = set()

        while todo:
            obj = todo.pop()
            if obj in seen:
                continue
            else:
                seen.add(obj)

            fn(obj)

            for name in dir(obj):
                if name.startswith('_'):
                    continue

                member = getattr(obj, name)
                module = _get_module(member)
                if module is None or not module.startswith(from_package):
                    continue
                # Is it a Cython-generated method? If so, try to detect
                # whether it only has a implicitly-generated docstring,
                # and no user-defined docstring following it.
                # The generated docstring would lack description of method
                # parameters and therefore fail Numpydoc validation.
                if hasattr(member, '__objclass__'):
                    doc = getattr(member, '__doc__', None)
                    # The Cython-generated docstring would be a one-liner,
                    # such as "ReadOptions.equals(self, ReadOptions other)".
                    if (doc and '\n' not in doc and f'.{name}(' in doc):
                        continue
                todo.append(member)

    @contextmanager
    def _apply_patches(self):
        """
        Patch Docstring class to bypass loading already loaded python objects.
        """
        orig_load_obj = Docstring._load_obj
        orig_signature = inspect.signature

        @staticmethod
        def _load_obj(obj):
            # By default it expects a qualname and import the object, but we
            # have already loaded object after the API traversal.
            if isinstance(obj, str):
                return orig_load_obj(obj)
            else:
                return obj

        def signature(obj):
            # inspect.signature tries to parse __text_signature__ if other
            # properties like __signature__ doesn't exists, but cython
            # doesn't set that property despite that embedsignature cython
            # directive is set. The only way to inspect a cython compiled
            # callable's signature to parse it from __doc__ while
            # embedsignature directive is set during the build phase.
            # So path inspect.signature function to attempt to parse the first
            # line of callable.__doc__ as a signature.
            try:
                return orig_signature(obj)
            except Exception as orig_error:
                try:
                    return inspect_signature(obj)
                except Exception:
                    raise orig_error

        try:
            Docstring._load_obj = _load_obj
            inspect.signature = signature
            yield
        finally:
            Docstring._load_obj = orig_load_obj
            inspect.signature = orig_signature

    def validate(self, from_package='', allow_rules=None,
                 disallow_rules=None):
        results = []

        def callback(obj):
            try:
                result = validate(obj)
            except OSError as e:
                symbol = f"{_get_module(obj, default='')}.{obj.__name__}"
                logger.warning(f"Unable to validate `{symbol}` due to `{e}`")
                return

            errors = []
            for errcode, errmsg in result.get('errors', []):
                if allow_rules and errcode not in allow_rules:
                    continue
                if disallow_rules and errcode in disallow_rules:
                    continue
                errors.append((errcode, errmsg))

            if len(errors):
                result['errors'] = errors
                results.append((obj, result))

        with self._apply_patches():
            for symbol in self.symbols:
                try:
                    obj = Docstring._load_obj(symbol)
                except (ImportError, AttributeError):
                    print('{} is not available for import'.format(symbol))
                else:
                    self.traverse(callback, obj, from_package=from_package)

        return results
