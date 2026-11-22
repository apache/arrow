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
from enum import EnumMeta
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


def _tokenize_signature(s):
    lines = s.encode('ascii').splitlines()
    generator = iter(lines).__next__
    return tokenize.tokenize(generator)


def _convert_typehint(tokens):
    tokens = list(tokens)
    names = []
    opening_bracket_reached = False
    skip_annotation = False
    annotation_depth = 0
    for i, token in enumerate(tokens):
        # omit the tokens before the opening bracket
        if not opening_bracket_reached:
            if token.string == '(':
                opening_bracket_reached = True
            else:
                continue

        if skip_annotation:
            if token.string in ('(', '['):
                annotation_depth += 1
            elif token.string in (')', ']'):
                annotation_depth -= 1
            elif annotation_depth == 0 and token.string in (',', ')', '='):
                # end of the annotation, keep the token itself and continue
                # with the default value or the next parameter
                skip_annotation = False
            else:
                # drop the annotation tokens, as annotations are not
                # supported by _signature_fromstr
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

            if token.string == ':' and i + 1 < len(tokens):
                # a parameter annotation follows, skip it until the default
                # value or the end of the parameter
                skip_annotation = True
                continue

            if (token.string == '*' and i + 1 < len(tokens)
                    and tokens[i + 1].string == ')'):
                # a trailing bare `*` marks keyword-only arguments in cython
                # signatures but is not valid python syntax, so drop it
                continue

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
    python_tokens = list(_convert_typehint(cython_tokens))
    python_signature = tokenize.untokenize(python_tokens)
    try:
        return inspect._signature_fromstr(inspect.Signature, obj, python_signature)
    except ValueError:
        # _signature_fromstr can only evaluate constant default values, so
        # retry without the defaults; the parameter names, kinds and their
        # order are what matter for the docstring validation
        python_signature = tokenize.untokenize(_strip_defaults(python_tokens))
        return inspect._signature_fromstr(inspect.Signature, obj, python_signature)


def _strip_defaults(tokens):
    """
    Remove the default values from a tokenized parameter list, e.g. turn
    ``(a=1, b=SomeClass(2))`` into ``(a, b)``. Nested parentheses and
    brackets within a default value are dropped as well.
    """
    depth = 0
    skipping = False
    for token in tokens:
        string = token[1]
        if skipping:
            if string in ('(', '[', '{'):
                depth += 1
                continue
            elif string in (')', ']', '}'):
                if depth == 0:
                    # end of the parameter, keep the closing parenthesis
                    skipping = False
                else:
                    depth -= 1
                    continue
            elif depth == 0 and string == ',':
                skipping = False
            else:
                continue
        elif string == '=':
            skipping = True
            continue
        yield token


def _signature_determinable(obj):
    """
    Whether the signature of the object could be determined.

    Must be called with the patched ``inspect.signature`` active, which
    falls back to parsing the signature from the docstring of cython
    generated callables.
    """
    try:
        return inspect.signature(obj) is not None
    except Exception:
        return False


class NumpyDoc:
    IGNORE_VALIDATION_ERRORS_FOR_TYPE = {
        # Enum function signatures should never be documented
        EnumMeta: ["PR01"]
    }

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
                if any(isinstance(obj, obj_type) and errcode in errcode_list
                       for obj_type, errcode_list
                       in NumpyDoc.IGNORE_VALIDATION_ERRORS_FOR_TYPE.items()):
                    continue
                if errcode in ('PR01', 'PR02', 'PR03') and not _signature_determinable(obj):
                    # numpydoc compares the documented parameters against an
                    # empty signature when the signature could not be
                    # determined, which would report every documented
                    # parameter as unknown (PR02)
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
                    print(f'{symbol} is not available for import')
                else:
                    self.traverse(callback, obj, from_package=from_package)

        return results
