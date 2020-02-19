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

import importlib
from contextlib import contextmanager

try:
    from numpydoc.validate import Docstring, validate
except ImportError:
    have_numpydoc = False
else:
    have_numpydoc = True

from ..utils.command import Command, default_bin


class Flake8(Command):
    def __init__(self, flake8_bin=None):
        self.bin = default_bin(flake8_bin, "flake8")


class NumpyDoc:

    def __init__(self, modules=None):
        if not have_numpydoc:
            raise RuntimeError(
                'Numpydoc is not available, install the development version '
                'of it via pip ...'
            )
        self.modules = set(modules or {'pyarrow'})

    def traverse(self, fn, obj, from_package):
        """Apply a function on publicly exposed API components.

        Recursively iterates over the members of the passed object. It omits
        any '_' prefixed and thirdparty (non pyarrow) symbols.

        Parameters
        ----------
        obj : Any
        from_package : string, default 'pyarrow'
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
                module = getattr(member, '__module__', None)
                if not (module and module.startswith(from_package)):
                    continue

                todo.append(member)

    @contextmanager
    def _patch_numpydoc(self):
        """
        Patch Docstring class to bypass loading already loaded python objects.

        By default it expects a qualname and import the object, but we have
        already loaded object after the API traversal.
        """
        original = Docstring._load_obj
        try:
            Docstring._load_obj = staticmethod(lambda obj: obj)
            yield
        finally:
            Docstring._load_obj = original

    def validate(self, rules_blacklist=None, rules_whitelist=None):
        results = []

        def callback(obj):
            result = validate(obj)

            errors = []
            for errcode, errmsg in result.get('errors', []):
                if rules_whitelist and errcode not in rules_whitelist:
                    continue
                if rules_blacklist and errcode in rules_blacklist:
                    continue
                errors.append((errcode, errmsg))

            if len(errors):
                result['errors'] = errors
                results.append((obj, result))

        with self._patch_numpydoc():
            for module_name in self.modules:
                try:
                    module = importlib.import_module(module_name)
                except ImportError:
                    print('{} is not available for import'.format(module_name))
                    continue
                else:
                    self.traverse(callback, module, module_name)

        return results
