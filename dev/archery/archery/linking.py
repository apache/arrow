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

import platform
import re
import subprocess

from .utils.command import Command


_ldd = Command("ldd")
_otool = Command("otool")
_nm = Command("nm")
_ldconfig = Command('ldconfig')


class DependencyError(Exception):
    pass


class DynamicLibrary:

    def __init__(self, path):
        self.path = path

    def list_dependencies(self):
        """
        List the full name of the library dependencies.
        """
        system = platform.system()
        if system == "Linux":
            result = _ldd.run(self.path, stdout=subprocess.PIPE)
            lines = result.stdout.splitlines()
            return [ll.split(None, 1)[0].decode() for ll in lines]
        elif system == "Darwin":
            result = _otool.run("-L", self.path, stdout=subprocess.PIPE)
            lines = result.stdout.splitlines()
            return [dl.split(None, 1)[0].decode() for dl in lines]
        else:
            raise ValueError(f"{platform} is not supported")

    def list_dependency_names(self):
        """
        List the truncated names of the dynamic library dependencies.
        """
        names = []
        for dependency in self.list_dependencies():
            *_, library = dependency.rsplit("/", 1)
            name, *_ = library.split(".", 1)
            names.append(name)
        return names

    def _just_symbols(self, symbol_info):
        return [line.split(' ')[-1] for line in symbol_info if line]

    def list_symbols_for_dependency(self, dependency, remove_symbol_versions=False):
        result = _nm.run('-D',
                         dependency, stdout=subprocess.PIPE)
        lines = result.stdout.decode('utf-8').splitlines()
        if remove_symbol_versions:
            lines = [line.split('@@', 1)[0] for line in lines]
        return self._just_symbols(lines)

    def list_undefined_symbols_for_dependency(self, dependency,
                                              remove_symbol_versions=False):
        result = _nm.run('-u',
                         dependency, stdout=subprocess.PIPE)
        lines = result.stdout.decode('utf-8').splitlines()
        if remove_symbol_versions:
            lines = [line.split('@@', 1)[0] for line in lines]
        return self._just_symbols(lines)

    def find_library_paths(self, libraries):
        paths = {}
        system = platform.system()
        for lib in libraries:
            paths[lib] = []
            if system == 'Linux':
                result = _ldconfig.run('-p', stdout=subprocess.PIPE)
                lines = result.stdout.decode('utf-8').splitlines()
                for line in lines:
                    if lib in line:
                        match = re.search(r' => (.*)', line)
                        if match:
                            paths[lib].append(match.group(1))
            elif system == 'Darwin':
                result = _otool.run("-L", lib, stdout=subprocess.PIPE)
                for line in lines[1:]:
                    match = re.search(r'\s*(\S*)', line)
                    if match:
                        paths[lib].append(match.group(1))
            else:
                raise ValueError(f"{platform} is not supported")
        return paths


def check_dynamic_library_dependencies(path, allowed, disallowed):
    dylib = DynamicLibrary(path)

    for dep in dylib.list_dependency_names():
        if allowed and dep not in allowed:
            raise DependencyError(
                f"Unexpected shared dependency found in {dylib.path}: `{dep}`"
            )
        if disallowed and dep in disallowed:
            raise DependencyError(
                f"Disallowed shared dependency found in {dylib.path}: `{dep}`"
            )
    # Check for undefined symbols
    undefined_symbols = dylib.list_undefined_symbols_for_dependency(path, True)
    print("Allowed: ", allowed)
    expected_lib_paths = dylib.find_library_paths(allowed)
    all_paths = []

    for paths in expected_lib_paths.values():
        all_paths.extend(paths)

    for lb_path in all_paths:
        expected_symbols = dylib.list_symbols_for_dependency(lb_path, True)
        for exp_sym in expected_symbols:
            if exp_sym in undefined_symbols:
                undefined_symbols.remove(exp_sym)

    if undefined_symbols:
        undefined_symbols_str = '\n'.join(undefined_symbols)
        raise DependencyError(
            f"Undefined symbols found in {dylib.path}:\n{undefined_symbols_str}"
        )
