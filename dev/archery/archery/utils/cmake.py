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
import re
from shutil import rmtree, which

from .command import Command


class CMake(Command):
    def __init__(self, cmake_bin=None):
        self.bin = cmake_bin if cmake_bin else os.environ.get("CMAKE", "cmake")

    @staticmethod
    def default_generator():
        """ Infer default generator.

        Gives precedence to ninja if there exists an executable named `ninja`
        in the search path.
        """
        found_ninja = which("ninja")
        return "Ninja" if found_ninja else "Unix Makefiles"


cmake = CMake()


class CMakeDefinition:
    """ CMakeDefinition captures the cmake invocation arguments.

    It allows creating build directories with the same definition, e.g.
    ```
    build_1 = cmake_def.build("/tmp/build-1")
    build_2 = cmake_def.build("/tmp/build-2")

    ...

    build1.all()
    build2.all()
    """

    def __init__(self, source, build_type="release", generator=None,
                 definitions=None, env=None):
        """ Initialize a CMakeDefinition

        Parameters
        ----------
        source : str
                 Source directory where the top-level CMakeLists.txt is
                 located. This is usually the root of the project.
        generator : str, optional
        definitions: list(str), optional
        env : dict(str,str), optional
              Environment to use when invoking cmake. This can be required to
              work around cmake deficiencies, e.g. CC and CXX.
        """
        self.source = os.path.abspath(source)
        self.build_type = build_type
        self.generator = generator if generator else cmake.default_generator()
        self.definitions = definitions if definitions else []
        self.env = env

    @property
    def arguments(self):
        """" Return the arguments to cmake invocation. """
        arguments = [
            f"-G{self.generator}",
        ] + self.definitions + [
            self.source
        ]
        return arguments

    def build(self, build_dir, force=False, **kwargs):
        """ Invoke cmake into a build directory.

        Parameters
        ----------
        build_dir : str
                    Directory in which the CMake build will be instanciated.
        force : bool
                If the build folder exists, delete it before. Otherwise if it's
                present, an error will be returned.
        """
        if os.path.exists(build_dir):
            # Extra safety to ensure we're deleting a build folder.
            if not CMakeBuild.is_build_dir(build_dir):
                raise FileExistsError(f"{build_dir} is not a cmake build")
            if not force:
                raise FileExistsError(f"{build_dir} exists use force=True")
            rmtree(build_dir)

        os.mkdir(build_dir)

        cmake(*self.arguments, cwd=build_dir, env=self.env)
        return CMakeBuild(build_dir, self.build_type, definition=self,
                          **kwargs)

    def __repr__(self):
        return f"CMakeDefinition[source={self.source}]"


CMAKE_BUILD_TYPE_RE = re.compile("CMAKE_BUILD_TYPE:STRING=([a-zA-Z]+)")


class CMakeBuild(CMake):
    """ CMakeBuild represents a build directory initialized by cmake.

    The build instance can be used to build/test/install. It alleviates the
    user to know which generator is used.
    """

    def __init__(self, build_dir, build_type, definition=None):
        """ Initialize a CMakeBuild.

        The caller must ensure that cmake was invoked in the build directory.

        Parameters
        ----------
        definition : CMakeDefinition
                     The definition to build from.
        build_dir : str
                    The build directory to setup into.
        """
        assert CMakeBuild.is_build_dir(build_dir)
        super().__init__()
        self.build_dir = os.path.abspath(build_dir)
        self.build_type = build_type
        self.definition = definition

    @property
    def binaries_dir(self):
        return os.path.join(self.build_dir, self.build_type)

    def run(self, *argv, verbose=False, **kwargs):
        cmake_args = ["--build", self.build_dir, "--"]
        extra = []
        if verbose:
            extra.append("-v" if self.bin.endswith("ninja") else "VERBOSE=1")
        # Commands must be ran under the build directory
        super().run(*cmake_args, *extra, *argv, **kwargs, cwd=self.build_dir)
        return self

    def all(self):
        return self.run("all")

    def clean(self):
        return self.run("clean")

    def install(self):
        return self.run("install")

    def test(self):
        return self.run("test")

    @staticmethod
    def is_build_dir(path):
        """ Indicate if a path is CMake build directory.

        This method only checks for the existence of paths and does not do any
        validation whatsoever.
        """
        cmake_cache = os.path.join(path, "CMakeCache.txt")
        cmake_files = os.path.join(path, "CMakeFiles")
        return os.path.exists(cmake_cache) and os.path.exists(cmake_files)

    @staticmethod
    def from_path(path):
        """ Instantiate a CMakeBuild from a path.

        This is used to recover from an existing physical directory (created
        with or without CMakeBuild).

        Note that this method is not idempotent as the original definition will
        be lost. Only build_type is recovered.
        """
        if not CMakeBuild.is_build_dir(path):
            raise ValueError(f"Not a valid CMakeBuild path: {path}")

        build_type = None
        # Infer build_type by looking at CMakeCache.txt and looking for a magic
        # definition
        cmake_cache_path = os.path.join(path, "CMakeCache.txt")
        with open(cmake_cache_path, "r") as cmake_cache:
            candidates = CMAKE_BUILD_TYPE_RE.findall(cmake_cache.read())
            build_type = candidates[0].lower() if candidates else "release"

        return CMakeBuild(path, build_type)

    def __repr__(self):
        return ("CMakeBuild["
                "build = {},"
                "build_type = {},"
                "definition = {}]".format(self.build_dir,
                                          self.build_type,
                                          self.definition))
