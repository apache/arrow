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

from .command import Command, default_bin


class Maven(Command):
    def __init__(self, maven_bin=None):
        self.bin = default_bin(maven_bin, "mvn")


maven = Maven()


class MavenDefinition:
    """ MavenDefinition captures the maven invocation arguments.

    It allows creating build directories with the same definition, e.g.
    ```
    build_1 = maven_def.build("/tmp/build-1")
    build_2 = maven_def.build("/tmp/build-2")

    ...

    build1.install()
    build2.install()
    """

    def __init__(self, source, build_definitions=None,
                 benchmark_definitions=None, env=None):
        """ Initialize a MavenDefinition

        Parameters
        ----------
        source : str
                 Source directory where the top-level pom.xml is
                 located. This is usually the root of the project.
        build_definitions: list(str), optional
        benchmark_definitions: list(str), optional
        """
        self.source = os.path.abspath(source)
        self.build_definitions = build_definitions if build_definitions else []
        self.benchmark_definitions =\
            benchmark_definitions if benchmark_definitions else []
        self.env = env

    @property
    def build_arguments(self):
        """" Return the arguments to maven invocation for build. """
        arguments = self.build_definitions + [
            "-B", "-DskipTests", "-Drat.skip=true",
            "-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer."
            "Slf4jMavenTransferListener=warn",
            "-T", "2C", "install"
        ]
        return arguments

    def build(self, build_dir, force=False, cmd_kwargs=None, **kwargs):
        """ Invoke maven into a build directory.

        Parameters
        ----------
        build_dir : str
                    Directory in which the Maven build will be instantiated.
        force : bool
                not used now
        """
        if os.path.exists(build_dir):
            # Extra safety to ensure we're deleting a build folder.
            if not MavenBuild.is_build_dir(build_dir):
                raise FileExistsError(
                    "{} is not a maven build".format(build_dir)
                )

        cmd_kwargs = cmd_kwargs if cmd_kwargs else {}
        assert MavenBuild.is_build_dir(build_dir)
        maven(*self.build_arguments, cwd=build_dir, env=self.env, **cmd_kwargs)
        return MavenBuild(build_dir, definition=self, **kwargs)

    @property
    def list_arguments(self):
        """" Return the arguments to maven invocation for list """
        arguments = [
            "-Dskip.perf.benchmarks=false", "-Dbenchmark.list=-lp", "install"
        ]
        return arguments

    @property
    def benchmark_arguments(self):
        """" Return the arguments to maven invocation for benchmark """
        arguments = self.benchmark_definitions + [
            "-Dskip.perf.benchmarks=false", "-Dbenchmark.fork=1",
            "-Dbenchmark.jvmargs=\"-Darrow.enable_null_check_for_get=false "
            "-Darrow.enable_unsafe_memory_access=true\"",
            "install"
        ]
        return arguments

    def __repr__(self):
        return "MavenDefinition[source={}]".format(self.source)


class MavenBuild(Maven):
    """ MavenBuild represents a build directory initialized by maven.

    The build instance can be used to build/test/install. It alleviates the
    user to know which generator is used.
    """

    def __init__(self, build_dir, definition=None):
        """ Initialize a MavenBuild.

        The caller must ensure that maven was invoked in the build directory.

        Parameters
        ----------
        definition : MavenDefinition
                     The definition to build from.
        build_dir : str
                    The build directory to setup into.
        """
        assert MavenBuild.is_build_dir(build_dir)
        super().__init__()
        self.build_dir = os.path.abspath(build_dir)
        self.definition = definition

    @property
    def binaries_dir(self):
        return self.build_dir

    def run(self, *argv, verbose=False, cwd=None, **kwargs):
        extra = []
        if verbose:
            extra.append("-X")
        if cwd is None:
            cwd = self.build_dir
        # Commands must be ran under the directory where pom.xml exists
        return super().run(*extra, *argv, **kwargs, cwd=cwd)

    def build(self, *argv, verbose=False, **kwargs):
        definition_args = self.definition.build_arguments
        cwd = self.binaries_dir
        return self.run(*argv, *definition_args, verbose=verbose, cwd=cwd,
                        env=self.definition.env, **kwargs)

    def list(self, *argv, verbose=False, **kwargs):
        definition_args = self.definition.list_arguments
        cwd = self.binaries_dir + "/performance"
        return self.run(*argv, *definition_args, verbose=verbose, cwd=cwd,
                        env=self.definition.env, **kwargs)

    def benchmark(self, *argv, verbose=False, **kwargs):
        definition_args = self.definition.benchmark_arguments
        cwd = self.binaries_dir + "/performance"
        return self.run(*argv, *definition_args, verbose=verbose, cwd=cwd,
                        env=self.definition.env, **kwargs)

    @staticmethod
    def is_build_dir(path):
        """ Indicate if a path is Maven top directory.

        This method only checks for the existence of paths and does not do any
        validation whatsoever.
        """
        pom_xml = os.path.join(path, "pom.xml")
        performance_dir = os.path.join(path, "performance")
        return os.path.exists(pom_xml) and os.path.isdir(performance_dir)

    @staticmethod
    def from_path(path):
        """ Instantiate a Maven from a path.

        This is used to recover from an existing physical directory (created
        with or without Maven).

        Note that this method is not idempotent as the original definition will
        be lost.
        """
        if not MavenBuild.is_build_dir(path):
            raise ValueError("Not a valid MavenBuild path: {}".format(path))

        return MavenBuild(path, definition=None)

    def __repr__(self):
        return ("MavenBuild["
                "build = {},"
                "definition = {}]".format(self.build_dir,
                                          self.definition))
