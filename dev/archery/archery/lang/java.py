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

from ..utils.command import Command, CommandStackMixin, default_bin
from ..utils.maven import MavenDefinition


class Java(Command):
    def __init__(self, java_bin=None):
        self.bin = default_bin(java_bin, "java")


class Jar(CommandStackMixin, Java):
    def __init__(self, jar, *args, **kwargs):
        self.jar = jar
        self.argv = ("-jar", jar)
        Java.__init__(self, *args, **kwargs)


class JavaConfiguration:
    def __init__(self,

                 # toolchain
                 java_home=None, java_options=None,
                 # build & benchmark
                 build_extras=None, benchmark_extras=None):
        self.java_home = java_home
        self.java_options = java_options

        self.build_extras = list(build_extras) if build_extras else []
        self.benchmark_extras = list(
            benchmark_extras) if benchmark_extras else []

    @property
    def build_definitions(self):
        return self.build_extras

    @property
    def benchmark_definitions(self):
        return self.benchmark_extras

    @property
    def environment(self):
        env = os.environ.copy()

        if self.java_home:
            env["JAVA_HOME"] = self.java_home

        if self.java_options:
            env["JAVA_OPTIONS"] = self.java_options

        return env


class JavaMavenDefinition(MavenDefinition):
    def __init__(self, source, conf, **kwargs):
        self.configuration = conf
        super().__init__(source, **kwargs,
                         build_definitions=conf.build_definitions,
                         benchmark_definitions=conf.benchmark_definitions,
                         env=conf.environment)
