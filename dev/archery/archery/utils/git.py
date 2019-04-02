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

from .command import Command


class Git(Command):
    def __init__(self, *argv, git_bin=None, git_dir=None, **kwargs):
        self.bin = git_bin if git_bin else os.environ.get("GIT", "git")
        self.git_dir = git_dir
        self.run(*argv, **kwargs)

    @property
    def global_opts(self):
        if self.git_dir:
            # For some reason, git does not accept `-Cdir`
            yield "-C"
            yield self.git_dir


class GitSubCommand(Git):
    def run(self, *argv, **kwargs):
        super().run(*self.global_opts, self.cmd, *argv, **kwargs)


class GitClone(GitSubCommand):
    @property
    def cmd(self):
        return "clone"


class GitCheckout(GitSubCommand):
    @property
    def cmd(self):
        return "checkout"
