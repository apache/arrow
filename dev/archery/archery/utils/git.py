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

from .command import Command, capture_stdout


# Decorator prepending argv with the git sub-command found with the method
# name.
def git_cmd(fn):
    # function name is the subcommand
    sub_cmd = fn.__name__.replace("_", "-")

    def wrapper(self, *argv, **kwargs):
        return fn(self, sub_cmd, *argv, **kwargs)
    return wrapper


class Git(Command):
    def __init__(self, git_bin=None):
        self.bin = git_bin if git_bin else os.environ.get("GIT", "git")

    def run_cmd(self, cmd, *argv, git_dir=None, **kwargs):
        """ Inject flags before sub-command in argv. """
        opts = []
        if git_dir and isinstance(git_dir, str):
            opts.extend(("-C", git_dir))

        return self.run(*opts, cmd, *argv, **kwargs)

    @git_cmd
    def clone(self, *argv, **kwargs):
        return self.run_cmd(*argv, **kwargs)

    @git_cmd
    def checkout(self, *argv, **kwargs):
        return self.run_cmd(*argv, **kwargs)

    @git_cmd
    def log(self, *argv, **kwargs):
        return self.run_cmd(*argv, **kwargs)

    @capture_stdout(strip=True)
    @git_cmd
    def rev_parse(self, *argv, **kwargs):
        return self.run_cmd(*argv, **kwargs)

    @capture_stdout(strip=True)
    def head(self, **kwargs):
        """ Return commit pointed by HEAD. """
        return self.rev_parse("HEAD", **kwargs)

    @capture_stdout(strip=True)
    def current_branch(self, **kwargs):
        return self.rev_parse("--abbrev-ref", "HEAD", **kwargs)


git = Git()
