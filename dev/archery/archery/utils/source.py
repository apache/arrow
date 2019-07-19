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

from .git import git


class ArrowSources:
    """ ArrowSources is a companion class representing a directory containing
    Apache Arrow's sources.
    """
    # Note that WORKSPACE is a reserved git revision name by this module to
    # reference the current git workspace. In other words, this indicates to
    # ArrowSources.at_revision that no cloning/checkout is required.
    WORKSPACE = "WORKSPACE"

    def __init__(self, path):
        """ Initialize an ArrowSources

        The caller must ensure that path is valid arrow source directory (can
        be checked with ArrowSources.valid)

        Parameters
        ----------
        path : src
        """
        assert isinstance(path, str) and ArrowSources.valid(path)
        self.path = path

    @property
    def cpp(self):
        """ Returns the cpp directory of an Arrow sources. """
        return os.path.join(self.path, "cpp")

    @property
    def python(self):
        """ Returns the python directory of an Arrow sources. """
        return os.path.join(self.path, "python")

    @property
    def git_backed(self):
        """ Indicate if the sources are backed by git. """
        git_path = os.path.join(self.path, ".git")
        return os.path.exists(git_path)

    def at_revision(self, revision, clone_dir):
        """ Return a copy of the current sources for a specified git revision.

        This method may return the current object if no checkout is required.
        The caller is responsible to remove the cloned repository directory.

        The user can use the special WORKSPACE token to mean the current git
        workspace (no checkout performed).

        The second value of the returned tuple indicates if a clone was
        performed.

        Parameters
        ----------
        revision : str
                   Revision to checkout sources at.
        clone_dir : str
                    Path to checkout the local clone.
        """
        if not self.git_backed:
            raise ValueError(f"{self} is not backed by git")

        if revision == ArrowSources.WORKSPACE:
            return self, False

        # A local clone is required to leave the current sources intact such
        # that builds depending on said sources are not invalidated (or worse
        # slightly affected when re-invoking the generator).
        git.clone("--local", self.path, clone_dir)

        # Revision can reference "origin/" (or any remotes) that are not found
        # in the local clone. Thus, revisions are dereferenced in the source
        # repository.
        original_revision = git.rev_parse(revision)

        git.checkout(original_revision, git_dir=clone_dir)

        return ArrowSources(clone_dir), True

    @staticmethod
    def valid(src):
        """ Indicate if current sources are valid. """
        if isinstance(src, ArrowSources):
            return True
        if isinstance(src, str):
            cpp_path = os.path.join(src, "cpp")
            cmake_path = os.path.join(cpp_path, "CMakeLists.txt")
            return os.path.exists(cmake_path)
        return False

    @staticmethod
    def find(path=None):
        """ Infer Arrow sources directory from various method.

        The following guesses are done in order until a valid match is found:

        1. Checks the given optional parameter.

        2. Checks if the environment variable `ARROW_SRC` is defined and use
           this.

        3. Checks if the current working directory (cwd) is an Arrow source
           directory.

        4. Checks if this file (cli.py) is still in the original source
           repository. If so, returns the relative path to the source
           directory.
        """

        # Explicit via environment
        env = os.environ.get("ARROW_SRC")

        # Implicit via cwd
        cwd = os.getcwd()

        # Implicit via current file
        this_dir = os.path.dirname(os.path.realpath(__file__))
        this = os.path.join(this_dir, "..", "..", "..", "..")

        for p in [path, env, cwd, this]:
            if ArrowSources.valid(p):
                return ArrowSources(p)

        return None

    def __repr__(self):
        return f"{self.path}"
