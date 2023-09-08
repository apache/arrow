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
from pathlib import Path
import subprocess

from .git import git


ARROW_ROOT_DEFAULT = os.environ.get(
    'ARROW_ROOT',
    Path(__file__).resolve().parents[4]
)


def arrow_path(path):
    """
    Return full path to a file given its path inside the Arrow repo.
    """
    return os.path.join(ARROW_ROOT_DEFAULT, path)


class InvalidArrowSource(Exception):
    pass


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
        path = Path(path)
        # validate by checking a specific path in the arrow source tree
        if not (path / 'cpp' / 'CMakeLists.txt').exists():
            raise InvalidArrowSource(
                "No Arrow C++ sources found in {}.".format(path)
            )
        self.path = path

    @property
    def archery(self):
        """ Returns the archery directory of an Arrow sources. """
        return self.dev / "archery"

    @property
    def cpp(self):
        """ Returns the cpp directory of an Arrow sources. """
        return self.path / "cpp"

    @property
    def dev(self):
        """ Returns the dev directory of an Arrow sources. """
        return self.path / "dev"

    @property
    def java(self):
        """ Returns the java directory of an Arrow sources. """
        return self.path / "java"

    @property
    def python(self):
        """ Returns the python directory of an Arrow sources. """
        return self.path / "python"

    @property
    def pyarrow(self):
        """ Returns the python/pyarrow directory of an Arrow sources. """
        return self.python / "pyarrow"

    @property
    def r(self):
        """ Returns the r directory of an Arrow sources. """
        return self.path / "r"

    @property
    def git_backed(self):
        """ Indicate if the sources are backed by git. """
        return (self.path / ".git").exists()

    @property
    def git_dirty(self):
        """ Indicate if the sources is a dirty git directory. """
        return self.git_backed and git.dirty(git_dir=self.path)

    def archive(self, path, dereference=False, compressor=None, revision=None):
        """ Saves a git archive at path. """
        if not self.git_backed:
            raise ValueError("{} is not backed by git".format(self))

        rev = revision if revision else "HEAD"
        archive = git.archive("--prefix=apache-arrow/", rev,
                              git_dir=self.path)

        # TODO(fsaintjacques): fix dereference for

        if compressor:
            archive = compressor(archive)

        with open(path, "wb") as archive_fd:
            archive_fd.write(archive)

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
            raise ValueError("{} is not backed by git".format(self))

        if revision == ArrowSources.WORKSPACE:
            return self, False

        # A local clone is required to leave the current sources intact such
        # that builds depending on said sources are not invalidated (or worse
        # slightly affected when re-invoking the generator).
        # "--local" only works when dest dir is on same volume of source dir.
        # "--shared" works even if dest dir is on different volume.
        git.clone("--shared", self.path, clone_dir)

        # Revision can reference "origin/" (or any remotes) that are not found
        # in the local clone. Thus, revisions are dereferenced in the source
        # repository.
        original_revision = git.rev_parse(revision)

        git.checkout(original_revision, git_dir=clone_dir)

        return ArrowSources(clone_dir), True

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
        cwd = Path.cwd()

        # Implicit via current file
        try:
            this = Path(__file__).parents[4]
        except IndexError:
            this = None

        # Implicit via git repository (if archery is installed system wide)
        try:
            repo = git.repository_root(git_dir=cwd)
        except subprocess.CalledProcessError:
            # We're not inside a git repository.
            repo = None

        paths = list(filter(None, [path, env, cwd, this, repo]))
        for p in paths:
            try:
                return ArrowSources(p)
            except InvalidArrowSource:
                pass

        searched_paths = "\n".join([" - {}".format(p) for p in paths])
        raise InvalidArrowSource(
            "Unable to locate Arrow's source directory. "
            "Searched paths are:\n{}".format(searched_paths)
        )

    def __repr__(self):
        return os.fspath(self.path)
