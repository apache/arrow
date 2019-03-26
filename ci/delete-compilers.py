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

"""
Remove all Unix-like compilers from the system.  Used on Windows CI
to force use of MSVC.
"""

import argparse
import os
import sys


all_binaries = {'gcc', 'g++', 'cc', 'c++'}


# Check that a given file can be accessed with the correct mode.
# Additionally check that `file` is not a directory, as on Windows
# directories pass the os.access check.
def _access_check(fn, mode):
    return (os.path.exists(fn) and os.access(fn, mode)
            and not os.path.isdir(fn))


def get_all_paths(cmd, mode=os.F_OK | os.X_OK):
    """Given a command, mode, and a PATH string, return the paths which
    conform to the given mode on the PATH.
    """
    # This code adapted from Lib/shutil.py in CPython
    path = os.environ.get("PATH", os.defpath)
    if not path:
        return []
    path = os.fsdecode(path)
    path = path.split(os.pathsep)

    if sys.platform == "win32":
        # The current directory takes precedence on Windows.
        curdir = os.curdir
        if curdir not in path:
            path.insert(0, curdir)

        # PATHEXT is necessary to check on Windows.
        pathext = os.environ.get("PATHEXT", "").split(os.pathsep)
        # See if the given file matches any of the expected path extensions.
        # This will allow us to short circuit when given "python.exe".
        # If it does match, only test that one, otherwise we have to try
        # others.
        if any(cmd.lower().endswith(ext.lower()) for ext in pathext):
            files = [cmd]
        else:
            files = [cmd + ext for ext in pathext]
    else:
        # On other platforms you don't have things like PATHEXT to tell you
        # what file suffixes are executable, so just pass on cmd as-is.
        files = [cmd]

    results = []
    seen = set()
    for dir in path:
        normdir = os.path.normcase(dir)
        if not normdir in seen:
            seen.add(normdir)
            for thefile in files:
                name = os.path.join(dir, thefile)
                if _access_check(name, mode):
                    results.append(name)
    return results


def main():
    parser = argparse.ArgumentParser(description="""
        Remove all Unix-like compilers from the system.
        Useful on Windows CI to force use of MSVC.
        """)
    parser.add_argument("-n", "--dry-run",
                        help="simulate actions, do not alter filesystem",
                        action="store_true")
    args = parser.parse_args()

    for cmd in all_binaries:
        paths = get_all_paths(cmd)
        for path in paths:
            print("Renaming", path)
            if not args.dry_run:
                os.rename(path, path + ".disabled")


if __name__ == "__main__":
    main()
