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
from fnmatch import fnmatch
from pathlib import Path
from subprocess import Popen, CompletedProcess
from collections.abc import Mapping, Sequence


def chunk(seq, n):
    """
    divide a sequence into equal sized chunks
    (the last chunk may be smaller, but won't be empty)
    """
    some = []
    for element in seq:
        if len(some) == n:
            yield some
            some = []
        else:
            some.append(element)
    if len(some) > 0:
        yield some


def dechunk(chunks):
    "flatten chunks into a single list"
    seq = []
    for chunk in chunks:
        seq.extend(chunk)
    return seq


def run_parallel(cmds, **kwargs):
    """
    run each of cmds (with shared **kwargs) using subprocess.Popen
    then wait for all of them to complete
    returns a list of each command's CompletedProcess
    """
    procs = []
    for cmd in cmds:
        if not isinstance(cmd, Sequence):
            continue
        procs.append(Popen(cmd, **kwargs))

    for cmd in cmds:
        if not isinstance(cmd, Mapping):
            continue
        cmd_kwargs = kwargs.copy()
        cmd_kwargs.update(cmd)
        procs.append(Popen(**cmd_kwargs))

    complete = []
    for proc in procs:
        result = proc.communicate()
        c = CompletedProcess(proc.args, proc.returncode)
        c.stdout, c.stderr = result
        complete.append(c)
    return complete


_source_extensions = '''
.h
.cc
'''.split()


def get_sources(source_dir, exclude_globs=[]):
    sources = []
    for directory, subdirs, basenames in os.walk(source_dir):
        for path in [Path(directory) / basename for basename in basenames]:
            # filter out non-source files
            if path.suffix not in _source_extensions:
                continue

            # filter out files that match the globs in the globs file
            if any([fnmatch(str(path), glob) for glob in exclude_globs]):
                continue

            sources.append(path.resolve())
    return sources


def stdout_pathcolonline(completed_process, filenames):
    """
    given a completed process which may have reported some files as problematic
    by printing the path name followed by ':' then a line number, examine
    stdout and return the set of actually reported file names
    """
    bfilenames = set()
    for filename in filenames:
        bfilenames.add(filename.encode('utf-8') + b':')
    problem_files = set()
    for line in completed_process.stdout.splitlines():
        for filename in bfilenames:
            if line.startswith(filename):
                problem_files.add(filename.decode('utf-8'))
                bfilenames.remove(filename)
                break
    return problem_files, completed_process.stdout
