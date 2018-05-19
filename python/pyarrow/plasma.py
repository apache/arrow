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

import contextlib
import os
import pyarrow as pa
import subprocess
import shutil
import tempfile
import time

from pyarrow._plasma import (ObjectID, ObjectNotAvailable, # noqa
                             PlasmaBuffer, PlasmaClient, connect)


@contextlib.contextmanager
def start_plasma_store(plasma_store_memory,
                       use_valgrind=False, use_profiler=False,
                       use_one_memory_mapped_file=False,
                       plasma_directory=None, use_hugepages=False):
    """Start a plasma store process.
    Args:
        plasma_store_memory (int): Capacity of the plasma store in bytes.
        use_valgrind (bool): True if the plasma store should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the plasma store should be started inside
            a profiler. If this is True, use_valgrind must be False.
        use_one_memory_mapped_file: If True, then the store will use only a
            single memory-mapped file.
        plasma_directory (str): Directory where plasma memory mapped files
            will be stored.
        use_hugepages (bool): True if the plasma store should use huge pages.
    Return:
        A tuple of the name of the plasma store socket and the process ID of
            the plasma store process.
    """
    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")

    tmpdir = tempfile.mkdtemp(prefix='test_plasma-')
    try:
        plasma_store_name = os.path.join(tmpdir, 'plasma.sock')
        plasma_store_executable = os.path.join(pa.__path__[0], "plasma_store")
        command = [plasma_store_executable,
                   "-s", plasma_store_name,
                   "-m", str(plasma_store_memory)]
        if use_one_memory_mapped_file:
            command += ["-f"]
        if plasma_directory:
            command += ["-d", plasma_directory]
        if use_hugepages:
            command += ["-h"]
        stdout_file = None
        stderr_file = None
        if use_valgrind:
            command = ["valgrind",
                       "--track-origins=yes",
                       "--leak-check=full",
                       "--show-leak-kinds=all",
                       "--leak-check-heuristics=stdstring",
                       "--error-exitcode=1"] + command
            proc = subprocess.Popen(command, stdout=stdout_file,
                                    stderr=stderr_file)
            time.sleep(1.0)
        elif use_profiler:
            command = ["valgrind", "--tool=callgrind"] + command
            proc = subprocess.Popen(command, stdout=stdout_file,
                                    stderr=stderr_file)
            time.sleep(1.0)
        else:
            proc = subprocess.Popen(command, stdout=stdout_file,
                                    stderr=stderr_file)
            time.sleep(0.1)
        rc = proc.poll()
        if rc is not None:
            raise RuntimeError("plasma_store exited unexpectedly with "
                               "code %d" % (rc,))

        yield plasma_store_name, proc
    finally:
        if proc.poll() is None:
            proc.kill()
        shutil.rmtree(tmpdir)
