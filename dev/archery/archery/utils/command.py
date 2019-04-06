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
import shutil
import subprocess

from .logger import logger


def find_exec(executable):
    if os.path.exists(executable):
        return executable

    return shutil.which(executable)


# Decorator running a command and returning stdout
class capture_stdout:
    def __init__(self, strip=False):
        self.strip = strip

    def __call__(self, f):
        def strip_it(x):
            return x.strip() if self.strip else x

        def wrapper(*argv, **kwargs):
            return strip_it(fn(*argv, **kwargs, stdout=subprocess.PIPE).stdout)
        return wrapper


class Command:
    def bin(self):
        raise NotImplementedError("Command must implement bin() method")

    def run(self, *argv, raise_on_failure=True, **kwargs):
        invocation = [find_exec(self.bin)]
        invocation.extend(argv)

        logger.debug(f"Executing `{invocation}`")
        result = subprocess.run(invocation, **kwargs)
        if raise_on_failure:
            result.check_returncode()

        return result

    def __call__(self, *argv, **kwargs):
        self.run(*argv, **kwargs)
