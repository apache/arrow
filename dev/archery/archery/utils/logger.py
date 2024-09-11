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

from contextlib import contextmanager
import logging
import os

""" Global logger. """
logger = logging.getLogger("archery")


class LoggingContext:
    def __init__(self, quiet=False):
        self.quiet = quiet


ctx = LoggingContext()

in_github_actions = (os.environ.get("GITHUB_ACTIONS") == "true")


@contextmanager
def group(name):
    """
    Group outputs in the given with block.

    This does nothing in non GitHub Actions environment for now.
    """
    if in_github_actions:
        print(f"::group::{name}", flush=True)
    try:
        yield
    finally:
        if in_github_actions:
            print("::endgroup::", flush=True)
