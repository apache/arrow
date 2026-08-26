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

from .tester import Tester
from .util import run_cmd, log
from ..utils.source import ARROW_ROOT_DEFAULT


_EXE_PATH = os.path.join(
    ARROW_ROOT_DEFAULT, "ruby/red-arrow-format/bin/red-arrow-format-integration-test")


class RubyTester(Tester):
    PRODUCER = True
    CONSUMER = True

    name = "Ruby"

    def _run(self, env):
        command_line = [_EXE_PATH]
        if self.debug:
            command_line_string = ""
            for key, value in env.items:
                command_line_string += f"{key}={value} "
            command_line_string += " ".join(command_line)
            log(command_line_string)
        run_cmd(command_line, env=os.environ | env)

    def validate(self, json_path, arrow_path, quirks=None):
        env = {
            "ARROW": arrow_path,
            "COMMAND": "validate",
            "JSON": json_path,
        }
        if quirks:
            for quirk in quirks:
                env[f"QUIRK_{quirk.upper()}"] = "true"
        self._run(env)

    def json_to_file(self, json_path, arrow_path):
        env = {
            "ARROW": arrow_path,
            "COMMAND": "json-to-file",
            "JSON": json_path,
        }
        self._run(env)

    def stream_to_file(self, stream_path, file_path):
        env = {
            "ARROW": file_path,
            "ARROWS": stream_path,
            "COMMAND": "stream-to-file",
        }
        self._run(env)

    def file_to_stream(self, file_path, stream_path):
        env = {
            "ARROW": file_path,
            "ARROWS": stream_path,
            "COMMAND": "file-to-stream",
        }
        self._run(env)
