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
    ARROW_ROOT_DEFAULT,
    "csharp/artifacts/Apache.Arrow.IntegrationTest",
    "Debug/net7.0/Apache.Arrow.IntegrationTest",
)


class CSharpTester(Tester):
    PRODUCER = True
    CONSUMER = True

    name = 'C#'

    def _run(self, json_path=None, arrow_path=None, command='validate'):
        cmd = [_EXE_PATH]

        cmd.extend(['--mode', command])

        if json_path is not None:
            cmd.extend(['-j', json_path])

        if arrow_path is not None:
            cmd.extend(['-a', arrow_path])

        if self.debug:
            log(' '.join(cmd))

        run_cmd(cmd)

    def validate(self, json_path, arrow_path, quirks=None):
        return self._run(json_path, arrow_path, 'validate')

    def json_to_file(self, json_path, arrow_path):
        return self._run(json_path, arrow_path, 'json-to-arrow')

    def stream_to_file(self, stream_path, file_path):
        cmd = [_EXE_PATH]
        cmd.extend(['--mode', 'stream-to-file', '-a', file_path])
        cmd.extend(['<', stream_path])
        self.run_shell_command(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = [_EXE_PATH]
        cmd.extend(['--mode', 'file-to-stream'])
        cmd.extend(['-a', file_path, '>', stream_path])
        self.run_shell_command(cmd)
