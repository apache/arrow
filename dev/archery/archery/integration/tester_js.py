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
from .util import run_cmd, ARROW_ROOT_DEFAULT, log


class JSTester(Tester):
    PRODUCER = True
    CONSUMER = True

    EXE_PATH = os.path.join(ARROW_ROOT_DEFAULT, 'js/bin')
    VALIDATE = os.path.join(EXE_PATH, 'integration.js')
    JSON_TO_ARROW = os.path.join(EXE_PATH, 'json-to-arrow.js')
    STREAM_TO_FILE = os.path.join(EXE_PATH, 'stream-to-file.js')
    FILE_TO_STREAM = os.path.join(EXE_PATH, 'file-to-stream.js')

    name = 'JS'

    def _run(self, exe_cmd, arrow_path=None, json_path=None,
             command='VALIDATE'):
        cmd = [exe_cmd]

        if arrow_path is not None:
            cmd.extend(['-a', arrow_path])

        if json_path is not None:
            cmd.extend(['-j', json_path])

        cmd.extend(['--mode', command])

        if self.debug:
            log(' '.join(cmd))

        run_cmd(cmd)

    def validate(self, json_path, arrow_path, quirks=None):
        return self._run(self.VALIDATE, arrow_path, json_path, 'VALIDATE')

    def json_to_file(self, json_path, arrow_path):
        cmd = ['node',
               '--no-warnings', self.JSON_TO_ARROW,
               '-a', arrow_path,
               '-j', json_path]
        self.run_shell_command(cmd)

    def stream_to_file(self, stream_path, file_path):
        cmd = ['node', '--no-warnings', self.STREAM_TO_FILE,
               '<', stream_path,
               '>', file_path]
        self.run_shell_command(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = ['node', '--no-warnings', self.FILE_TO_STREAM,
               '<', file_path,
               '>', stream_path]
        self.run_shell_command(cmd)
