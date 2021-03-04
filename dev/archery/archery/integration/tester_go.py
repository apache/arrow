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


class GoTester(Tester):
    PRODUCER = True
    CONSUMER = True

    # FIXME(sbinet): revisit for Go modules
    HOME = os.getenv('HOME', '~')
    GOPATH = os.getenv('GOPATH', os.path.join(HOME, 'go'))
    GOBIN = os.environ.get('GOBIN', os.path.join(GOPATH, 'bin'))

    GO_INTEGRATION_EXE = os.path.join(GOBIN, 'arrow-json-integration-test')
    STREAM_TO_FILE = os.path.join(GOBIN, 'arrow-stream-to-file')
    FILE_TO_STREAM = os.path.join(GOBIN, 'arrow-file-to-stream')

    name = 'Go'

    def _run(self, arrow_path=None, json_path=None, command='VALIDATE'):
        cmd = [self.GO_INTEGRATION_EXE]

        if arrow_path is not None:
            cmd.extend(['-arrow', arrow_path])

        if json_path is not None:
            cmd.extend(['-json', json_path])

        cmd.extend(['-mode', command])

        if self.debug:
            log(' '.join(cmd))

        run_cmd(cmd)

    def validate(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'VALIDATE')

    def json_to_file(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'JSON_TO_ARROW')

    def stream_to_file(self, stream_path, file_path):
        cmd = [self.STREAM_TO_FILE, '<', stream_path, '>', file_path]
        self.run_shell_command(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = [self.FILE_TO_STREAM, file_path, '>', stream_path]
        self.run_shell_command(cmd)
