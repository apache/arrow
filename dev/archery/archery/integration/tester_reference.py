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
import subprocess

from .tester import Tester
from .util import run_cmd, log


_HOME = os.getenv("HOME", "~")
_GOPATH = os.getenv("GOPATH", os.path.join(_HOME, "go"))
_GOBIN = os.environ.get("GOBIN", os.path.join(_GOPATH, "bin"))

_FLIGHT_SERVER_CMD = [os.path.join(_GOBIN, "reference-flight-integration-server")]
_FLIGHT_CLIENT_CMD = [
    os.path.join(_GOBIN, "reference-flight-integration-client"),
    "-host",
    "localhost",
]


class FlightReferenceTester(Tester):
    # PRODUCER = True
    # CONSUMER = True
    FLIGHT_SERVER = True
    FLIGHT_CLIENT = True
    # C_DATA_SCHEMA_EXPORTER = True
    # C_DATA_ARRAY_EXPORTER = True
    # C_DATA_SCHEMA_IMPORTER = True
    # C_DATA_ARRAY_IMPORTER = True

    name = 'Reference Implementation'

    @contextlib.contextmanager
    def flight_server(self, scenario_name=None):
        cmd = _FLIGHT_SERVER_CMD + ['-port=0']
        if scenario_name:
            cmd = cmd + ['-scenarios', scenario_name]
        if self.debug:
            log(' '.join(cmd))
        server = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            output = server.stdout.readline().decode()
            if not output.startswith('Server listening on localhost:'):
                server.kill()
                out, err = server.communicate()
                raise RuntimeError(
                    'Flight-Go server did not start properly, '
                    'stdout: \n{}\n\nstderr:\n{}\n'.format(
                        output + out.decode(), err.decode()
                    )
                )
            port = int(output.split(':')[1])
            yield port
        finally:
            server.terminate()
            return_code = server.wait(5)
            if return_code != 0:
                raise RuntimeError(server.stderr.read().decode())

    def flight_request(self, port, json_path=None, scenario_name=None):
        cmd = _FLIGHT_CLIENT_CMD + [
            '-port=' + str(port),
        ]
        if json_path:
            cmd.extend(('-path', json_path))
        elif scenario_name:
            cmd.extend(('-scenarios', scenario_name))
        else:
            raise TypeError('Must provide one of json_path or scenario_name')

        if self.debug:
            log(' '.join(cmd))
        
        run_cmd(cmd)
