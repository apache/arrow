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

# Base class for language-specific integration test harnesses

import subprocess

from .util import log


class Tester(object):
    """
    The interface to declare a tester to run integration tests against.
    """
    # whether the language supports producing / writing IPC
    PRODUCER = False
    # whether the language supports consuming / reading IPC
    CONSUMER = False
    # whether the language supports serving Flight
    FLIGHT_SERVER = False
    # whether the language supports receiving Flight
    FLIGHT_CLIENT = False

    # the name shown in the logs
    name = "unknown"

    def __init__(self, debug=False, **args):
        self.args = args
        self.debug = debug

    def run_shell_command(self, cmd):
        cmd = ' '.join(cmd)
        if self.debug:
            log(cmd)
        subprocess.check_call(cmd, shell=True)

    def json_to_file(self, json_path, arrow_path):
        """
        Run the conversion of an Arrow JSON integration file
        to an Arrow IPC file
        """
        raise NotImplementedError

    def stream_to_file(self, stream_path, file_path):
        """
        Run the conversion of an Arrow IPC stream to an
        Arrow IPC file
        """
        raise NotImplementedError

    def file_to_stream(self, file_path, stream_path):
        """
        Run the conversion of an Arrow IPC file to an Arrow IPC stream
        """
        raise NotImplementedError

    def validate(self, json_path, arrow_path, quirks=None):
        """
        Validate that the Arrow IPC file is equal to the corresponding
        Arrow JSON integration file
        """
        raise NotImplementedError

    def flight_server(self, scenario_name=None):
        """Start the Flight server on a free port.

        This should be a context manager that returns the port as the
        managed object, and cleans up the server on exit.
        """
        raise NotImplementedError

    def flight_request(self, port, json_path=None, scenario_name=None):
        raise NotImplementedError
