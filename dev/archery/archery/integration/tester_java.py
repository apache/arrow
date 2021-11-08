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
from .util import run_cmd, ARROW_ROOT_DEFAULT, log


def load_version_from_pom():
    import xml.etree.ElementTree as ET
    tree = ET.parse(os.path.join(ARROW_ROOT_DEFAULT, 'java', 'pom.xml'))
    tag_pattern = '{http://maven.apache.org/POM/4.0.0}version'
    version_tag = list(tree.getroot().findall(tag_pattern))[0]
    return version_tag.text


class JavaTester(Tester):
    PRODUCER = True
    CONSUMER = True
    FLIGHT_SERVER = True
    FLIGHT_CLIENT = True

    JAVA_OPTS = ['-Dio.netty.tryReflectionSetAccessible=true',
                 '-Darrow.struct.conflict.policy=CONFLICT_APPEND']

    _arrow_version = load_version_from_pom()
    ARROW_TOOLS_JAR = os.environ.get(
        'ARROW_JAVA_INTEGRATION_JAR',
        os.path.join(ARROW_ROOT_DEFAULT,
                     'java/tools/target/arrow-tools-{}-'
                     'jar-with-dependencies.jar'.format(_arrow_version)))
    ARROW_FLIGHT_JAR = os.environ.get(
        'ARROW_FLIGHT_JAVA_INTEGRATION_JAR',
        os.path.join(ARROW_ROOT_DEFAULT,
                     'java/flight/flight-core/target/flight-core-{}-'
                     'jar-with-dependencies.jar'.format(_arrow_version)))
    ARROW_FLIGHT_SERVER = ('org.apache.arrow.flight.example.integration.'
                           'IntegrationTestServer')
    ARROW_FLIGHT_CLIENT = ('org.apache.arrow.flight.example.integration.'
                           'IntegrationTestClient')

    name = 'Java'

    def _run(self, arrow_path=None, json_path=None, command='VALIDATE'):
        cmd = ['java'] + self.JAVA_OPTS + \
            ['-cp', self.ARROW_TOOLS_JAR, 'org.apache.arrow.tools.Integration']

        if arrow_path is not None:
            cmd.extend(['-a', arrow_path])

        if json_path is not None:
            cmd.extend(['-j', json_path])

        cmd.extend(['-c', command])

        if self.debug:
            log(' '.join(cmd))

        run_cmd(cmd)

    def validate(self, json_path, arrow_path, quirks=None):
        return self._run(arrow_path, json_path, 'VALIDATE')

    def json_to_file(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'JSON_TO_ARROW')

    def stream_to_file(self, stream_path, file_path):
        cmd = ['java'] + self.JAVA_OPTS + \
            ['-cp', self.ARROW_TOOLS_JAR,
             'org.apache.arrow.tools.StreamToFile', stream_path, file_path]
        if self.debug:
            log(' '.join(cmd))
        run_cmd(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = ['java'] + self.JAVA_OPTS + \
            ['-cp', self.ARROW_TOOLS_JAR,
             'org.apache.arrow.tools.FileToStream', file_path, stream_path]
        if self.debug:
            log(' '.join(cmd))
        run_cmd(cmd)

    def flight_request(self, port, json_path=None, scenario_name=None):
        cmd = ['java'] + self.JAVA_OPTS + \
            ['-cp', self.ARROW_FLIGHT_JAR, self.ARROW_FLIGHT_CLIENT,
             '-port', str(port)]

        if json_path:
            cmd.extend(('-j', json_path))
        elif scenario_name:
            cmd.extend(('-scenario', scenario_name))
        else:
            raise TypeError("Must provide one of json_path or scenario_name")

        if self.debug:
            log(' '.join(cmd))
        run_cmd(cmd)

    @contextlib.contextmanager
    def flight_server(self, scenario_name=None):
        cmd = ['java'] + self.JAVA_OPTS + \
            ['-cp', self.ARROW_FLIGHT_JAR, self.ARROW_FLIGHT_SERVER,
             '-port', '0']
        if scenario_name:
            cmd.extend(('-scenario', scenario_name))
        if self.debug:
            log(' '.join(cmd))
        server = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        try:
            output = server.stdout.readline().decode()
            if not output.startswith("Server listening on localhost:"):
                server.kill()
                out, err = server.communicate()
                raise RuntimeError(
                    "Flight-Java server did not start properly, "
                    "stdout:\n{}\n\nstderr:\n{}\n"
                    .format(output + out.decode(), err.decode()))
            port = int(output.split(":")[1])
            yield port
        finally:
            server.kill()
            server.wait(5)
