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

import argparse
import glob
import itertools
import os
import six
import subprocess
import tempfile
import uuid


ARROW_HOME = os.path.abspath(__file__).rsplit("/", 2)[0]


def guid():
    return uuid.uuid4().hex


def run_cmd(cmd):
    if isinstance(cmd, six.string_types):
        cmd = cmd.split(' ')

    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        # this avoids hiding the stdout / stderr of failed processes
        print('Command failed: %s' % ' '.join(cmd))
        print('With output:')
        print('--------------')
        print(e.output)
        print('--------------')
        raise e

    if isinstance(output, six.binary_type):
        output = output.decode('utf-8')
    return output


class IntegrationRunner(object):

    def __init__(self, json_files, testers, debug=False):
        self.json_files = json_files
        self.testers = testers
        self.temp_dir = tempfile.mkdtemp()
        self.debug = debug

    def run(self):
        for producer, consumer in itertools.product(self.testers,
                                                    self.testers):
            if producer is consumer:
                continue

            print('-- {0} producing, {1} consuming'.format(producer.name,
                                                           consumer.name))

            for json_path in self.json_files:
                print('Testing with {0}'.format(json_path))

                arrow_path = os.path.join(self.temp_dir, guid())

                producer.json_to_arrow(json_path, arrow_path)
                consumer.validate(json_path, arrow_path)


class Tester(object):

    def __init__(self, debug=False):
        self.debug = debug

    def json_to_arrow(self, json_path, arrow_path):
        raise NotImplementedError

    def validate(self, json_path, arrow_path):
        raise NotImplementedError


class JavaTester(Tester):

    ARROW_TOOLS_JAR = os.path.join(ARROW_HOME,
                                   'java/tools/target/arrow-tools-0.1.1-'
                                   'SNAPSHOT-jar-with-dependencies.jar')

    name = 'Java'

    def _run(self, arrow_path=None, json_path=None, command='VALIDATE'):
        cmd = ['java', '-cp', self.ARROW_TOOLS_JAR,
               'org.apache.arrow.tools.Integration']

        if arrow_path is not None:
            cmd.extend(['-a', arrow_path])

        if json_path is not None:
            cmd.extend(['-j', json_path])

        cmd.extend(['-c', command])

        if self.debug:
            print(' '.join(cmd))

        return run_cmd(cmd)

    def validate(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'VALIDATE')

    def json_to_arrow(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'JSON_TO_ARROW')


class CPPTester(Tester):

    CPP_INTEGRATION_EXE = os.environ.get(
        'ARROW_CPP_TESTER',
        os.path.join(ARROW_HOME,
                     'cpp/test-build/debug/json-integration-test'))

    name = 'C++'

    def _run(self, arrow_path=None, json_path=None, command='VALIDATE'):
        cmd = [self.CPP_INTEGRATION_EXE, '--integration']

        if arrow_path is not None:
            cmd.append('--arrow=' + arrow_path)

        if json_path is not None:
            cmd.append('--json=' + json_path)

        cmd.append('--mode=' + command)

        if self.debug:
            print(' '.join(cmd))

        return run_cmd(cmd)

    def validate(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'VALIDATE')

    def json_to_arrow(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'JSON_TO_ARROW')


def get_json_files():
    glob_pattern = os.path.join(ARROW_HOME, 'integration', 'data', '*.json')
    return glob.glob(glob_pattern)


def run_all_tests(debug=False):
    testers = [JavaTester(debug=debug), CPPTester(debug=debug)]
    json_files = get_json_files()

    runner = IntegrationRunner(json_files, testers, debug=debug)
    runner.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Arrow integration test CLI')
    parser.add_argument('--debug', dest='debug', action='store_true',
                        default=False,
                        help='Run executables in debug mode as relevant')

    args = parser.parse_args()
    run_all_tests(debug=args.debug)
