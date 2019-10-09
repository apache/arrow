# licensed to the Apache Software Foundation (ASF) under one
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

import glob
import gzip
import itertools
import os
import sys
import tempfile
import traceback

from .tester_cpp import CPPTester
from .tester_go import GoTester
from .tester_java import JavaTester
from .tester_js import JSTester
from .util import ARROW_ROOT_DEFAULT, guid, SKIP_ARROW, SKIP_FLIGHT
from . import datagen


class IntegrationRunner(object):

    def __init__(self, json_files, testers, tempdir=None, debug=False,
                 stop_on_error=True, gold_dirs=None, **unused_kwargs):
        self.json_files = json_files
        self.testers = testers
        self.temp_dir = tempdir or tempfile.mkdtemp()
        self.debug = debug
        self.stop_on_error = stop_on_error
        self.gold_dirs = gold_dirs

    def run(self):
        failures = []

        for producer, consumer in itertools.product(
                filter(lambda t: t.PRODUCER, self.testers),
                filter(lambda t: t.CONSUMER, self.testers)):
            for failure in self._compare_implementations(
              producer, consumer, self._produce_consume, self.json_files):
                failures.append(failure)
        if self.gold_dirs:
            for gold_dir, consumer in itertools.product(
                    self.gold_dirs,
                    filter(lambda t: t.CONSUMER, self.testers)):
                print('\n\n\n\n')
                print('******************************************************')
                print('Tests against golden files in {}'.format(gold_dir))
                print('******************************************************')

                def run_gold(producer, consumer, test_case):
                    self._run_gold(gold_dir, producer, consumer, test_case)
                for failure in self._compare_implementations(
                  consumer, consumer,  run_gold, self._gold_tests(gold_dir)):
                    failures.append(failure)

        return failures

    def run_flight(self):
        failures = []
        servers = filter(lambda t: t.FLIGHT_SERVER, self.testers)
        clients = filter(lambda t: (t.FLIGHT_CLIENT and t.CONSUMER),
                         self.testers)
        for server, client in itertools.product(servers, clients):
            for failure in self._compare_flight_implementations(server,
                                                                client):
                failures.append(failure)
        return failures

    def _gold_tests(self, gold_dir):
        prefix = os.path.basename(os.path.normpath(gold_dir))
        SUFFIX = ".json.gz"
        golds = [jf for jf in os.listdir(gold_dir) if jf.endswith(SUFFIX)]
        for json_path in golds:
            name = json_path[json_path.index('_')+1: -len(SUFFIX)]
            base_name = prefix + "_" + name + ".gold.json"
            out_path = os.path.join(self.temp_dir, base_name)
            with gzip.open(os.path.join(gold_dir, json_path)) as i:
                with open(out_path, "wb") as out:
                    out.write(i.read())

            try:
                skip = next(f for f in self.json_files
                            if f.name == name).skip
            except StopIteration:
                skip = set()
            yield datagen.JsonFile(name, None, None, skip=skip, path=out_path)

    def _compare_implementations(
            self, producer, consumer, run_binaries, test_cases):
        print('##########################################################')
        print(
            '{0} producing, {1} consuming'.format(producer.name, consumer.name)
        )
        print('##########################################################')

        for test_case in test_cases:
            json_path = test_case.path
            print('==========================================================')
            print('Testing file {0}'.format(json_path))
            print('==========================================================')

            if producer.name in test_case.skip:
                print('-- Skipping test because producer {0} does '
                      'not support'.format(producer.name))
                continue

            if consumer.name in test_case.skip:
                print('-- Skipping test because consumer {0} does '
                      'not support'.format(consumer.name))
                continue

            if SKIP_ARROW in test_case.skip:
                print('-- Skipping test')
                continue

            try:
                run_binaries(producer, consumer, test_case)
            except Exception:
                traceback.print_exc()
                yield (test_case, producer, consumer, sys.exc_info())
                if self.stop_on_error:
                    break
                else:
                    continue

    def _produce_consume(self, producer, consumer, test_case):
        # Make the random access file
        json_path = test_case.path
        file_id = guid()[:8]
        name = os.path.splitext(os.path.basename(json_path))[0]

        producer_file_path = os.path.join(self.temp_dir, file_id + '_' +
                                          name + '.json_as_file')
        producer_stream_path = os.path.join(self.temp_dir, file_id + '_' +
                                            name + '.producer_file_as_stream')
        consumer_file_path = os.path.join(self.temp_dir, file_id + '_' +
                                          name + '.consumer_stream_as_file')

        print('-- Creating binary inputs')
        producer.json_to_file(json_path, producer_file_path)

        # Validate the file
        print('-- Validating file')
        consumer.validate(json_path, producer_file_path)

        print('-- Validating stream')
        producer.file_to_stream(producer_file_path, producer_stream_path)
        consumer.stream_to_file(producer_stream_path, consumer_file_path)
        consumer.validate(json_path, consumer_file_path)

    def _run_gold(self, gold_dir, producer, consumer, test_case):
        json_path = test_case.path

        # Validate the file
        print('-- Validating file')
        producer_file_path = os.path.join(
            gold_dir, "generated_" + test_case.name + ".arrow_file")
        consumer.validate(json_path, producer_file_path)

        print('-- Validating stream')
        consumer_stream_path = os.path.join(
            gold_dir, "generated_" + test_case.name + ".stream")
        file_id = guid()[:8]
        name = os.path.splitext(os.path.basename(json_path))[0]

        consumer_file_path = os.path.join(self.temp_dir, file_id + '_' +
                                          name + '.consumer_stream_as_file')

        consumer.stream_to_file(consumer_stream_path, consumer_file_path)
        consumer.validate(json_path, consumer_file_path)

    def _compare_flight_implementations(self, producer, consumer):
        print('##########################################################')
        print(
            '{0} serving, {1} requesting'.format(producer.name, consumer.name)
        )
        print('##########################################################')

        for test_case in self.json_files:
            json_path = test_case.path
            print('=' * 58)
            print('Testing file {0}'.format(json_path))
            print('=' * 58)

            if ('Java' in (producer.name, consumer.name) and
               "map" in test_case.name):
                print('TODO(ARROW-1279): Enable map tests ' +
                      ' for Java and JS once Java supports them and JS\'' +
                      ' are unbroken')
                continue

            if SKIP_FLIGHT in test_case.skip:
                print('-- Skipping test')
                continue

            try:
                with producer.flight_server():
                    # Have the client upload the file, then download and
                    # compare
                    consumer.flight_request(producer.FLIGHT_PORT, json_path)
            except Exception:
                traceback.print_exc()
                yield (test_case, producer, consumer, sys.exc_info())
                continue


def get_static_json_files():
    glob_pattern = os.path.join(ARROW_ROOT_DEFAULT,
                                'integration', 'data', '*.json')
    return [
        datagen.JsonFile(name=os.path.basename(p), path=p, skip=set(),
                         schema=None, batches=None)
        for p in glob.glob(glob_pattern)
    ]


def run_all_tests(enable_cpp=True, enable_java=True, enable_js=True,
                  enable_go=True, run_flight=False,
                  tempdir=None, **kwargs):
    tempdir = tempdir or tempfile.mkdtemp()

    testers = []

    if enable_cpp:
        testers.append(CPPTester(kwargs))

    if enable_java:
        testers.append(JavaTester(kwargs))

    if enable_js:
        testers.append(JSTester(kwargs))

    if enable_go:
        testers.append(GoTester(kwargs))

    static_json_files = get_static_json_files()
    generated_json_files = datagen.get_generated_json_files(
        tempdir=tempdir,
        flight=run_flight
    )
    json_files = static_json_files + generated_json_files

    runner = IntegrationRunner(json_files, testers, **kwargs)
    failures = []
    failures.extend(runner.run())
    if run_flight:
        failures.extend(runner.run_flight())

    fail_count = 0
    if failures:
        print("################# FAILURES #################")
        for test_case, producer, consumer, exc_info in failures:
            fail_count += 1
            print("FAILED TEST:", end=" ")
            print(test_case.name, producer.name, "producing, ",
                  consumer.name, "consuming")
            if exc_info:
                traceback.print_exception(*exc_info)
            print()

    print(fail_count, "failures")
    if fail_count > 0:
        sys.exit(1)


def write_js_test_json(directory):
    datagen.generate_map_case().write(
        os.path.join(directory, 'map.json')
    )
    datagen.generate_nested_case().write(
        os.path.join(directory, 'nested.json')
    )
    datagen.generate_decimal_case().write(
        os.path.join(directory, 'decimal.json')
    )
    datagen.generate_datetime_case().write(
        os.path.join(directory, 'datetime.json')
    )
    datagen.generate_dictionary_case().write(
        os.path.join(directory, 'dictionary.json')
    )
    datagen.generate_primitive_case([]).write(
        os.path.join(directory, 'primitive_no_batches.json')
    )
    datagen.generate_primitive_case([7, 10]).write(
        os.path.join(directory, 'primitive.json')
    )
    datagen.generate_primitive_case([0, 0, 0]).write(
        os.path.join(directory, 'primitive-empty.json')
    )
