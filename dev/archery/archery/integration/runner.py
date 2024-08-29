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

from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import glob
import gzip
import itertools
import os
import sys
import tempfile
import traceback
from typing import Callable, List, Optional

from . import cdata
from .scenario import Scenario
from .tester import Tester, CDataExporter, CDataImporter
from .tester_cpp import CppTester
from .tester_go import GoTester
from .tester_rust import RustTester
from .tester_java import JavaTester
from .tester_js import JSTester
from .tester_csharp import CSharpTester
from .util import guid, printer
from .util import SKIP_C_ARRAY, SKIP_C_SCHEMA, SKIP_FLIGHT, SKIP_IPC
from ..utils.source import ARROW_ROOT_DEFAULT
from . import datagen


Failure = namedtuple('Failure',
                     ('test_case', 'producer', 'consumer', 'exc_info'))

log = printer.print


class Outcome:
    def __init__(self):
        self.failure = None
        self.skipped = False


class IntegrationRunner(object):

    def __init__(self, json_files,
                 flight_scenarios: List[Scenario],
                 testers: List[Tester], tempdir=None,
                 debug=False, stop_on_error=True, gold_dirs=None,
                 serial=False, match=None, **unused_kwargs):
        self.json_files = json_files
        self.flight_scenarios = flight_scenarios
        self.testers = testers
        self.temp_dir = tempdir or tempfile.mkdtemp()
        self.debug = debug
        self.stop_on_error = stop_on_error
        self.serial = serial
        self.gold_dirs = gold_dirs
        self.failures: List[Outcome] = []
        self.skips: List[Outcome] = []
        self.match = match

        if self.match is not None:
            print("-- Only running tests with {} in their name"
                  .format(self.match))
            self.json_files = [json_file for json_file in self.json_files
                               if self.match in json_file.name]

    def run_ipc(self):
        """
        Run Arrow IPC integration tests for the matrix of enabled
        implementations.
        """
        for producer, consumer in itertools.product(
                filter(lambda t: t.PRODUCER, self.testers),
                filter(lambda t: t.CONSUMER, self.testers)):
            self._compare_ipc_implementations(
                producer, consumer, self._produce_consume,
                self.json_files)
        if self.gold_dirs:
            for gold_dir, consumer in itertools.product(
                    self.gold_dirs,
                    filter(lambda t: t.CONSUMER, self.testers)):
                log('\n')
                log('******************************************************')
                log('Tests against golden files in {}'.format(gold_dir))
                log('******************************************************')

                def run_gold(_, consumer, test_case: datagen.File):
                    return self._run_gold(gold_dir, consumer, test_case)
                self._compare_ipc_implementations(
                    consumer, consumer, run_gold,
                    self._gold_tests(gold_dir))
        log('\n')

    def run_flight(self):
        """
        Run Arrow Flight integration tests for the matrix of enabled
        implementations.
        """
        servers = filter(lambda t: t.FLIGHT_SERVER, self.testers)
        clients = filter(lambda t: (t.FLIGHT_CLIENT and t.CONSUMER),
                         self.testers)
        for server, client in itertools.product(servers, clients):
            self._compare_flight_implementations(server, client)
        log('\n')

    def run_c_data(self):
        """
        Run Arrow C Data interface integration tests for the matrix of
        enabled implementations.
        """
        for producer, consumer in itertools.product(
                filter(lambda t: t.C_DATA_SCHEMA_EXPORTER, self.testers),
                filter(lambda t: t.C_DATA_SCHEMA_IMPORTER, self.testers)):
            self._compare_c_data_implementations(producer, consumer)
        log('\n')

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

            # Find the generated file with the same name as this gold file
            try:
                equiv_json_file = next(f for f in self.json_files
                                       if f.name == name)
            except StopIteration:
                equiv_json_file = None

            skip_testers = set()
            if name == 'union' and prefix == '0.17.1':
                skip_testers.add("Java")
                skip_testers.add("JS")
            if prefix == '1.0.0-bigendian' or prefix == '1.0.0-littleendian':
                skip_testers.add("C#")
                skip_testers.add("Java")
                skip_testers.add("JS")
                skip_testers.add("Rust")
            if prefix == '2.0.0-compression':
                skip_testers.add("JS")

            # See https://github.com/apache/arrow/pull/9822 for how to
            # disable specific compression type tests.

            if prefix == '4.0.0-shareddict':
                skip_testers.add("C#")

            quirks = set()
            if prefix in {'0.14.1', '0.17.1',
                          '1.0.0-bigendian', '1.0.0-littleendian'}:
                # ARROW-13558: older versions generated decimal values that
                # were out of range for the given precision.
                quirks.add("no_decimal_validate")
                quirks.add("no_date64_validate")
                quirks.add("no_times_validate")

            json_file = datagen.File(name, schema=None, batches=None,
                                     path=out_path,
                                     skip_testers=skip_testers,
                                     quirks=quirks)
            if equiv_json_file is not None:
                json_file.add_skips_from(equiv_json_file)
            yield json_file

    def _run_test_cases(self,
                        case_runner: Callable[[datagen.File], Outcome],
                        test_cases: List[datagen.File],
                        *, serial: Optional[bool] = None) -> None:
        """
        Populate self.failures with the outcomes of the
        ``case_runner`` ran against ``test_cases``
        """
        def case_wrapper(test_case):
            if serial:
                return case_runner(test_case)
            with printer.cork():
                return case_runner(test_case)

        if serial is None:
            serial = self.serial

        if self.failures and self.stop_on_error:
            return

        if serial:
            for outcome in map(case_wrapper, test_cases):
                if outcome.failure is not None:
                    self.failures.append(outcome.failure)
                    if self.stop_on_error:
                        break
                elif outcome.skipped:
                    self.skips.append(outcome)

        else:
            with ThreadPoolExecutor() as executor:
                for outcome in executor.map(case_wrapper, test_cases):
                    if outcome.failure is not None:
                        self.failures.append(outcome.failure)
                        if self.stop_on_error:
                            break
                    elif outcome.skipped:
                        self.skips.append(outcome)

    def _compare_ipc_implementations(
        self,
        producer: Tester,
        consumer: Tester,
        run_binaries: Callable[[Tester, Tester, datagen.File], None],
        test_cases: List[datagen.File]
    ):
        """
        Compare Arrow IPC for two implementations (one producer, one consumer).
        """
        log('##########################################################')
        log('IPC: {0} producing, {1} consuming'
            .format(producer.name, consumer.name))
        log('##########################################################')

        case_runner = partial(self._run_ipc_test_case,
                              producer, consumer, run_binaries)
        self._run_test_cases(case_runner, test_cases)

    def _run_ipc_test_case(
        self,
        producer: Tester,
        consumer: Tester,
        run_binaries: Callable[[Tester, Tester, datagen.File], None],
        test_case: datagen.File,
    ) -> Outcome:
        """
        Run one IPC test case.
        """
        outcome = Outcome()

        json_path = test_case.path
        log('=' * 70)
        log('Testing file {0}'.format(json_path))

        if test_case.should_skip(producer.name, SKIP_IPC):
            log(f'-- Skipping test because producer {producer.name} does '
                f'not support IPC')
            outcome.skipped = True

        elif test_case.should_skip(consumer.name, SKIP_IPC):
            log(f'-- Skipping test because consumer {consumer.name} does '
                f'not support IPC')
            outcome.skipped = True

        else:
            try:
                run_binaries(producer, consumer, test_case)
            except Exception:
                traceback.print_exc(file=printer.stdout)
                outcome.failure = Failure(test_case, producer, consumer,
                                          sys.exc_info())

        log('=' * 70)

        return outcome

    def _produce_consume(self,
                         producer: Tester,
                         consumer: Tester,
                         test_case: datagen.File
                         ) -> None:
        """
        Given a producer and a consumer, run different combination of
        tests for the ``test_case``
        * read and write are consistent
        * stream to file is consistent
        """
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

        log('-- Creating binary inputs')
        producer.json_to_file(json_path, producer_file_path)

        # Validate the file
        log('-- Validating file')
        consumer.validate(json_path, producer_file_path)

        log('-- Validating stream')
        producer.file_to_stream(producer_file_path, producer_stream_path)
        consumer.stream_to_file(producer_stream_path, consumer_file_path)
        consumer.validate(json_path, consumer_file_path)

    def _run_gold(self,
                  gold_dir: str,
                  consumer: Tester,
                  test_case: datagen.File) -> None:
        """
        Given a directory with:
        * an ``.arrow_file``
        * a ``.stream``
        associated to the json integration file at ``test_case.path``

        verify that the consumer can read both and agrees with
        what the json file contains; also run ``stream_to_file`` and
        verify that the consumer produces an equivalent file from the
        IPC stream.
        """
        json_path = test_case.path

        # Validate the file
        log('-- Validating file')
        producer_file_path = os.path.join(
            gold_dir, "generated_" + test_case.name + ".arrow_file")
        consumer.validate(json_path, producer_file_path,
                          quirks=test_case.quirks)

        log('-- Validating stream')
        consumer_stream_path = os.path.join(
            gold_dir, "generated_" + test_case.name + ".stream")
        file_id = guid()[:8]
        name = os.path.splitext(os.path.basename(json_path))[0]

        consumer_file_path = os.path.join(self.temp_dir, file_id + '_' +
                                          name + '.consumer_stream_as_file')

        consumer.stream_to_file(consumer_stream_path, consumer_file_path)
        consumer.validate(json_path, consumer_file_path,
                          quirks=test_case.quirks)

    def _compare_flight_implementations(
        self,
        producer: Tester,
        consumer: Tester
    ):
        log('##########################################################')
        log('Flight: {0} serving, {1} requesting'
            .format(producer.name, consumer.name))
        log('##########################################################')

        case_runner = partial(self._run_flight_test_case, producer, consumer)
        self._run_test_cases(
            case_runner, self.json_files + self.flight_scenarios)

    def _run_flight_test_case(self,
                              producer: Tester,
                              consumer: Tester,
                              test_case: datagen.File) -> Outcome:
        """
        Run one Flight test case.
        """
        outcome = Outcome()

        log('=' * 70)
        log('Testing file {0}'.format(test_case.name))

        if test_case.should_skip(producer.name, SKIP_FLIGHT):
            log(f'-- Skipping test because producer {producer.name} does '
                f'not support Flight')
            outcome.skipped = True

        elif test_case.should_skip(consumer.name, SKIP_FLIGHT):
            log(f'-- Skipping test because consumer {consumer.name} does '
                f'not support Flight')
            outcome.skipped = True

        else:
            try:
                if isinstance(test_case, Scenario):
                    server = producer.flight_server(test_case.name)
                    client_args = {'scenario_name': test_case.name}
                else:
                    server = producer.flight_server()
                    client_args = {'json_path': test_case.path}

                with server as port:
                    # Have the client upload the file, then download and
                    # compare
                    consumer.flight_request(port, **client_args)
            except Exception:
                traceback.print_exc(file=printer.stdout)
                outcome.failure = Failure(test_case, producer, consumer,
                                          sys.exc_info())

        log('=' * 70)

        return outcome

    def _compare_c_data_implementations(
        self,
        producer: Tester,
        consumer: Tester
    ):
        log('##########################################################')
        log(f'C Data Interface: '
            f'{producer.name} exporting, {consumer.name} importing')
        log('##########################################################')

        # Serial execution is required for proper memory accounting
        serial = True

        with producer.make_c_data_exporter() as exporter:
            with consumer.make_c_data_importer() as importer:
                case_runner = partial(self._run_c_schema_test_case,
                                      producer, consumer,
                                      exporter, importer)
                self._run_test_cases(case_runner, self.json_files, serial=serial)

                if producer.C_DATA_ARRAY_EXPORTER and consumer.C_DATA_ARRAY_IMPORTER:
                    case_runner = partial(self._run_c_array_test_cases,
                                          producer, consumer,
                                          exporter, importer)
                    self._run_test_cases(case_runner, self.json_files, serial=serial)

    def _run_c_schema_test_case(self,
                                producer: Tester, consumer: Tester,
                                exporter: CDataExporter,
                                importer: CDataImporter,
                                test_case: datagen.File) -> Outcome:
        """
        Run one C ArrowSchema test case.
        """
        outcome = Outcome()

        def do_run():
            json_path = test_case.path
            ffi = cdata.ffi()
            c_schema_ptr = ffi.new("struct ArrowSchema*")
            with cdata.check_memory_released(exporter, importer):
                exporter.export_schema_from_json(json_path, c_schema_ptr)
                importer.import_schema_and_compare_to_json(json_path, c_schema_ptr)

        log('=' * 70)
        log(f'Testing C ArrowSchema from file {test_case.name!r}')

        if test_case.should_skip(producer.name, SKIP_C_SCHEMA):
            log(f'-- Skipping test because producer {producer.name} does '
                f'not support C ArrowSchema')
            outcome.skipped = True

        elif test_case.should_skip(consumer.name, SKIP_C_SCHEMA):
            log(f'-- Skipping test because consumer {consumer.name} does '
                f'not support C ArrowSchema')
            outcome.skipped = True

        else:
            try:
                do_run()
            except Exception:
                traceback.print_exc(file=printer.stdout)
                outcome.failure = Failure(test_case, producer, consumer,
                                          sys.exc_info())

        log('=' * 70)

        return outcome

    def _run_c_array_test_cases(self,
                                producer: Tester, consumer: Tester,
                                exporter: CDataExporter,
                                importer: CDataImporter,
                                test_case: datagen.File) -> Outcome:
        """
        Run one set C ArrowArray test cases.
        """
        outcome = Outcome()

        def do_run():
            json_path = test_case.path
            ffi = cdata.ffi()
            c_array_ptr = ffi.new("struct ArrowArray*")
            for num_batch in range(test_case.num_batches):
                log(f'... with record batch #{num_batch}')
                with cdata.check_memory_released(exporter, importer):
                    exporter.export_batch_from_json(json_path,
                                                    num_batch,
                                                    c_array_ptr)
                    importer.import_batch_and_compare_to_json(json_path,
                                                              num_batch,
                                                              c_array_ptr)

        log('=' * 70)
        log(f'Testing C ArrowArray '
            f'from file {test_case.name!r}')

        if test_case.should_skip(producer.name, SKIP_C_ARRAY):
            log(f'-- Skipping test because producer {producer.name} does '
                f'not support C ArrowArray')
            outcome.skipped = True

        elif test_case.should_skip(consumer.name, SKIP_C_ARRAY):
            log(f'-- Skipping test because consumer {consumer.name} does '
                f'not support C ArrowArray')
            outcome.skipped = True

        else:
            try:
                do_run()
            except Exception:
                traceback.print_exc(file=printer.stdout)
                outcome.failure = Failure(test_case, producer, consumer,
                                          sys.exc_info())

        log('=' * 70)

        return outcome


def get_static_json_files():
    glob_pattern = os.path.join(ARROW_ROOT_DEFAULT,
                                'integration', 'data', '*.json')
    return [
        datagen.File(name=os.path.basename(p), path=p,
                     schema=None, batches=None)
        for p in glob.glob(glob_pattern)
    ]


def run_all_tests(with_cpp=True, with_java=True, with_js=True,
                  with_csharp=True, with_go=True, with_rust=False,
                  run_ipc=False, run_flight=False, run_c_data=False,
                  tempdir=None, **kwargs):
    tempdir = tempdir or tempfile.mkdtemp(prefix='arrow-integration-')

    testers: List[Tester] = []

    if with_cpp:
        testers.append(CppTester(**kwargs))

    if with_java:
        testers.append(JavaTester(**kwargs))

    if with_js:
        testers.append(JSTester(**kwargs))

    if with_csharp:
        testers.append(CSharpTester(**kwargs))

    if with_go:
        testers.append(GoTester(**kwargs))

    if with_rust:
        testers.append(RustTester(**kwargs))

    static_json_files = get_static_json_files()
    generated_json_files = datagen.get_generated_json_files(tempdir=tempdir)
    json_files = static_json_files + generated_json_files

    # Additional integration test cases for Arrow Flight.
    flight_scenarios = [
        Scenario(
            "auth:basic_proto",
            description="Authenticate using the BasicAuth protobuf."),
        Scenario(
            "middleware",
            description="Ensure headers are propagated via middleware.",
        ),
        Scenario(
            "ordered",
            description="Ensure FlightInfo.ordered is supported.",
            skip_testers={"JS", "C#", "Rust"},
        ),
        Scenario(
            "expiration_time:do_get",
            description=("Ensure FlightEndpoint.expiration_time with "
                         "DoGet is working as expected."),
            skip_testers={"JS", "C#", "Rust"},
        ),
        Scenario(
            "expiration_time:list_actions",
            description=("Ensure FlightEndpoint.expiration_time related "
                         "pre-defined actions is working with ListActions "
                         "as expected."),
            skip_testers={"JS", "C#", "Rust"},
        ),
        Scenario(
            "expiration_time:cancel_flight_info",
            description=("Ensure FlightEndpoint.expiration_time and "
                         "CancelFlightInfo are working as expected."),
            skip_testers={"JS", "C#", "Rust"},
        ),
        Scenario(
            "expiration_time:renew_flight_endpoint",
            description=("Ensure FlightEndpoint.expiration_time and "
                         "RenewFlightEndpoint are working as expected."),
            skip_testers={"JS", "C#", "Rust"},
        ),
        Scenario(
            "poll_flight_info",
            description="Ensure PollFlightInfo is supported.",
            skip_testers={"JS", "C#", "Rust"}
        ),
        Scenario(
            "app_metadata_flight_info_endpoint",
            description="Ensure support FlightInfo and Endpoint app_metadata",
            skip_testers={"JS", "C#", "Rust"}
        ),
        Scenario(
            "flight_sql",
            description="Ensure Flight SQL protocol is working as expected.",
            skip_testers={"Rust"}
        ),
        Scenario(
            "flight_sql:extension",
            description="Ensure Flight SQL extensions work as expected.",
            skip_testers={"Rust"}
        ),
    ]

    runner = IntegrationRunner(json_files, flight_scenarios, testers, **kwargs)
    if run_ipc:
        runner.run_ipc()
    if run_flight:
        runner.run_flight()
    if run_c_data:
        runner.run_c_data()

    fail_count = 0
    if runner.failures:
        log("################# FAILURES #################")
        for test_case, producer, consumer, exc_info in runner.failures:
            fail_count += 1
            log("FAILED TEST:", end=" ")
            log(test_case.name, producer.name, "producing, ",
                consumer.name, "consuming")
            if exc_info:
                exc_type, exc_value, exc_tb = exc_info
                log(f'{exc_type}: {exc_value}')
            log()

    log(f"{fail_count} failures, {len(runner.skips)} skips")
    if fail_count > 0:
        sys.exit(1)


def write_js_test_json(directory):
    datagen.generate_primitive_case([], name='primitive_no_batches').write(
        os.path.join(directory, 'primitive-no-batches.json')
    )
    datagen.generate_primitive_case([17, 20], name='primitive').write(
        os.path.join(directory, 'primitive.json')
    )
    datagen.generate_primitive_case([0, 0, 0], name='primitive_zerolength').write(
        os.path.join(directory, 'primitive-empty.json')
    )
    # datagen.generate_primitive_large_offsets_case([17, 20]).write(
    #     os.path.join(directory, 'primitive-large-offsets.json')
    # )
    datagen.generate_null_case([10, 0]).write(
        os.path.join(directory, 'null.json')
    )
    datagen.generate_null_trivial_case([0, 0]).write(
        os.path.join(directory, 'null-trivial.json')
    )
    datagen.generate_decimal128_case().write(
        os.path.join(directory, 'decimal128.json')
    )
    # datagen.generate_decimal256_case().write(
    #     os.path.join(directory, 'decimal256.json')
    # )
    datagen.generate_datetime_case().write(
        os.path.join(directory, 'datetime.json')
    )
    # datagen.generate_duration_case().write(
    #     os.path.join(directory, 'duration.json')
    # )
    # datagen.generate_interval_case().write(
    #     os.path.join(directory, 'interval.json')
    # )
    # datagen.generate_month_day_nano_interval_case().write(
    #     os.path.join(directory, 'month_day_nano_interval.json')
    # )
    datagen.generate_map_case().write(
        os.path.join(directory, 'map.json')
    )
    datagen.generate_non_canonical_map_case().write(
        os.path.join(directory, 'non_canonical_map.json')
    )
    datagen.generate_nested_case().write(
        os.path.join(directory, 'nested.json')
    )
    datagen.generate_recursive_nested_case().write(
        os.path.join(directory, 'recursive-nested.json')
    )
    # datagen.generate_nested_large_offsets_case().write(
    #     os.path.join(directory, 'nested-large-offsets.json')
    # )
    datagen.generate_unions_case().write(
        os.path.join(directory, 'unions.json')
    )
    datagen.generate_custom_metadata_case().write(
        os.path.join(directory, 'custom-metadata.json')
    )
    # datagen.generate_duplicate_fieldnames_case().write(
    #     os.path.join(directory, 'duplicate-fieldnames.json')
    # )
    datagen.generate_dictionary_case().write(
        os.path.join(directory, 'dictionary.json')
    )
    datagen.generate_dictionary_unsigned_case().write(
        os.path.join(directory, 'dictionary-unsigned.json')
    )
    datagen.generate_nested_dictionary_case().write(
        os.path.join(directory, 'dictionary-nested.json')
    )
    # datagen.generate_run_end_encoded_case().write(
    #     os.path.join(directory, 'run_end_encoded.json')
    # )
    datagen.generate_extension_case().write(
        os.path.join(directory, 'extension.json')
    )
