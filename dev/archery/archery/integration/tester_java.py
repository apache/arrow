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
import functools
import os
import subprocess

from . import cdata
from .tester import Tester, CDataExporter, CDataImporter
from .util import run_cmd, log
from ..utils.source import ARROW_ROOT_DEFAULT


def load_version_from_pom():
    import xml.etree.ElementTree as ET
    tree = ET.parse(os.path.join(ARROW_ROOT_DEFAULT, 'java', 'pom.xml'))
    tag_pattern = '{http://maven.apache.org/POM/4.0.0}version'
    version_tag = list(tree.getroot().findall(tag_pattern))[0]
    return version_tag.text


# NOTE: we don't add "-Darrow.memory.debug.allocator=true" here as it adds a
# couple minutes to total CPU usage of the integration test suite
# (see setup_jpype() below).
_JAVA_OPTS = [
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Darrow.struct.conflict.policy=CONFLICT_APPEND",
    # GH-39113: avoid failures accessing files in `/tmp/hsperfdata_...`
    "-XX:-UsePerfData",
]

_arrow_version = load_version_from_pom()
_ARROW_TOOLS_JAR = os.environ.get(
    "ARROW_JAVA_INTEGRATION_JAR",
    os.path.join(
        ARROW_ROOT_DEFAULT,
        "java/tools/target",
        f"arrow-tools-{_arrow_version}-jar-with-dependencies.jar"
    )
)
_ARROW_C_DATA_JAR = os.environ.get(
    "ARROW_C_DATA_JAVA_INTEGRATION_JAR",
    os.path.join(
        ARROW_ROOT_DEFAULT,
        "java/c/target",
        f"arrow-c-data-{_arrow_version}.jar"
    )
)
_ARROW_FLIGHT_JAR = os.environ.get(
    "ARROW_FLIGHT_JAVA_INTEGRATION_JAR",
    os.path.join(
        ARROW_ROOT_DEFAULT,
        "java/flight/flight-integration-tests/target",
        f"flight-integration-tests-{_arrow_version}-jar-with-dependencies.jar"
    )
)
_ARROW_FLIGHT_SERVER = (
    "org.apache.arrow.flight.integration.tests.IntegrationTestServer"
)
_ARROW_FLIGHT_CLIENT = (
    "org.apache.arrow.flight.integration.tests.IntegrationTestClient"
)


@functools.lru_cache
def setup_jpype():
    import jpype
    jar_path = f"{_ARROW_TOOLS_JAR}:{_ARROW_C_DATA_JAR}"
    # XXX Didn't manage to tone down the logging level here (DEBUG -> INFO)
    java_opts = _JAVA_OPTS[:]
    proc = subprocess.run(
        ['java', '--add-opens'],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True)
    if 'Unrecognized option: --add-opens' not in proc.stderr:
        # Java 9+
        java_opts.append(
            '--add-opens=java.base/java.nio='
            'org.apache.arrow.memory.core,ALL-UNNAMED')
    jpype.startJVM(jpype.getDefaultJVMPath(),
                   "-Djava.class.path=" + jar_path,
                   # This flag is too heavy for IPC and Flight tests
                   "-Darrow.memory.debug.allocator=true",
                   # Reduce internal use of signals by the JVM
                   "-Xrs",
                   *java_opts)


class _CDataBase:

    def __init__(self, debug, args):
        import jpype
        self.debug = debug
        self.args = args
        self.ffi = cdata.ffi()
        setup_jpype()
        # JPype pointers to java.io, org.apache.arrow...
        self.java_io = jpype.JPackage("java").io
        self.java_arrow = jpype.JPackage("org").apache.arrow
        self.java_allocator = self._make_java_allocator()

    def _pointer_to_int(self, c_ptr):
        return int(self.ffi.cast('uintptr_t', c_ptr))

    def _wrap_c_schema_ptr(self, c_schema_ptr):
        return self.java_arrow.c.ArrowSchema.wrap(
            self._pointer_to_int(c_schema_ptr))

    def _wrap_c_array_ptr(self, c_array_ptr):
        return self.java_arrow.c.ArrowArray.wrap(
            self._pointer_to_int(c_array_ptr))

    def _make_java_allocator(self):
        # Return a new allocator
        return self.java_arrow.memory.RootAllocator()

    def _assert_schemas_equal(self, expected, actual):
        # XXX This is fragile for dictionaries, as Schema.equals compares
        # dictionary ids.
        self.java_arrow.vector.util.Validator.compareSchemas(
            expected, actual)

    def _assert_batches_equal(self, expected, actual):
        self.java_arrow.vector.util.Validator.compareVectorSchemaRoot(
            expected, actual)

    def _assert_dict_providers_equal(self, expected, actual):
        self.java_arrow.vector.util.Validator.compareDictionaryProviders(
            expected, actual)

    # Note: no need to call the Java GC anywhere thanks to AutoCloseable


class JavaCDataExporter(CDataExporter, _CDataBase):

    def export_schema_from_json(self, json_path, c_schema_ptr):
        json_file = self.java_io.File(json_path)
        with self.java_arrow.vector.ipc.JsonFileReader(
                json_file, self.java_allocator) as json_reader:
            schema = json_reader.start()
            dict_provider = json_reader
            self.java_arrow.c.Data.exportSchema(
                self.java_allocator, schema, dict_provider,
                self._wrap_c_schema_ptr(c_schema_ptr)
            )

    def export_batch_from_json(self, json_path, num_batch, c_array_ptr):
        json_file = self.java_io.File(json_path)
        with self.java_arrow.vector.ipc.JsonFileReader(
                json_file, self.java_allocator) as json_reader:
            json_reader.start()
            if num_batch > 0:
                actually_skipped = json_reader.skip(num_batch)
                assert actually_skipped == num_batch
            with json_reader.read() as batch:
                dict_provider = json_reader
                self.java_arrow.c.Data.exportVectorSchemaRoot(
                    self.java_allocator, batch, dict_provider,
                    self._wrap_c_array_ptr(c_array_ptr))

    @property
    def supports_releasing_memory(self):
        return True

    def record_allocation_state(self):
        return self.java_allocator.getAllocatedMemory()

    def close(self):
        self.java_allocator.close()


class JavaCDataImporter(CDataImporter, _CDataBase):

    def import_schema_and_compare_to_json(self, json_path, c_schema_ptr):
        json_file = self.java_io.File(json_path)
        with self.java_arrow.vector.ipc.JsonFileReader(
                json_file, self.java_allocator) as json_reader:
            json_schema = json_reader.start()
            with self.java_arrow.c.CDataDictionaryProvider() as dict_provider:
                imported_schema = self.java_arrow.c.Data.importSchema(
                    self.java_allocator,
                    self._wrap_c_schema_ptr(c_schema_ptr),
                    dict_provider)
                self._assert_schemas_equal(json_schema, imported_schema)

    def import_batch_and_compare_to_json(self, json_path, num_batch,
                                         c_array_ptr):
        json_file = self.java_io.File(json_path)
        with self.java_arrow.vector.ipc.JsonFileReader(
                json_file, self.java_allocator) as json_reader:
            schema = json_reader.start()
            if num_batch > 0:
                actually_skipped = json_reader.skip(num_batch)
                assert actually_skipped == num_batch
            with json_reader.read() as batch:
                with self.java_arrow.vector.VectorSchemaRoot.create(
                        schema, self.java_allocator) as imported_batch:
                    # We need to pass a dict provider primed with dictionary ids
                    # matching those in the schema, hence an empty
                    # CDataDictionaryProvider would not work here.
                    dict_provider = (self.java_arrow.vector.dictionary
                                     .DictionaryProvider.MapDictionaryProvider())
                    dict_provider.copyStructureFrom(json_reader, self.java_allocator)
                    with dict_provider:
                        self.java_arrow.c.Data.importIntoVectorSchemaRoot(
                            self.java_allocator,
                            self._wrap_c_array_ptr(c_array_ptr),
                            imported_batch, dict_provider)
                        self._assert_batches_equal(batch, imported_batch)
                        self._assert_dict_providers_equal(json_reader, dict_provider)

    @property
    def supports_releasing_memory(self):
        return True

    def close(self):
        self.java_allocator.close()


class JavaTester(Tester):
    PRODUCER = True
    CONSUMER = True
    FLIGHT_SERVER = True
    FLIGHT_CLIENT = True
    C_DATA_SCHEMA_EXPORTER = True
    C_DATA_SCHEMA_IMPORTER = True
    C_DATA_ARRAY_EXPORTER = True
    C_DATA_ARRAY_IMPORTER = True

    name = 'Java'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Detect whether we're on Java 8 or Java 9+
        self._java_opts = _JAVA_OPTS[:]
        proc = subprocess.run(
            ['java', '--add-opens'],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True)
        if 'Unrecognized option: --add-opens' not in proc.stderr:
            # Java 9+
            self._java_opts.append(
                '--add-opens=java.base/java.nio='
                'org.apache.arrow.memory.core,ALL-UNNAMED')
            self._java_opts.append(
                '--add-reads=org.apache.arrow.flight.core=ALL-UNNAMED')

    def _run(self, arrow_path=None, json_path=None, command='VALIDATE'):
        cmd = (
            ['java'] +
            self._java_opts +
            ['-cp', _ARROW_TOOLS_JAR, 'org.apache.arrow.tools.Integration']
        )

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
        cmd = (
            ['java'] + self._java_opts + [
                '-cp',
                _ARROW_TOOLS_JAR,
                'org.apache.arrow.tools.StreamToFile',
                stream_path,
                file_path,
            ]
        )
        if self.debug:
            log(' '.join(cmd))
        run_cmd(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = (
            ['java'] + self._java_opts + [
                '-cp',
                _ARROW_TOOLS_JAR,
                'org.apache.arrow.tools.FileToStream',
                file_path,
                stream_path,
            ]
        )
        if self.debug:
            log(' '.join(cmd))
        run_cmd(cmd)

    def flight_request(self, port, json_path=None, scenario_name=None):
        cmd = (
            ['java'] + self._java_opts + [
                '-cp', _ARROW_FLIGHT_JAR, _ARROW_FLIGHT_CLIENT, '-port', str(
                    port)
            ])

        if json_path:
            cmd.extend(('-j', json_path))
        elif scenario_name:
            cmd.extend(('-scenario', scenario_name))
        else:
            raise TypeError('Must provide one of json_path or scenario_name')

        if self.debug:
            log(' '.join(cmd))
        run_cmd(cmd)

    @contextlib.contextmanager
    def flight_server(self, scenario_name=None):
        cmd = (
            ['java'] +
            self._java_opts +
            ['-cp', _ARROW_FLIGHT_JAR, _ARROW_FLIGHT_SERVER, '-port', '0']
        )
        if scenario_name:
            cmd.extend(('-scenario', scenario_name))
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
                    'Flight-Java server did not start properly, '
                    'stdout:\n{}\n\nstderr:\n{}\n'.format(
                        output + out.decode(), err.decode()
                    )
                )
            port = int(output.split(':')[1])
            yield port
        finally:
            server.kill()
            server.wait(5)

    def make_c_data_exporter(self):
        return JavaCDataExporter(self.debug, self.args)

    def make_c_data_importer(self):
        return JavaCDataImporter(self.debug, self.args)
