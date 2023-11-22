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


# FIXME(sbinet): revisit for Go modules
_HOME = os.getenv("HOME", "~")
_GOPATH = os.getenv("GOPATH", os.path.join(_HOME, "go"))
_GOBIN = os.environ.get("GOBIN", os.path.join(_GOPATH, "bin"))

_GO_INTEGRATION_EXE = os.path.join(_GOBIN, "arrow-json-integration-test")
_STREAM_TO_FILE = os.path.join(_GOBIN, "arrow-stream-to-file")
_FILE_TO_STREAM = os.path.join(_GOBIN, "arrow-file-to-stream")

_FLIGHT_SERVER_CMD = [os.path.join(_GOBIN, "arrow-flight-integration-server")]
_FLIGHT_CLIENT_CMD = [
    os.path.join(_GOBIN, "arrow-flight-integration-client"),
    "-host",
    "localhost",
]

_DLL_PATH = os.path.join(
    ARROW_ROOT_DEFAULT,
    "go/arrow/internal/cdata_integration")
_INTEGRATION_DLL = os.path.join(_DLL_PATH, "arrow_go_integration" + cdata.dll_suffix)


class GoTester(Tester):
    PRODUCER = True
    CONSUMER = True
    FLIGHT_SERVER = True
    FLIGHT_CLIENT = True
    C_DATA_SCHEMA_EXPORTER = True
    C_DATA_ARRAY_EXPORTER = True
    C_DATA_SCHEMA_IMPORTER = True
    C_DATA_ARRAY_IMPORTER = True

    name = 'Go'

    def _run(self, arrow_path=None, json_path=None, command='VALIDATE'):
        cmd = [_GO_INTEGRATION_EXE]

        if arrow_path is not None:
            cmd.extend(['-arrow', arrow_path])

        if json_path is not None:
            cmd.extend(['-json', json_path])

        cmd.extend(['-mode', command])

        if self.debug:
            log(' '.join(cmd))

        run_cmd(cmd)

    def validate(self, json_path, arrow_path, quirks=None):
        return self._run(arrow_path, json_path, 'VALIDATE')

    def json_to_file(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, 'JSON_TO_ARROW')

    def stream_to_file(self, stream_path, file_path):
        cmd = [_STREAM_TO_FILE, '<', stream_path, '>', file_path]
        self.run_shell_command(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = [_FILE_TO_STREAM, file_path, '>', stream_path]
        self.run_shell_command(cmd)

    @contextlib.contextmanager
    def flight_server(self, scenario_name=None):
        cmd = _FLIGHT_SERVER_CMD + ['-port=0']
        if scenario_name:
            cmd = cmd + ['-scenario', scenario_name]
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
            server.kill()
            server.wait(5)

    def flight_request(self, port, json_path=None, scenario_name=None):
        cmd = _FLIGHT_CLIENT_CMD + [
            '-port=' + str(port),
        ]
        if json_path:
            cmd.extend(('-path', json_path))
        elif scenario_name:
            cmd.extend(('-scenario', scenario_name))
        else:
            raise TypeError('Must provide one of json_path or scenario_name')

        if self.debug:
            log(' '.join(cmd))
        run_cmd(cmd)

    def make_c_data_exporter(self):
        return GoCDataExporter(self.debug, self.args)

    def make_c_data_importer(self):
        return GoCDataImporter(self.debug, self.args)


_go_c_data_entrypoints = """
    const char* ArrowGo_ExportSchemaFromJson(const char* json_path,
                                             uintptr_t out);
    const char* ArrowGo_ImportSchemaAndCompareToJson(
        const char* json_path, uintptr_t c_schema);

    const char* ArrowGo_ExportBatchFromJson(const char* json_path,
                                            int num_batch,
                                            uintptr_t out);
    const char* ArrowGo_ImportBatchAndCompareToJson(
        const char* json_path, int num_batch, uintptr_t c_array);

    int64_t ArrowGo_BytesAllocated();
    void ArrowGo_RunGC();
    void ArrowGo_FreeError(const char*);
    """


@functools.lru_cache
def _load_ffi(ffi, lib_path=_INTEGRATION_DLL):
    # NOTE that setting Go environment variables here (such as GODEBUG)
    # would be ignored by the Go runtime. The environment variables need
    # to be set from the process calling Archery.
    ffi.cdef(_go_c_data_entrypoints)
    dll = ffi.dlopen(lib_path)
    return dll


class _CDataBase:

    def __init__(self, debug, args):
        self.debug = debug
        self.args = args
        self.ffi = cdata.ffi()
        self.dll = _load_ffi(self.ffi)

    def _pointer_to_int(self, c_ptr):
        return self.ffi.cast('uintptr_t', c_ptr)

    def _check_go_error(self, go_error):
        """
        Check a `const char*` error return from an integration entrypoint.

        A null means success, a non-empty string is an error message.
        The string is dynamically allocated on the Go side.
        """
        assert self.ffi.typeof(go_error) is self.ffi.typeof("const char*")
        if go_error != self.ffi.NULL:
            try:
                error = self.ffi.string(go_error).decode('utf8',
                                                         errors='replace')
                raise RuntimeError(
                    f"Go C Data Integration call failed: {error}")
            finally:
                self.dll.ArrowGo_FreeError(go_error)


class GoCDataExporter(CDataExporter, _CDataBase):
    # Note: the Arrow Go C Data export functions expect their output
    # ArrowStream or ArrowArray argument to be zero-initialized.
    # This is currently ensured through the use of `ffi.new`.

    def export_schema_from_json(self, json_path, c_schema_ptr):
        go_error = self.dll.ArrowGo_ExportSchemaFromJson(
            str(json_path).encode(), self._pointer_to_int(c_schema_ptr))
        self._check_go_error(go_error)

    def export_batch_from_json(self, json_path, num_batch, c_array_ptr):
        go_error = self.dll.ArrowGo_ExportBatchFromJson(
            str(json_path).encode(), num_batch,
            self._pointer_to_int(c_array_ptr))
        self._check_go_error(go_error)

    @property
    def supports_releasing_memory(self):
        return True

    def record_allocation_state(self):
        return self.dll.ArrowGo_BytesAllocated()

    # Note: no need to call the Go GC anywhere thanks to Arrow Go's
    # explicit refcounting.


class GoCDataImporter(CDataImporter, _CDataBase):

    def import_schema_and_compare_to_json(self, json_path, c_schema_ptr):
        go_error = self.dll.ArrowGo_ImportSchemaAndCompareToJson(
            str(json_path).encode(), self._pointer_to_int(c_schema_ptr))
        self._check_go_error(go_error)

    def import_batch_and_compare_to_json(self, json_path, num_batch,
                                         c_array_ptr):
        go_error = self.dll.ArrowGo_ImportBatchAndCompareToJson(
            str(json_path).encode(), num_batch,
            self._pointer_to_int(c_array_ptr))
        self._check_go_error(go_error)

    @property
    def supports_releasing_memory(self):
        return True
