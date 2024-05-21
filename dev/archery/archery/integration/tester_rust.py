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


_EXE_PATH = os.environ.get(
    "ARROW_RUST_EXE_PATH", os.path.join(ARROW_ROOT_DEFAULT, "rust/target/debug")
)
_INTEGRATION_EXE = os.path.join(_EXE_PATH, "arrow-json-integration-test")
_STREAM_TO_FILE = os.path.join(_EXE_PATH, "arrow-stream-to-file")
_FILE_TO_STREAM = os.path.join(_EXE_PATH, "arrow-file-to-stream")

_FLIGHT_SERVER_CMD = [os.path.join(
    _EXE_PATH, "flight-test-integration-server")]
_FLIGHT_CLIENT_CMD = [
    os.path.join(_EXE_PATH, "flight-test-integration-client"),
    "--host",
    "localhost",
]

_INTEGRATION_DLL = os.path.join(_EXE_PATH,
                                "libarrow_integration_testing" + cdata.dll_suffix)


class RustTester(Tester):
    PRODUCER = True
    CONSUMER = True
    FLIGHT_SERVER = True
    FLIGHT_CLIENT = True
    C_DATA_SCHEMA_EXPORTER = True
    C_DATA_ARRAY_EXPORTER = True
    C_DATA_SCHEMA_IMPORTER = True
    C_DATA_ARRAY_IMPORTER = True

    name = 'Rust'

    def _run(self, arrow_path=None, json_path=None, command='VALIDATE'):
        cmd = [_INTEGRATION_EXE, '--integration']

        if arrow_path is not None:
            cmd.append('--arrow=' + arrow_path)

        if json_path is not None:
            cmd.append('--json=' + json_path)

        cmd.append('--mode=' + command)

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
        cmd = _FLIGHT_SERVER_CMD + ['--port=0']
        if scenario_name:
            cmd = cmd + ['--scenario', scenario_name]
        if self.debug:
            log(' '.join(cmd))
        server = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            output = server.stdout.readline().decode()
            if not output.startswith('Server listening on localhost:'):
                server.kill()
                out, err = server.communicate()
                raise RuntimeError(
                    'Flight-Rust server did not start properly, '
                    'stdout:\n{}\n\nstderr:\n{}\n'.format(
                        output + out.decode(), err.decode()
                    )
                )
            port = int(output.split(':')[1])
            yield port
        finally:
            server.kill()
            server.wait(5)

    def flight_request(self, port, json_path=None, scenario_name=None):
        cmd = _FLIGHT_CLIENT_CMD + [f'--port={port}']
        if json_path:
            cmd.extend(('--path', json_path))
        elif scenario_name:
            cmd.extend(('--scenario', scenario_name))
        else:
            raise TypeError('Must provide one of json_path or scenario_name')

        if self.debug:
            log(' '.join(cmd))
        run_cmd(cmd)

    def make_c_data_exporter(self):
        return RustCDataExporter(self.debug, self.args)

    def make_c_data_importer(self):
        return RustCDataImporter(self.debug, self.args)


_rust_c_data_entrypoints = """
    const char* arrow_rs_cdata_integration_export_schema_from_json(
        const char* json_path, uintptr_t out);
    const char* arrow_rs_cdata_integration_import_schema_and_compare_to_json(
        const char* json_path, uintptr_t c_schema);

    const char* arrow_rs_cdata_integration_export_batch_from_json(
        const char* json_path, int num_batch, uintptr_t out);
    const char* arrow_rs_cdata_integration_import_batch_and_compare_to_json(
        const char* json_path, int num_batch, uintptr_t c_array);

    void arrow_rs_free_error(const char*);
    """


@functools.lru_cache
def _load_ffi(ffi, lib_path=_INTEGRATION_DLL):
    ffi.cdef(_rust_c_data_entrypoints)
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

    def _check_rust_error(self, rs_error):
        """
        Check a `const char*` error return from an integration entrypoint.

        A null means success, a non-empty string is an error message.
        The string is dynamically allocated on the Rust side.
        """
        assert self.ffi.typeof(rs_error) is self.ffi.typeof("const char*")
        if rs_error != self.ffi.NULL:
            try:
                error = self.ffi.string(rs_error).decode(
                    'utf8', errors='replace')
                raise RuntimeError(
                    f"Rust C Data Integration call failed: {error}")
            finally:
                self.dll.arrow_rs_free_error(rs_error)


class RustCDataExporter(CDataExporter, _CDataBase):

    def export_schema_from_json(self, json_path, c_schema_ptr):
        rs_error = self.dll.arrow_rs_cdata_integration_export_schema_from_json(
            str(json_path).encode(), self._pointer_to_int(c_schema_ptr))
        self._check_rust_error(rs_error)

    def export_batch_from_json(self, json_path, num_batch, c_array_ptr):
        rs_error = self.dll.arrow_rs_cdata_integration_export_batch_from_json(
            str(json_path).encode(), num_batch,
            self._pointer_to_int(c_array_ptr))
        self._check_rust_error(rs_error)

    @property
    def supports_releasing_memory(self):
        return True

    def record_allocation_state(self):
        # FIXME we should track the amount of Rust-allocated memory (GH-38822)
        return 0


class RustCDataImporter(CDataImporter, _CDataBase):

    def import_schema_and_compare_to_json(self, json_path, c_schema_ptr):
        rs_error = \
            self.dll.arrow_rs_cdata_integration_import_schema_and_compare_to_json(
                str(json_path).encode(), self._pointer_to_int(c_schema_ptr))
        self._check_rust_error(rs_error)

    def import_batch_and_compare_to_json(self, json_path, num_batch,
                                         c_array_ptr):
        rs_error = \
            self.dll.arrow_rs_cdata_integration_import_batch_and_compare_to_json(
                str(json_path).encode(), num_batch, self._pointer_to_int(c_array_ptr))
        self._check_rust_error(rs_error)

    @property
    def supports_releasing_memory(self):
        return True
