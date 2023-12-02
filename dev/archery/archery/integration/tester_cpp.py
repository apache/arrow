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
    "ARROW_CPP_EXE_PATH", os.path.join(ARROW_ROOT_DEFAULT, "cpp/build/debug")
)
_INTEGRATION_EXE = os.path.join(_EXE_PATH, "arrow-json-integration-test")
_STREAM_TO_FILE = os.path.join(_EXE_PATH, "arrow-stream-to-file")
_FILE_TO_STREAM = os.path.join(_EXE_PATH, "arrow-file-to-stream")

_FLIGHT_SERVER_CMD = [os.path.join(
    _EXE_PATH, "flight-test-integration-server")]
_FLIGHT_CLIENT_CMD = [
    os.path.join(_EXE_PATH, "flight-test-integration-client"),
    "-host",
    "localhost",
]

_DLL_PATH = _EXE_PATH
_ARROW_DLL = os.path.join(_DLL_PATH, "libarrow" + cdata.dll_suffix)


class CppTester(Tester):
    PRODUCER = True
    CONSUMER = True
    FLIGHT_SERVER = True
    FLIGHT_CLIENT = True
    C_DATA_SCHEMA_EXPORTER = True
    C_DATA_ARRAY_EXPORTER = True
    C_DATA_SCHEMA_IMPORTER = True
    C_DATA_ARRAY_IMPORTER = True

    name = 'C++'

    def _run(
        self,
        arrow_path=None,
        json_path=None,
        command='VALIDATE',
        quirks=None
    ):
        cmd = [_INTEGRATION_EXE, '--integration']

        if arrow_path is not None:
            cmd.append('--arrow=' + arrow_path)

        if json_path is not None:
            cmd.append('--json=' + json_path)

        cmd.append('--mode=' + command)

        if quirks:
            if "no_decimal_validate" in quirks:
                cmd.append("--validate_decimals=false")
            if "no_date64_validate" in quirks:
                cmd.append("--validate_date64=false")
            if "no_times_validate" in quirks:
                cmd.append("--validate_times=false")

        if self.debug:
            log(' '.join(cmd))

        run_cmd(cmd)

    def validate(self, json_path, arrow_path, quirks=None):
        return self._run(arrow_path, json_path, 'VALIDATE', quirks=quirks)

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
            cmd = cmd + ["-scenario", scenario_name]
        if self.debug:
            log(" ".join(cmd))
        server = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            output = server.stdout.readline().decode()
            if not output.startswith("Server listening on localhost:"):
                server.kill()
                out, err = server.communicate()
                raise RuntimeError(
                    "Flight-C++ server did not start properly, "
                    "stdout:\n{}\n\nstderr:\n{}\n".format(
                        output + out.decode(), err.decode()
                    )
                )
            port = int(output.split(":")[1])
            yield port
        finally:
            server.kill()
            server.wait(5)

    def flight_request(self, port, json_path=None, scenario_name=None):
        cmd = _FLIGHT_CLIENT_CMD + [f'-port={port}']
        if json_path:
            cmd.extend(('-path', json_path))
        elif scenario_name:
            cmd.extend(('-scenario', scenario_name))
        else:
            raise TypeError("Must provide one of json_path or scenario_name")

        if self.debug:
            log(' '.join(cmd))
        run_cmd(cmd)

    def make_c_data_exporter(self):
        return CppCDataExporter(self.debug, self.args)

    def make_c_data_importer(self):
        return CppCDataImporter(self.debug, self.args)


_cpp_c_data_entrypoints = """
    const char* ArrowCpp_CDataIntegration_ExportSchemaFromJson(
        const char* json_path, struct ArrowSchema* out);
    const char* ArrowCpp_CDataIntegration_ImportSchemaAndCompareToJson(
        const char* json_path, struct ArrowSchema* schema);

    const char* ArrowCpp_CDataIntegration_ExportBatchFromJson(
        const char* json_path, int num_batch, struct ArrowArray* out);
    const char* ArrowCpp_CDataIntegration_ImportBatchAndCompareToJson(
        const char* json_path, int num_batch, struct ArrowArray* batch);

    int64_t ArrowCpp_BytesAllocated();
    """


@functools.lru_cache
def _load_ffi(ffi, lib_path=_ARROW_DLL):
    os.environ['ARROW_DEBUG_MEMORY_POOL'] = 'trap'
    ffi.cdef(_cpp_c_data_entrypoints)
    dll = ffi.dlopen(lib_path)
    dll.ArrowCpp_CDataIntegration_ExportSchemaFromJson
    return dll


class _CDataBase:

    def __init__(self, debug, args):
        self.debug = debug
        self.args = args
        self.ffi = cdata.ffi()
        self.dll = _load_ffi(self.ffi)

    def _check_c_error(self, c_error):
        """
        Check a `const char*` error return from an integration entrypoint.

        A null means success, a non-empty string is an error message.
        The string is statically allocated on the C++ side.
        """
        assert self.ffi.typeof(c_error) is self.ffi.typeof("const char*")
        if c_error != self.ffi.NULL:
            error = self.ffi.string(c_error).decode('utf8',
                                                    errors='replace')
            raise RuntimeError(
                f"C++ C Data Integration call failed: {error}")


class CppCDataExporter(CDataExporter, _CDataBase):

    def export_schema_from_json(self, json_path, c_schema_ptr):
        c_error = self.dll.ArrowCpp_CDataIntegration_ExportSchemaFromJson(
            str(json_path).encode(), c_schema_ptr)
        self._check_c_error(c_error)

    def export_batch_from_json(self, json_path, num_batch, c_array_ptr):
        c_error = self.dll.ArrowCpp_CDataIntegration_ExportBatchFromJson(
            str(json_path).encode(), num_batch, c_array_ptr)
        self._check_c_error(c_error)

    @property
    def supports_releasing_memory(self):
        return True

    def record_allocation_state(self):
        return self.dll.ArrowCpp_BytesAllocated()


class CppCDataImporter(CDataImporter, _CDataBase):

    def import_schema_and_compare_to_json(self, json_path, c_schema_ptr):
        c_error = self.dll.ArrowCpp_CDataIntegration_ImportSchemaAndCompareToJson(
            str(json_path).encode(), c_schema_ptr)
        self._check_c_error(c_error)

    def import_batch_and_compare_to_json(self, json_path, num_batch,
                                         c_array_ptr):
        c_error = self.dll.ArrowCpp_CDataIntegration_ImportBatchAndCompareToJson(
            str(json_path).encode(), num_batch, c_array_ptr)
        self._check_c_error(c_error)

    @property
    def supports_releasing_memory(self):
        return True
