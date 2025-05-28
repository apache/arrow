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

from pathlib import Path
from typing import Final
from . import cdata
from .tester import Tester, CDataExporter, CDataImporter
from .util import run_cmd, log
import functools

_EXTERNAL_LIBRARY_C_DATA_EXPORT_SCHEMA_FROM_JSON_ENTRYPOINT = " const char* external_CDataIntegration_ExportSchemaFromJson(const char* json_path, struct ArrowSchema* out);"
_EXTERNAL_LIBRARY_C_DATA_IMPORT_SCHEMA_AND_COMPARE_TO_JSON_ENTRYPOINT = "const char* external_CDataIntegration_ImportSchemaAndCompareToJson(const char* json_path, struct ArrowSchema* schema);"

_EXTERNAL_LIBRARY_C_DATA_EXPORT_BATCH_FROM_JSON_ENTRYPOINT = "const char* external_CDataIntegration_ExportBatchFromJson(const char* json_path, int num_batch, struct ArrowArray* out);"
_EXTERNAL_LIBRARY_C_DATA_IMPORT_BATCH_AND_COMPARE_TO_JSON_ENTRYPOINT = "const char* external_CDataIntegration_ImportBatchAndCompareToJson(const char* json_path, int num_batch, struct ArrowArray* batch);"

_EXTERNAL_BYTES_ALLOCATED_ENTRYPOINT = "int64_t external_BytesAllocated(void);"


class ExternalLibraryTester(Tester):
    """
    A tester for external Arrow libraries that implements integration testing capabilities.
    This class provides a framework for testing external Arrow libraries by running
    integration tests through external executables. It supports various Arrow features
    including producers, consumers, C Data interface exporters/importers, and stream/file
    operations.
    Attributes:
        PRODUCER (bool): Whether the library supports producing Arrow data.
        CONSUMER (bool): Whether the library supports consuming Arrow data.
        FLIGHT_SERVER (bool): Whether the library supports Flight server functionality.
        FLIGHT_CLIENT (bool): Whether the library supports Flight client functionality.
        C_DATA_SCHEMA_EXPORTER (bool): Whether the library supports C Data schema export.
        C_DATA_ARRAY_EXPORTER (bool): Whether the library supports C Data array export.
        C_DATA_SCHEMA_IMPORTER (bool): Whether the library supports C Data schema import.
        C_DATA_ARRAY_IMPORTER (bool): Whether the library supports C Data array import.
        name (str): The name identifier for this tester type.
    Args:
        path (Path): Path to the directory containing the external library and executables.
        is_producer_compatible (bool): Whether the library can produce Arrow data.
        is_consumer_compatible (bool): Whether the library can consume Arrow data.
        is_c_data_schema_exporter_compatible (bool): C Data schema export compatibility.
        is_c_data_array_exporter_compatible (bool): C Data array export compatibility.
        is_c_data_schema_importer_compatible (bool): C Data schema import compatibility.
        is_c_data_array_importer_compatible (bool): C Data array import compatibility.
        supports_releasing_memory (bool): Whether the library supports memory release operations.
        **args: Additional arguments passed to the parent Tester class.
    The class manages external executables for:
    - 'arrow-json-integration-test': Main integration testing executable
    - 'arrow-stream-to-file': Converts Arrow streams to file format
    - 'arrow-file-to-stream': Converts Arrow files to stream format
    - 'c_data_integration.[dll/so]' library: C Data interface operations
    Executable and library must strictly follow the expected naming conventions
    """

    PRODUCER = True
    CONSUMER = True
    FLIGHT_SERVER = False
    FLIGHT_CLIENT = False
    C_DATA_SCHEMA_EXPORTER = True
    C_DATA_ARRAY_EXPORTER = True
    C_DATA_SCHEMA_IMPORTER = True
    C_DATA_ARRAY_IMPORTER = True

    _EXE_PATH: Path
    _INTEGRATION_EXE: Path
    _STREAM_TO_FILE: Path
    _FILE_TO_STREAM: Path
    _INTEGRATION_DLL: Path

    _supports_releasing_memory: bool = True

    _entrypoints: str

    name = "external_library"

    def __init__(
        self,
        path: Path,
        is_producer_compatible: bool,
        is_consumer_compatible: bool,
        is_c_data_schema_exporter_compatible: bool,
        is_c_data_array_exporter_compatible: bool,
        is_c_data_schema_importer_compatible: bool,
        is_c_data_array_importer_compatible: bool,
        supports_releasing_memory: bool,
        **args,
    ):
        super().__init__(**args)
        self.PRODUCER = is_producer_compatible
        self.CONSUMER = is_consumer_compatible
        self.C_DATA_SCHEMA_EXPORTER = is_c_data_schema_exporter_compatible
        self.C_DATA_ARRAY_EXPORTER = is_c_data_array_exporter_compatible
        self.C_DATA_SCHEMA_IMPORTER = is_c_data_schema_importer_compatible
        self.C_DATA_ARRAY_IMPORTER = is_c_data_array_importer_compatible
        self._supports_releasing_memory = supports_releasing_memory
        self._EXE_PATH = path
        self._INTEGRATION_EXE = path / "arrow-json-integration-test"
        self._STREAM_TO_FILE = path / "arrow-stream-to-file"
        self._FILE_TO_STREAM = path / "arrow-file-to-stream"
        self._INTEGRATION_DLL = self._EXE_PATH / (
            "c_data_integration" + cdata.dll_suffix
        )
        self._entrypoints = get_entrypoints(
            is_c_data_schema_exporter_compatible,
            is_c_data_array_exporter_compatible,
            is_c_data_schema_importer_compatible,
            is_c_data_array_importer_compatible,
        )

    def _run(self, arrow_path: str, json_path: str, command: str, quirks):
        env = {
            "ARROW_PATH": arrow_path,
            "JSON_PATH": json_path,
            "COMMAND": command,
            **{f"QUIRK_{q}": "1" for q in quirks or ()},
        }

        if self.debug:
            log(f"{self._INTEGRATION_EXE} {env}")

        run_cmd([self._INTEGRATION_EXE], env=env)

    def validate(self, json_path: str, arrow_path: str, quirks=None):
        return self._run(arrow_path, json_path, "VALIDATE", quirks)

    def json_to_file(self, json_path: str, arrow_path: str):
        return self._run(arrow_path, json_path, "JSON_TO_ARROW", quirks=None)

    def stream_to_file(self, stream_path, file_path):
        cmd = [self._STREAM_TO_FILE, "<", stream_path, ">", file_path]
        self.run_shell_command(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = [self._FILE_TO_STREAM, file_path, ">", stream_path]
        self.run_shell_command(cmd)

    def make_c_data_exporter(self):
        return ExternalLibraryCDataExporter(
            self.debug,
            self._INTEGRATION_DLL,
            self._entrypoints,
            self._supports_releasing_memory,
            self.args,
        )

    def make_c_data_importer(self):
        return ExternalLibraryCDataImporter(
            self.debug,
            self._INTEGRATION_DLL,
            self._entrypoints,
            self._supports_releasing_memory,
            self.args,
        )


def get_entrypoints(
    is_c_data_schema_exporter_compatible: bool,
    is_c_data_array_exporter_compatible: bool,
    is_c_data_schema_importer_compatible: bool,
    is_c_data_array_importer_compatible: bool,
) -> str:
    """
    Get the entrypoints for the C Data integration library based on compatibility flags.
    """
    entrypoint_conditions = [
        (
            is_c_data_schema_exporter_compatible,
            _EXTERNAL_LIBRARY_C_DATA_EXPORT_SCHEMA_FROM_JSON_ENTRYPOINT,
        ),
        (
            is_c_data_array_exporter_compatible,
            _EXTERNAL_LIBRARY_C_DATA_EXPORT_BATCH_FROM_JSON_ENTRYPOINT,
        ),
        (
            is_c_data_schema_importer_compatible,
            _EXTERNAL_LIBRARY_C_DATA_IMPORT_SCHEMA_AND_COMPARE_TO_JSON_ENTRYPOINT,
        ),
        (
            is_c_data_array_importer_compatible,
            _EXTERNAL_LIBRARY_C_DATA_IMPORT_BATCH_AND_COMPARE_TO_JSON_ENTRYPOINT,
        ),
    ]
    return (
        "\n".join(
            entrypoint for condition, entrypoint in entrypoint_conditions if condition
        )
        + "\n"
        + _EXTERNAL_BYTES_ALLOCATED_ENTRYPOINT
    )


@functools.lru_cache
def _load_ffi(ffi, lib_path: Path, entrypoints: str):
    ffi.cdef(entrypoints)
    dll = ffi.dlopen(str(lib_path))
    return dll


class _CDataBase:

    def __init__(
        self,
        debug: bool,
        integration_dll_path: Path,
        entrypoints: str,
        args,
    ):
        self.debug = debug
        self.args = args
        self.ffi = cdata.ffi()
        self.dll = _load_ffi(self.ffi, integration_dll_path, entrypoints)

    def _check_external_library_error(self, na_error):
        """
        Check a `const char*` error return from an integration entrypoint.

        A null means success, a non-empty string is an error message.
        The string is statically allocated on the external library side and does not
        need to be released.
        """
        assert self.ffi.typeof(na_error) is self.ffi.typeof("const char*")
        if na_error != self.ffi.NULL:
            error = self.ffi.string(na_error).decode("utf8", errors="replace")
            raise RuntimeError(
                f"External library C Data Integration call failed: {error}"
            )


class ExternalLibraryCDataExporter(CDataExporter, _CDataBase):
    _supports_releasing_memory: bool = True

    def __init__(
        self,
        debug: bool,
        integration_dll_path: Path,
        entrypoints: str,
        supports_releasing_memory: bool,
        args,
    ):
        super().__init__(
            debug=debug,
            integration_dll_path=integration_dll_path,
            entrypoints=entrypoints,
            args=args,
        )
        self._supports_releasing_memory = supports_releasing_memory

    def export_schema_from_json(self, json_path, c_schema_ptr):
        na_error = self.dll.external_CDataIntegration_ExportSchemaFromJson(
            str(json_path).encode(), c_schema_ptr
        )
        self._check_external_library_error(na_error)

    def export_batch_from_json(self, json_path, num_batch: int, c_array_ptr):
        na_error = self.dll.external_CDataIntegration_ExportBatchFromJson(
            str(json_path).encode(), num_batch, c_array_ptr
        )
        self._check_external_library_error(na_error)

    @property
    def supports_releasing_memory(self):
        return self._supports_releasing_memory

    def record_allocation_state(self):
        return self.dll.BytesAllocated()


class ExternalLibraryCDataImporter(CDataImporter, _CDataBase):
    _supports_releasing_memory: bool = True

    def __init__(
        self,
        debug: bool,
        integration_dll_path: Path,
        entrypoints: str,
        supports_releasing_memory: bool,
        args,
    ):
        super().__init__(
            debug=debug,
            integration_dll_path=integration_dll_path,
            entrypoints=entrypoints,
            args=args,
        )
        self._supports_releasing_memory = supports_releasing_memory

    def import_schema_and_compare_to_json(self, json_path, c_schema_ptr):
        na_error = self.dll.external_CDataIntegration_ImportSchemaAndCompareToJson(
            str(json_path).encode(), c_schema_ptr
        )
        self._check_external_library_error(na_error)

    def import_batch_and_compare_to_json(self, json_path, num_batch, c_array_ptr):
        na_error = self.dll.external_CDataIntegration_ImportBatchAndCompareToJson(
            str(json_path).encode(), num_batch, c_array_ptr
        )
        self._check_external_library_error(na_error)

    @property
    def supports_releasing_memory(self):
        return self._supports_releasing_memory
