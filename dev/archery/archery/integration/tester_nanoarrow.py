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

import functools
import os

from . import cdata
from .tester import Tester, CDataExporter, CDataImporter
from ..utils.source import ARROW_ROOT_DEFAULT


_NANOARROW_PATH = os.environ.get(
    "ARROW_NANOARROW_PATH",
    os.path.join(ARROW_ROOT_DEFAULT, "nanoarrow/cdata"),
)

_INTEGRATION_DLL = os.path.join(
    _NANOARROW_PATH, "libnanoarrow_c_data_integration" + cdata.dll_suffix
)


class NanoarrowTester(Tester):
    PRODUCER = False
    CONSUMER = False
    FLIGHT_SERVER = False
    FLIGHT_CLIENT = False
    C_DATA_SCHEMA_EXPORTER = True
    C_DATA_ARRAY_EXPORTER = True
    C_DATA_SCHEMA_IMPORTER = True
    C_DATA_ARRAY_IMPORTER = True

    name = "nanoarrow"

    def validate(self, json_path, arrow_path, quirks=None):
        raise NotImplementedError()

    def json_to_file(self, json_path, arrow_path):
        raise NotImplementedError()

    def stream_to_file(self, stream_path, file_path):
        raise NotImplementedError()

    def file_to_stream(self, file_path, stream_path):
        raise NotImplementedError()

    def make_c_data_exporter(self):
        return NanoarrowCDataExporter(self.debug, self.args)

    def make_c_data_importer(self):
        return NanoarrowCDataImporter(self.debug, self.args)


_nanoarrow_c_data_entrypoints = """
    const char* nanoarrow_CDataIntegration_ExportSchemaFromJson(
        const char* json_path, struct ArrowSchema* out);

    const char* nanoarrow_CDataIntegration_ImportSchemaAndCompareToJson(
        const char* json_path, struct ArrowSchema* schema);

    const char* nanoarrow_CDataIntegration_ExportBatchFromJson(
        const char* json_path, int num_batch, struct ArrowArray* out);

    const char* nanoarrow_CDataIntegration_ImportBatchAndCompareToJson(
        const char* json_path, int num_batch, struct ArrowArray* batch);

    int64_t nanoarrow_BytesAllocated(void);
    """


@functools.lru_cache
def _load_ffi(ffi, lib_path=_INTEGRATION_DLL):
    ffi.cdef(_nanoarrow_c_data_entrypoints)
    dll = ffi.dlopen(lib_path)
    return dll


class _CDataBase:
    def __init__(self, debug, args):
        self.debug = debug
        self.args = args
        self.ffi = cdata.ffi()
        self.dll = _load_ffi(self.ffi)

    def _check_nanoarrow_error(self, na_error):
        """
        Check a `const char*` error return from an integration entrypoint.

        A null means success, a non-empty string is an error message.
        The string is statically allocated on the nanoarrow side and does not
        need to be released.
        """
        assert self.ffi.typeof(na_error) is self.ffi.typeof("const char*")
        if na_error != self.ffi.NULL:
            error = self.ffi.string(na_error).decode("utf8", errors="replace")
            raise RuntimeError(f"nanoarrow C Data Integration call failed: {error}")


class NanoarrowCDataExporter(CDataExporter, _CDataBase):
    def export_schema_from_json(self, json_path, c_schema_ptr):
        na_error = self.dll.nanoarrow_CDataIntegration_ExportSchemaFromJson(
            str(json_path).encode(), c_schema_ptr
        )
        self._check_nanoarrow_error(na_error)

    def export_batch_from_json(self, json_path, num_batch, c_array_ptr):
        na_error = self.dll.nanoarrow_CDataIntegration_ExportBatchFromJson(
            str(json_path).encode(), num_batch, c_array_ptr
        )
        self._check_nanoarrow_error(na_error)

    @property
    def supports_releasing_memory(self):
        return True

    def record_allocation_state(self):
        return self.dll.nanoarrow_BytesAllocated()


class NanoarrowCDataImporter(CDataImporter, _CDataBase):
    def import_schema_and_compare_to_json(self, json_path, c_schema_ptr):
        na_error = self.dll.nanoarrow_CDataIntegration_ImportSchemaAndCompareToJson(
            str(json_path).encode(), c_schema_ptr
        )
        self._check_nanoarrow_error(na_error)

    def import_batch_and_compare_to_json(self, json_path, num_batch, c_array_ptr):
        na_error = self.dll.nanoarrow_CDataIntegration_ImportBatchAndCompareToJson(
            str(json_path).encode(), num_batch, c_array_ptr
        )
        self._check_nanoarrow_error(na_error)

    @property
    def supports_releasing_memory(self):
        return True
