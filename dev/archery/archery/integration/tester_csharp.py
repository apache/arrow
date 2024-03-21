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

from contextlib import contextmanager
import os

from . import cdata
from .tester import Tester, CDataExporter, CDataImporter
from .util import run_cmd, log
from ..utils.source import ARROW_ROOT_DEFAULT


_ARTIFACTS_PATH = os.path.join(ARROW_ROOT_DEFAULT, "csharp/artifacts")

_EXE_PATH = os.path.join(_ARTIFACTS_PATH,
                         "Apache.Arrow.IntegrationTest",
                         "Debug/net7.0/Apache.Arrow.IntegrationTest",
                         )

_clr_loaded = False


def _load_clr():
    global _clr_loaded
    if not _clr_loaded:
        _clr_loaded = True
        os.environ['DOTNET_GCHeapHardLimit'] = '0xC800000'  # 200 MiB
        import pythonnet
        pythonnet.load("coreclr")
        import clr
        clr.AddReference(
            f"{_ARTIFACTS_PATH}/Apache.Arrow.IntegrationTest/"
            f"Debug/net7.0/Apache.Arrow.IntegrationTest.dll")
        clr.AddReference(
            f"{_ARTIFACTS_PATH}/Apache.Arrow.Tests/"
            f"Debug/net7.0/Apache.Arrow.Tests.dll")

        from Apache.Arrow.IntegrationTest import CDataInterface
        CDataInterface.Initialize()


@contextmanager
def _disposing(disposable):
    """
    Ensure the IDisposable object is disposed of when the enclosed block exits.
    """
    try:
        yield disposable
    finally:
        disposable.Dispose()


class _CDataBase:

    def __init__(self, debug, args):
        self.debug = debug
        self.args = args
        self.ffi = cdata.ffi()
        _load_clr()

    def _pointer_to_int(self, c_ptr):
        return int(self.ffi.cast('uintptr_t', c_ptr))

    def _read_batch_from_json(self, json_path, num_batch):
        from Apache.Arrow.IntegrationTest import CDataInterface

        return CDataInterface.ParseJsonFile(json_path).ToArrow(num_batch)

    def _run_gc(self):
        from Apache.Arrow.IntegrationTest import CDataInterface
        CDataInterface.RunGC()


class CSharpCDataExporter(CDataExporter, _CDataBase):

    def export_schema_from_json(self, json_path, c_schema_ptr):
        from Apache.Arrow.IntegrationTest import CDataInterface

        jf = CDataInterface.ParseJsonFile(json_path)
        CDataInterface.ExportSchema(jf.Schema.ToArrow(),
                                    self._pointer_to_int(c_schema_ptr))

    def export_batch_from_json(self, json_path, num_batch, c_array_ptr):
        from Apache.Arrow.IntegrationTest import CDataInterface

        _, batch = self._read_batch_from_json(json_path, num_batch)
        with _disposing(batch):
            CDataInterface.ExportRecordBatch(batch,
                                             self._pointer_to_int(c_array_ptr))

    @property
    def supports_releasing_memory(self):
        # XXX the C# GC doesn't give reliable allocation measurements
        return False

    def run_gc(self):
        self._run_gc()


class CSharpCDataImporter(CDataImporter, _CDataBase):

    def import_schema_and_compare_to_json(self, json_path, c_schema_ptr):
        from Apache.Arrow.IntegrationTest import CDataInterface
        from Apache.Arrow.Tests import SchemaComparer

        jf = CDataInterface.ParseJsonFile(json_path)
        imported_schema = CDataInterface.ImportSchema(
            self._pointer_to_int(c_schema_ptr))
        SchemaComparer.Compare(jf.Schema.ToArrow(), imported_schema)

    def import_batch_and_compare_to_json(self, json_path, num_batch,
                                         c_array_ptr):
        from Apache.Arrow.IntegrationTest import CDataInterface
        from Apache.Arrow.Tests import ArrowReaderVerifier

        schema, batch = self._read_batch_from_json(json_path, num_batch)
        with _disposing(batch):
            imported_batch = CDataInterface.ImportRecordBatch(
                self._pointer_to_int(c_array_ptr), schema)
            with _disposing(imported_batch):
                ArrowReaderVerifier.CompareBatches(batch, imported_batch,
                                                   strictCompare=False)

    @property
    def supports_releasing_memory(self):
        return True

    def run_gc(self):
        self._run_gc()


class CSharpTester(Tester):
    PRODUCER = True
    CONSUMER = True
    C_DATA_SCHEMA_EXPORTER = True
    C_DATA_SCHEMA_IMPORTER = True
    C_DATA_ARRAY_EXPORTER = True
    C_DATA_ARRAY_IMPORTER = True

    name = 'C#'

    def _run(self, json_path=None, arrow_path=None, command='validate'):
        cmd = [_EXE_PATH]

        cmd.extend(['--mode', command])

        if json_path is not None:
            cmd.extend(['-j', json_path])

        if arrow_path is not None:
            cmd.extend(['-a', arrow_path])

        if self.debug:
            log(' '.join(cmd))

        run_cmd(cmd)

    def validate(self, json_path, arrow_path, quirks=None):
        return self._run(json_path, arrow_path, 'validate')

    def json_to_file(self, json_path, arrow_path):
        return self._run(json_path, arrow_path, 'json-to-arrow')

    def stream_to_file(self, stream_path, file_path):
        cmd = [_EXE_PATH]
        cmd.extend(['--mode', 'stream-to-file', '-a', file_path])
        cmd.extend(['<', stream_path])
        self.run_shell_command(cmd)

    def file_to_stream(self, file_path, stream_path):
        cmd = [_EXE_PATH]
        cmd.extend(['--mode', 'file-to-stream'])
        cmd.extend(['-a', file_path, '>', stream_path])
        self.run_shell_command(cmd)

    def make_c_data_exporter(self):
        return CSharpCDataExporter(self.debug, self.args)

    def make_c_data_importer(self):
        return CSharpCDataImporter(self.debug, self.args)
