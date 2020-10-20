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

import os
import pyarrow as pa
import pyarrow.parquet as pq


class SplittedParquetWriter(object):
    def __init__(self, source_path, destination_path, chunksize):
        self.source_path = source_path
        self.destination_path = destination_path
        self.chunksize = chunksize
        self.file = pq.ParquetFile(source_path)
        self._schema = self.file.schema_arrow
        self._fileno = -1

    def __del__(self):
        self.close()

    def _create_new_file(self):
        self._fileno += 1
        _fd = open(os.path.join(
            self.destination_path, f"file.{self._fileno}.parquet"), "wb")
        return _fd

    def _open_new_file(self):
        self._current_fd = self._create_new_file()
        self._current_sink = pa.PythonFile(self._current_fd, mode="w")
        self._current_writer = pq.ParquetWriter(
            self._current_sink, self._schema)

    def _close_current_file(self):
        self._current_writer.close()
        self._current_sink.close()
        self._current_fd.close()

    def write(self):
        self._open_new_file()
        for batch in self.file.iter_batches():  # default batch_size=64k
            table = pa.Table.from_batches([batch])
            self._current_writer.write_table(table)
            if self._current_sink.tell() < self.chunksize:
                continue
            else:
                self._close_current_file()
                self._open_new_file()

        self._close_current_file()

    def close(self):
        num_files_written = self._fileno + 1
        for i in range(num_files_written):
            table = pq.read_table(f"file.{i}.parquet")
            pq.write_table(
                table,
                where=f"file.{i}.parquet",
                row_group_size=table.num_rows
            )

        self._fileno = -1
        return num_files_written
