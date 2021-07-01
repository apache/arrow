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
import uuid
import time
import pyarrow.parquet as pq
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor


class SplittedParquetWriter(object):
    """
    Split a large multi rowgroup Parquet file to a set of single rowgroup
    Parquet files of a specified size.

    Parameters
    ---------
    filename: the name of the file to split.
    destination: The directory where to write the split Parquet files.
    chunksize: The required chunk size.
    """

    def __init__(self, filename, destination, chunksize=128*1024*1024):
        self.filename = filename
        self.destination = destination
        self.chunksize = chunksize

    def round(self, num):
        """
        Round a number.

        Parameter
        ---------
        num: The number to round off.

        Returns
        ---------
        result: int
            The rounded off number.
        """
        num_str = str(int(num))
        result_str = ""
        result_str += num_str[0]
        for i in range(len(num_str) - 1):
            result_str += "0"
        return int(result_str)

    def write_file(self, filename, table):
        """
        Update CephFS striping strategy and writes
        the table to a set of split files.

        Parameters
        ---------
        filename: Name of the file to write to.
        table: The table slice to write.
        """
        open(filename, 'a').close()
        attribute = "ceph.file.layout.object_size"
        os.system(
            f"setfattr -n {attribute} -v 134217728 {filename}")
        pq.write_table(
            table, filename,
            row_group_size=table.num_rows, compression=None
        )

    def estimate_rows(self):
        """
        Estimate the number of rows in the table.

        Returns
        ---------
        num_rows: int
            The number of rows in the table.
        rounded: int
            The rounded number of rows required per file.
        """
        self.table = pq.read_table(self.filename)
        disk_size = os.stat(self.filename).st_size
        inmemory_table_size = self.table.nbytes
        inmemory_row_size = inmemory_table_size/self.table.num_rows
        compression_ratio = inmemory_table_size/disk_size
        required_inmemory_table_size = self.chunksize * compression_ratio
        required_rows_per_file = required_inmemory_table_size/inmemory_row_size
        return self.table.num_rows, self.round(required_rows_per_file)

    def write(self):
        """
        Write the data to a set of split files.
        """
        os.makedirs(self.destination, exist_ok=True)
        s_time = time.time()
        total_rows, rows_per_file = self.estimate_rows()
        i = 0
        with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
            while i < total_rows:
                executor.submit(
                    self.write_file,
                    os.path.join(
                        self.destination, f"{uuid.uuid4().hex}.parquet"),
                    self.table.slice(i, rows_per_file)
                )
                i += rows_per_file
        e_time = time.time()
        print(f"Finished writing in {e_time - s_time} seconds")
