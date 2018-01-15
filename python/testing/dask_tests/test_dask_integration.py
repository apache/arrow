#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pytest

from datetime import date, timedelta
import csv
from random import randint

import pyarrow as pa

dd = pytest.importorskip('dask.dataframe')


def make_datafiles(tmpdir, prefix='data', num_files=20):
    rowcount = 5000
    fieldnames = ['date', 'temperature', 'dewpoint']
    start_date = date(1900, 1, 1)
    for i in range(num_files):
        filename = '{0}/{1}-{2}.csv'.format(tmpdir, prefix, i)
        with open(filename, 'w') as outcsv:
            writer = csv.DictWriter(outcsv, fieldnames)
            writer.writeheader()
            the_date = start_date
            for _ in range(rowcount):
                temperature = randint(-10, 35)
                dewpoint = temperature - randint(0, 10)
                writer.writerow({'date': the_date, 'temperature': temperature,
                                 'dewpoint': dewpoint})
                the_date += timedelta(days=1)


def test_dask_file_read(tmpdir):
    prefix = 'data'
    make_datafiles(tmpdir, prefix)
    # Read all datafiles in parallel
    datafiles = '{0}/{1}-*.csv'.format(tmpdir, prefix)
    dask_df = dd.read_csv(datafiles)
    # Convert Dask dataframe to Arrow table
    table = pa.Table.from_pandas(dask_df.compute())
    # Second column (1) is temperature
    dask_temp = int(1000 * dask_df['temperature'].mean().compute())
    arrow_temp = int(1000 * table[1].to_pandas().mean())
    assert dask_temp == arrow_temp
