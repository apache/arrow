# -*- coding: utf-8 -*-
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

from collections import OrderedDict

import numpy as np
import pandas as pd
import pyarrow as pa


def dataframe_with_arrays():
    """
    Dataframe with numpy arrays columns of every possible primtive type.

    Returns
    -------
    df: pandas.DataFrame
    schema: pyarrow.Schema
        Arrow schema definition that is in line with the constructed df.
    """
    dtypes = [('i1', pa.int8()), ('i2', pa.int16()),
              ('i4', pa.int32()), ('i8', pa.int64()),
              ('u1', pa.uint8()), ('u2', pa.uint16()),
              ('u4', pa.uint32()), ('u8', pa.uint64()),
              ('f4', pa.float_()), ('f8', pa.double())]

    arrays = OrderedDict()
    fields = []
    for dtype, arrow_dtype in dtypes:
        fields.append(pa.field(dtype, pa.list_(arrow_dtype)))
        arrays[dtype] = [
            np.arange(10, dtype=dtype),
            np.arange(5, dtype=dtype),
            None,
            np.arange(1, dtype=dtype)
        ]

    fields.append(pa.field('str', pa.list_(pa.string())))
    arrays['str'] = [
        np.array([u"1", u"ä"], dtype="object"),
        None,
        np.array([u"1"], dtype="object"),
        np.array([u"1", u"2", u"3"], dtype="object")
    ]

    fields.append(pa.field('datetime64', pa.list_(pa.timestamp('ms'))))
    arrays['datetime64'] = [
        np.array(['2007-07-13T01:23:34.123456789',
                  None,
                  '2010-08-13T05:46:57.437699912'],
                 dtype='datetime64[ms]'),
        None,
        None,
        np.array(['2007-07-13T02',
                  None,
                  '2010-08-13T05:46:57.437699912'],
                 dtype='datetime64[ms]'),
    ]

    df = pd.DataFrame(arrays)
    schema = pa.Schema.from_fields(fields)

    return df, schema
