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

import asyncio

import pytest

import pyarrow as pa
from pyarrow.lib import _test_roundtrip_async


def test_async_record_batch_reader_basic():
    schema = pa.schema([('x', pa.int64())])
    batches = [
        pa.record_batch([pa.array([1, 2, 3])], schema=schema),
        pa.record_batch([pa.array([4, 5, 6])], schema=schema),
    ]

    async def _test():
        reader = await _test_roundtrip_async(schema, batches)
        assert isinstance(reader, pa.AsyncRecordBatchReader)
        assert reader.schema == schema

        results = []
        async for batch in reader:
            results.append(batch)

        assert len(results) == 2
        assert results[0].equals(batches[0])
        assert results[1].equals(batches[1])

    asyncio.run(_test())


def test_async_record_batch_reader_empty():
    schema = pa.schema([('x', pa.int64())])

    async def _test():
        reader = await _test_roundtrip_async(schema, [])
        assert reader.schema == schema

        results = [b async for b in reader]
        assert len(results) == 0

    asyncio.run(_test())


def test_async_record_batch_reader_schema():
    schema = pa.schema([
        ('a', pa.float32()),
        ('b', pa.utf8()),
        ('c', pa.list_(pa.int32())),
    ])
    batch = pa.record_batch(
        [
            pa.array([1.0, 2.0], type=pa.float32()),
            pa.array(['hello', 'world']),
            pa.array([[1, 2], [3]], type=pa.list_(pa.int32())),
        ],
        schema=schema,
    )

    async def _test():
        reader = await _test_roundtrip_async(schema, [batch])
        assert reader.schema == schema

        results = [b async for b in reader]
        assert len(results) == 1
        assert results[0].equals(batch)

    asyncio.run(_test())


def test_async_record_batch_reader_context_manager():
    schema = pa.schema([('x', pa.int64())])
    batches = [pa.record_batch([pa.array([1, 2, 3])], schema=schema)]

    async def _test():
        reader = await _test_roundtrip_async(schema, batches)
        async with reader as r:
            results = [b async for b in r]
        assert len(results) == 1
        assert results[0].equals(batches[0])

    asyncio.run(_test())


def test_async_record_batch_reader_many_batches():
    schema = pa.schema([('x', pa.int64())])
    batches = [
        pa.record_batch([pa.array([i])], schema=schema)
        for i in range(20)
    ]

    async def _test():
        reader = await _test_roundtrip_async(schema, batches, queue_size=2)
        results = [b async for b in reader]
        assert len(results) == 20
        for i, batch in enumerate(results):
            assert batch.equals(batches[i])

    asyncio.run(_test())
