// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow.Memory;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    /// <summary>
    /// Represents a reader that can read Arrow streams.
    /// </summary>
    public class ArrowStreamReader : IArrowReader, IArrowArrayStream, IDisposable
    {
        private protected readonly ArrowReaderImplementation _implementation;

        public Schema Schema => _implementation.Schema;

        public ArrowStreamReader(Stream stream)
            : this(stream, allocator: null, compressionCodecFactory: null, leaveOpen: false)
        {
        }

        public ArrowStreamReader(Stream stream, MemoryAllocator allocator)
            : this(stream, allocator, compressionCodecFactory: null, leaveOpen: false)
        {
        }

        public ArrowStreamReader(Stream stream, ICompressionCodecFactory compressionCodecFactory)
            : this(stream, allocator: null, compressionCodecFactory, leaveOpen: false)
        {
        }

        public ArrowStreamReader(Stream stream, bool leaveOpen)
            : this(stream, allocator: null, compressionCodecFactory: null, leaveOpen)
        {
        }

        public ArrowStreamReader(Stream stream, MemoryAllocator allocator, bool leaveOpen)
            : this(stream, allocator, compressionCodecFactory: null, leaveOpen)
        {
        }

        public ArrowStreamReader(Stream stream, ICompressionCodecFactory compressionCodecFactory, bool leaveOpen)
            : this(stream, allocator: null, compressionCodecFactory, leaveOpen)
        {
        }

        public ArrowStreamReader(Stream stream, MemoryAllocator allocator, ICompressionCodecFactory compressionCodecFactory, bool leaveOpen)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            _implementation = new ArrowStreamReaderImplementation(stream, allocator, compressionCodecFactory, leaveOpen);
        }

        public ArrowStreamReader(ReadOnlyMemory<byte> buffer)
        {
            _implementation = new ArrowMemoryReaderImplementation(buffer, compressionCodecFactory: null);
        }

        public ArrowStreamReader(ReadOnlyMemory<byte> buffer, ICompressionCodecFactory compressionCodecFactory)
        {
            _implementation = new ArrowMemoryReaderImplementation(buffer, compressionCodecFactory);
        }

        private protected ArrowStreamReader(ArrowReaderImplementation implementation)
        {
            _implementation = implementation;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _implementation.Dispose();
            }
        }

        public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            return _implementation.ReadNextRecordBatchAsync(cancellationToken);
        }

        public RecordBatch ReadNextRecordBatch()
        {
            return _implementation.ReadNextRecordBatch();
        }
    }
}
