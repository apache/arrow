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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    /// <summary>
    /// Implements an <see cref="ArrowStreamReader"/> that can read Arrow files.
    /// </summary>
    public class ArrowFileReader : ArrowStreamReader
    {
        private ArrowFileReaderImplementation Implementation =>
            (ArrowFileReaderImplementation)_implementation;

        public bool IsFileValid => Implementation.IsFileValid;

        public ArrowFileReader(Stream stream)
            : this(stream, leaveOpen: false)
        {
        }

        public ArrowFileReader(Stream stream, MemoryAllocator allocator)
            : this(stream, allocator, leaveOpen: false)
        {
        }

        public ArrowFileReader(Stream stream, bool leaveOpen)
            : this(stream, allocator: null, leaveOpen)
        {
        }

        public ArrowFileReader(Stream stream, MemoryAllocator allocator, bool leaveOpen)
            : base(new ArrowFileReaderImplementation(stream, allocator, leaveOpen))
        {
        }

        public static ArrowFileReader FromFile(string filename)
        {
            var stream = new FileStream(filename, FileMode.Open, FileAccess.Read);
            return new ArrowFileReader(stream);
        }

        public ValueTask<int> RecordBatchCountAsync()
        {
            return Implementation.RecordBatchCountAsync();
        }

        public ValueTask<RecordBatch> ReadRecordBatchAsync(int index, CancellationToken cancellationToken = default)
        {
            return Implementation.ReadRecordBatchAsync(index, cancellationToken);
        }
    }
}
