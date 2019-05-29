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

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Apache.Arrow
{
    public class RecordBatch : IDisposable
    {
        public Schema Schema { get; }
        public int ColumnCount => _arrays.Count;
        public IEnumerable<IArrowArray> Arrays => _arrays;
        public int Length { get; }

        private readonly IMemoryOwner<byte> _memoryOwner;
        private readonly IList<IArrowArray> _arrays;

        public IArrowArray Column(int i)
        {
            return _arrays[i];
        }

        public IArrowArray Column(string columnName)
        {
            var fieldIndex = Schema.GetFieldIndex(columnName);
            return _arrays[fieldIndex];
        }

        public void Dispose()
        {
            Dispose(disposing: true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _memoryOwner?.Dispose();

                foreach (IArrowArray array in _arrays)
                {
                    array.Dispose();
                }
            }
        }

        public RecordBatch(Schema schema, IEnumerable<IArrowArray> data, int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            _arrays = data?.ToList() ?? throw new ArgumentNullException(nameof(data));

            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            Length = length;
        }

        internal RecordBatch(Schema schema, IMemoryOwner<byte> memoryOwner, List<IArrowArray> arrays, int length)
        {
            Debug.Assert(schema != null);
            Debug.Assert(arrays != null);
            Debug.Assert(length >= 0);

            _memoryOwner = memoryOwner;
            _arrays = arrays;
            Schema = schema;
            Length = length;
        }
    }
}
