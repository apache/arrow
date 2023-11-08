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
using System.Collections.Generic;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// A data structure to manage a list of primitive Array arrays logically as one large array
    /// </summary>
    public class ChunkedArray
    {
        private IList<IArrowArray> Arrays { get; }
        public IArrowType DataType { get; }
        public long Length { get; }
        public long NullCount { get; }

        public int ArrayCount
        {
            get => Arrays.Count;
        }

        public Array Array(int index) => Arrays[index] as Array;

        public IArrowArray ArrowArray(int index) => Arrays[index];

        public ChunkedArray(IList<Array> arrays)
            : this(Cast(arrays))
        {
        }

        public ChunkedArray(IList<IArrowArray> arrays)
        {
            Arrays = arrays ?? throw new ArgumentNullException(nameof(arrays));
            if (arrays.Count < 1)
            {
                throw new ArgumentException($"Count must be at least 1. Got {arrays.Count} instead");
            }
            DataType = arrays[0].Data.DataType;
            foreach (IArrowArray array in arrays)
            {
                Length += array.Length;
                NullCount += array.NullCount;
            }
        }

        public ChunkedArray(Array array) : this(new IArrowArray[] { array }) { }

        public ChunkedArray Slice(long offset, long length)
        {
            if (offset >= Length)
            {
                throw new ArgumentException($"Index {offset} cannot be greater than the Column's Length {Length}");
            }

            int curArrayIndex = 0;
            int numArrays = Arrays.Count;
            while (curArrayIndex < numArrays && offset > Arrays[curArrayIndex].Length)
            {
                offset -= Arrays[curArrayIndex].Length;
                curArrayIndex++;
            }

            IList<IArrowArray> newArrays = new List<IArrowArray>();
            while (curArrayIndex < numArrays && length > 0)
            {
                newArrays.Add(ArrowArrayFactory.Slice(Arrays[curArrayIndex], (int)offset,
                              length > Arrays[curArrayIndex].Length ? Arrays[curArrayIndex].Length : (int)length));
                length -= Arrays[curArrayIndex].Length - offset;
                offset = 0;
                curArrayIndex++;
            }
            return new ChunkedArray(newArrays);
        }

        public ChunkedArray Slice(long offset)
        {
            return Slice(offset, Length - offset);
        }

        public override string ToString() => $"{nameof(ChunkedArray)}: Length={Length}, DataType={DataType.Name}";
      
        private static IArrowArray[] Cast(IList<Array> arrays)
        {
            IArrowArray[] arrowArrays = new IArrowArray[arrays.Count];
            for (int i = 0; i < arrays.Count; i++)
            {
                arrowArrays[i] = arrays[i];
            }
            return arrowArrays;
        }

        // TODO: Flatten for Structs
    }
}
