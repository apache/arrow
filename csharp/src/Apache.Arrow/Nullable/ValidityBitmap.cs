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

namespace Apache.Arrow.Nullable
{
    using global::Apache.Arrow;
    using System;

    public class ValidityBitmap
    {
        private Memory<byte> memory;

        private int count;

        public ValidityBitmap(int count)
        {
            if (count <= 0)
            {
                throw new ArgumentException("count must be positive integer", nameof(count));
            }

            this.count = count;

            this.NullCount = count;

            var nullbytes = (int)Math.Ceiling(count / 8.0);
            memory = new Memory<byte>(new byte[nullbytes]);
        }


        public int NullCount { get; private set; }

        public ArrowBuffer Build()
        {
            if (this.NullCount == 0)
            {
                return ArrowBuffer.Empty;
            }

            return new ArrowBuffer(this.memory.ToArray());
        }

        public void SetValid(int index)
        {
            if (index >= count || index < 0)
            {
                throw new IndexOutOfRangeException($"capacity:{count} index:{index}");
            }

            var isValid = BitUtility.GetBit(this.memory.Span, index);

            if (!isValid)
            {
                BitUtility.SetBit(this.memory.Span, index);
                --NullCount;
            }
        }
    }
}