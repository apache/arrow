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

using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class StringArray: BinaryArray
    {
        public StringArray(ArrayData data) 
            : base(ArrowTypeId.String, data) { }

        public StringArray(int length,
            ArrowBuffer valueOffsetsBuffer,
            ArrowBuffer dataBuffer,
            ArrowBuffer nullBitmapBuffer,
            int nullCount = 0, int offset = 0)
            : this(new ArrayData(StringType.Default, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueOffsetsBuffer, dataBuffer }))
        { }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public string GetString(int index, Encoding encoding = default)
        {
            encoding = encoding ?? Encoding.UTF8;

            var bytes = GetBytes(index);

            unsafe
            {
                fixed (byte* data = &MemoryMarshal.GetReference(bytes))
                    return encoding.GetString(data, bytes.Length);
            }
        }
    }
}
