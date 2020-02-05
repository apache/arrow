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

using Apache.Arrow.Types;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Apache.Arrow
{
    public class StringArray: BinaryArray
    {
        public static readonly Encoding DefaultEncoding = Encoding.UTF8;

        public new class Builder : BuilderBase<StringArray, Builder>
        {
            public Builder() : base(StringType.Default) { }

            protected override StringArray Build(ArrayData data)
            {
                return new StringArray(data);
            }

            public Builder Append(string value, Encoding encoding = null)
            {
                encoding = encoding ?? DefaultEncoding;
                var span = encoding.GetBytes(value);
                return Append(span);
            }

            public Builder AppendRange(IEnumerable<string> values, Encoding encoding = null)
            {
                foreach (var value in values)
                {
                    Append(value);
                }

                return this;
            }
        }

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
            encoding = encoding ?? DefaultEncoding;

            var bytes = GetBytes(index);

            unsafe
            {
                fixed (byte* data = &MemoryMarshal.GetReference(bytes))
                    return encoding.GetString(data, bytes.Length);
            }
        }
    }
}
