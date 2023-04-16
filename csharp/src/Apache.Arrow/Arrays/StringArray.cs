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
using System.Text;
using Apache.Arrow.Types;

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
                if (value == null)
                {
                    return AppendNull();
                }
                encoding = encoding ?? DefaultEncoding;
                byte[] span = encoding.GetBytes(value);
                return Append(span.AsSpan());
            }

            public Builder AppendRange(IEnumerable<string> values, Encoding encoding = null)
            {
                foreach (string value in values)
                {
                    Append(value, encoding);
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

        public new string this[int index]
        {
            get
            {
                return index < 0 ? GetString(Length + index) : GetString(index);
            }
            // TODO: Implement setter
            //set
            //{
            //    data[index] = value;
            //}
        }

        // Accessors
        public new Accessor<StringArray, string> Items() => Items<StringArray, string>((a, i) => a.GetString(i));
        public new Accessor<StringArray, string> NotNullItems() => Items<StringArray, string>((a, i) => a.GetString(i));
    }
}
