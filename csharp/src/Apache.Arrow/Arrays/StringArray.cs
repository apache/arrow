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
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class StringArray: BinaryArray, IReadOnlyList<string>, ICollection<string>
    {
        public static readonly Encoding DefaultEncoding = Encoding.UTF8;

        private Dictionary<Encoding, string[]> materializedStringStore;

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

        /// <summary>
        /// Get the string value at the given index
        /// </summary>
        /// <param name="index">Input index</param>
        /// <param name="encoding">Optional: the string encoding, default is UTF8</param>
        /// <returns>The string object at the given index</returns>
        public string GetString(int index, Encoding encoding = default)
        {
            encoding ??= DefaultEncoding;

            if (materializedStringStore != null && materializedStringStore.TryGetValue(encoding, out string[] materializedStrings))
            {
                return materializedStrings[index];
            }

            ReadOnlySpan<byte> bytes = GetBytes(index, out bool isNull);

            if (isNull)
            {
                return null;
            }

            if (bytes.Length == 0)
            {
                return string.Empty;
            }

            unsafe
            {
                fixed (byte* data = &MemoryMarshal.GetReference(bytes))
                    return encoding.GetString(data, bytes.Length);
            }
        }

        /// <summary>
        /// Materialize the array for the given encoding to accelerate the string access
        /// </summary>
        /// <param name="encoding">Optional: the string encoding, default is UTF8</param>
        /// <remarks>This method is not thread safe when it is called in parallel with <see cref="GetString(int, Encoding)"/> or <see cref="Materialize(Encoding)"/>.</remarks>
        public void Materialize(Encoding encoding = default)
        {
            encoding ??= DefaultEncoding;

            if (IsMaterialized(encoding))
            {
                return;
            }

            if (materializedStringStore == null)
            {
                materializedStringStore = new Dictionary<Encoding, string[]>();
            }

            var stringStore = new string[Length];
            for (int i = 0; i < Length; i++)
            {
                stringStore[i] = GetString(i, encoding);
            }

            materializedStringStore[encoding] = stringStore;
        }

        /// <summary>
        /// Check if the array has been materialized for the given encoding
        /// </summary>
        /// <param name="encoding">Optional: the string encoding, default is UTF8</param>
        /// <returns>True of false whether the array has been materialized</returns>
        public bool IsMaterialized(Encoding encoding = default)
        {
            if (materializedStringStore == null)
            {
                return false;
            }

            encoding ??= DefaultEncoding;
            return materializedStringStore.ContainsKey(encoding);
        }

        int IReadOnlyCollection<string>.Count => Length;

        string IReadOnlyList<string>.this[int index] => GetString(index);

        IEnumerator<string> IEnumerable<string>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetString(index);
            };
        }

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<string>)this).GetEnumerator();

        int ICollection<string>.Count => Length;
        bool ICollection<string>.IsReadOnly => true;
        void ICollection<string>.Add(string item) => throw new NotSupportedException("Collection is read-only.");
        bool ICollection<string>.Remove(string item) => throw new NotSupportedException("Collection is read-only.");
        void ICollection<string>.Clear() => throw new NotSupportedException("Collection is read-only.");

        bool ICollection<string>.Contains(string item)
        {
            for (int index = 0; index < Length; index++)
            {
                if (GetString(index) == item)
                    return true;
            }

            return false;
        }

        void ICollection<string>.CopyTo(string[] array, int arrayIndex)
        {
            for (int srcIndex = 0, destIndex = arrayIndex; srcIndex < Length; srcIndex++, destIndex++)
            {
                array[destIndex] = GetString(srcIndex);
            }
        }
    }
}
