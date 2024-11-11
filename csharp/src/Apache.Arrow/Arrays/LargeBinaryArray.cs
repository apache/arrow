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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Apache.Arrow;

public class LargeBinaryArray : Array, IReadOnlyList<byte[]>, ICollection<byte[]>
{
    public LargeBinaryArray(ArrayData data)
        : base(data)
    {
        data.EnsureDataType(ArrowTypeId.LargeBinary);
        data.EnsureBufferCount(3);
    }

    public LargeBinaryArray(ArrowTypeId typeId, ArrayData data)
        : base(data)
    {
        data.EnsureDataType(typeId);
        data.EnsureBufferCount(3);
    }

    public LargeBinaryArray(IArrowType dataType, int length,
        ArrowBuffer valueOffsetsBuffer,
        ArrowBuffer dataBuffer,
        ArrowBuffer nullBitmapBuffer,
        int nullCount = 0, int offset = 0)
    : this(new ArrayData(dataType, length, nullCount, offset,
        new[] { nullBitmapBuffer, valueOffsetsBuffer, dataBuffer }))
    { }

    public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

    public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

    public ArrowBuffer ValueBuffer => Data.Buffers[2];

    public ReadOnlySpan<long> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<long>().Slice(Offset, Length + 1);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetValueLength(int index)
    {
        if (index < 0 || index >= Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }
        if (!IsValid(index))
        {
            return 0;
        }

        ReadOnlySpan<long> offsets = ValueOffsets;
        return checked((int)(offsets[index + 1] - offsets[index]));
    }

    /// <summary>
    /// Get the collection of bytes, as a read-only span, at a given index in the array.
    /// </summary>
    /// <remarks>
    /// Note that this method cannot reliably identify null values, which are indistinguishable from empty byte
    /// collection values when seen in the context of this method's return type of <see cref="ReadOnlySpan{Byte}"/>.
    /// Use the <see cref="Array.IsNull"/> method or the <see cref="GetBytes(int, out bool)"/> overload instead
    /// to reliably determine null values.
    /// </remarks>
    /// <param name="index">Index at which to get bytes.</param>
    /// <returns>Returns a <see cref="ReadOnlySpan{Byte}"/> object.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If the index is negative or beyond the length of the array.
    /// </exception>
    public ReadOnlySpan<byte> GetBytes(int index) => GetBytes(index, out _);

    /// <summary>
    /// Get the collection of bytes, as a read-only span, at a given index in the array.
    /// </summary>
    /// <param name="index">Index at which to get bytes.</param>
    /// <param name="isNull">Set to <see langword="true"/> if the value at the given index is null.</param>
    /// <returns>Returns a <see cref="ReadOnlySpan{Byte}"/> object.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If the index is negative or beyond the length of the array.
    /// </exception>
    public ReadOnlySpan<byte> GetBytes(int index, out bool isNull)
    {
        if (index < 0 || index >= Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        isNull = IsNull(index);

        if (isNull)
        {
            // Note that `return null;` is valid syntax, but would be misleading as `null` in the context of a span
            // is actually returned as an empty span.
            return ReadOnlySpan<byte>.Empty;
        }

        var offset = checked((int)ValueOffsets[index]);
        return ValueBuffer.Span.Slice(offset, GetValueLength(index));
    }

    int IReadOnlyCollection<byte[]>.Count => Length;

    byte[] IReadOnlyList<byte[]>.this[int index] => GetBytes(index).ToArray();

    IEnumerator<byte[]> IEnumerable<byte[]>.GetEnumerator()
    {
        for (int index = 0; index < Length; index++)
        {
            yield return GetBytes(index).ToArray();
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<byte[]>)this).GetEnumerator();

    int ICollection<byte[]>.Count => Length;
    bool ICollection<byte[]>.IsReadOnly => true;
    void ICollection<byte[]>.Add(byte[] item) => throw new NotSupportedException("Collection is read-only.");
    bool ICollection<byte[]>.Remove(byte[] item) => throw new NotSupportedException("Collection is read-only.");
    void ICollection<byte[]>.Clear() => throw new NotSupportedException("Collection is read-only.");

    bool ICollection<byte[]>.Contains(byte[] item)
    {
        for (int index = 0; index < Length; index++)
        {
            if (GetBytes(index).SequenceEqual(item))
                return true;
        }

        return false;
    }

    void ICollection<byte[]>.CopyTo(byte[][] array, int arrayIndex)
    {
        for (int srcIndex = 0, destIndex = arrayIndex; srcIndex < Length; srcIndex++, destIndex++)
        {
            array[destIndex] = GetBytes(srcIndex).ToArray();
        }
    }
}
