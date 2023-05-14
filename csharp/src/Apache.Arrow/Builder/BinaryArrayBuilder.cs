using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class BinaryArrayBuilder : VariableBinaryArrayBuilder
    {
        public BinaryArrayBuilder(int capacity = ArrayBuilder.DefaultCapacity)
            : base(BinaryType.Default, capacity)
        {
        }

        internal BinaryArrayBuilder(IArrowType dtype, int capacity = ArrayBuilder.DefaultCapacity)
            : base(dtype, capacity)
        {
        }
        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public BinaryArray Build(MemoryAllocator allocator = default, bool bin = true)
            => new BinaryArray(FinishInternal(allocator));
    }

    public class StringArrayBuilder : BinaryArrayBuilder
    {
        public StringArrayBuilder(int capacity = ArrayBuilder.DefaultCapacity)
            : this(StringType.Default, capacity)
        {
        }

        internal StringArrayBuilder(IArrowType dtype, int capacity = ArrayBuilder.DefaultCapacity)
            : base(dtype, capacity)
        {
        }

        /// <summary>
        /// Append string value to builder encoded in UTF8
        /// </summary>
        /// <param name="value">string value</param>
        /// <param name="count">repeat x times</param>
        public virtual Status AppendValue(string value) => AppendValue(value, StringType.DefaultEncoding);

        /// <summary>
        /// Append string value to builder encoded
        /// </summary>
        /// <param name="value">string value</param>
        /// <param name="count">repeat x times</param>
        /// <param name="encoding"><see cref="Encoding"/> to convert string to bytes</param>
        public virtual Status AppendValue(string value, Encoding encoding)
        {
            if (value is null)
                return AppendNull();
            else if (value.Length == 0)
                return AppendValue(ReadOnlySpan<byte>.Empty);

            int byteLength = encoding.GetByteCount(value);

            ReadOnlySpan<char> chars = value.AsSpan();
            Span<byte> encoded = byteLength < MaxByteStackAllocSize ?
                stackalloc byte[byteLength] : new byte[byteLength];

            unsafe
            {
                fixed (byte* pBytes = encoded)
                fixed (char* pChars = chars)
                {
                    encoding.GetBytes(pChars, chars.Length, pBytes, encoded.Length);
                    return AppendValue(encoded);
                }
            }
        }

        /// <summary>
        /// Append string value to builder repeated x times.
        /// </summary>
        /// <param name="value">string value</param>
        /// <param name="count">repeat x times</param>
        public virtual Status AppendValues(string value, int count)
            => AppendValues(value, count, StringType.DefaultEncoding);

        /// <summary>
        /// Append string value to builder repeated x times encoded
        /// </summary>
        /// <param name="value">string value</param>
        /// <param name="count">repeat x times</param>
        /// <param name="encoding"><see cref="Encoding"/> to convert string to bytes</param>
        public virtual Status AppendValues(string value, int count, Encoding encoding)
        {
            if (value is null)
                return AppendNulls(count);
            else if (value.Length == 0)
                return AppendValues(ReadOnlySpan<byte>.Empty, count);

            int byteLength = encoding.GetByteCount(value);

            ReadOnlySpan<char> chars = value.AsSpan();
            Span<byte> encoded = byteLength < MaxByteStackAllocSize ?
                stackalloc byte[byteLength] : new byte[byteLength];

            unsafe
            {
                fixed (byte* pBytes = encoded)
                fixed (char* pChars = chars)
                {
                    encoding.GetBytes(pChars, chars.Length, pBytes, encoded.Length);
                    return AppendValues(encoded, count);
                }
            }
        }

        public virtual Status AppendValues(IEnumerable<string> values)
            => AppendValues(values, StringType.DefaultEncoding);

        public virtual Status AppendValues(IEnumerable<string> values, Encoding encoding)
            => AppendValues(values.Select(str => str is null ? null : encoding.GetBytes(str)));

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public StringArray Build(MemoryAllocator allocator = default, bool bin = false, bool str = true)
            => new StringArray(FinishInternal(allocator));
    }
}
