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

        public virtual Status AppendValue(string value)
            => value is null ? AppendNull() : AppendValue(StringType.DefaultEncoding.GetBytes(value));

        /// <summary>
        /// Append string value too builder repeated x times.
        /// </summary>
        /// <param name="value">string value</param>
        /// <param name="count">repeat x times</param>
        public virtual Status AppendValues(string value, int count)
            => AppendValues(value, count, StringType.DefaultEncoding);

        /// <summary>
        /// Append string value too builder repeated x times.
        /// </summary>
        /// <param name="value">string value</param>
        /// <param name="count">repeat x times</param>
        /// <param name="encoding">encoding to convert string to bytes</param>
        public virtual Status AppendValues(string value, int count, Encoding encoding)
        {
            if (value is null)
                return AppendNulls(count);

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
