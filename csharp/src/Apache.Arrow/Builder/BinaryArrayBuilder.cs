using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class BinaryArrayBuilder : VariableBinaryArrayBuilder
    {
        public BinaryArrayBuilder(int capacity = 32)
            : base(BinaryType.Default, capacity)
        {
        }

        internal BinaryArrayBuilder(IArrowType dtype, int capacity = 32)
            : base(dtype, capacity)
        {
        }
        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public BinaryArray Build(MemoryAllocator allocator = default, bool bin = true)
            => new BinaryArray(FinishInternal(allocator));
    }

    public class StringArrayBuilder : BinaryArrayBuilder
    {
        public StringArrayBuilder(int capacity = 32)
            : this(StringType.Default, capacity)
        {
        }

        internal StringArrayBuilder(IArrowType dtype, int capacity = 32)
            : base(dtype, capacity)
        {
        }

        public virtual Status AppendValue(string value)
            => value == null ? AppendNull() : AppendValue(StringType.DefaultEncoding.GetBytes(value));

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
            if (value == null)
                return AppendNulls(count);
            var encoded = encoding.GetBytes(value);
            return AppendValues(encoded, count);
        }

        public virtual Status AppendValues(IEnumerable<string> values)
            => AppendValues(values, StringType.DefaultEncoding);

        public virtual Status AppendValues(IEnumerable<string> values, Encoding encoding)
            => AppendValues(values.Select(str => str == null ? null : encoding.GetBytes(str)));

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public StringArray Build(MemoryAllocator allocator = default, bool bin = false, bool str = true)
            => new StringArray(FinishInternal(allocator));
    }
}
