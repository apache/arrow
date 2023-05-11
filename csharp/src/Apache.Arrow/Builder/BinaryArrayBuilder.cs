﻿using Apache.Arrow.Memory;
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

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public StringArray Build(MemoryAllocator allocator = default, bool bin = false, bool str = true)
            => new StringArray(FinishInternal(allocator));
    }
}
