using System;
using System.Collections.Generic;
using System.Linq;
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

        public override IArrayBuilder AppendDotNet(DotNetScalarArray values) => AppendDotNet(values, true);
        public BinaryArrayBuilder AppendDotNet(DotNetScalarArray values, bool bin = true)
        {
            switch (values.ArrowType.TypeId)
            {
                case ArrowTypeId.Binary:
                    AppendValues(values.ValuesAs<IEnumerable<byte>>().Select(value => value.ToArray()).ToArray());
                    break;
                default:
                    throw new ArgumentException($"Cannot dynamically append values of type {values.DotNetType}");
            };
            return this;
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

        public override IArrayBuilder AppendDotNet(DotNetScalarArray values) => AppendDotNet(values, false, true);
        public StringArrayBuilder AppendDotNet(DotNetScalarArray values, bool bin = false, bool str = true)
        {
            switch (values.ArrowType.TypeId)
            {
                case ArrowTypeId.String:
                    AppendValues(values.ValuesAs<string>().ToArray());
                    break;
                default:
                    base.AppendDotNet(values);
                    break;
            };
            return this;
        }

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public StringArray Build(MemoryAllocator allocator = default, bool bin = false, bool str = true)
            => new StringArray(FinishInternal(allocator));
    }
}
