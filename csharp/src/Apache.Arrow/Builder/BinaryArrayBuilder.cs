using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class BinaryArrayBuilder : VariablePrimitiveArrayBuilder<byte>
    {
        public BinaryArrayBuilder(int capacity = 64)
            : base(BinaryType.Default, capacity)
        {
        }

        internal BinaryArrayBuilder(IArrowType dtype, int capacity = 64)
            : base(dtype, capacity)
        {
        }
    }

    public class StringArrayBuilder : BinaryArrayBuilder
    {
        public StringArrayBuilder(int capacity = 64)
            : this(StringType.Default, capacity)
        {
        }

        internal StringArrayBuilder(IArrowType dtype, int capacity = 64)
            : base(dtype, capacity)
        {
        }
    }
}
