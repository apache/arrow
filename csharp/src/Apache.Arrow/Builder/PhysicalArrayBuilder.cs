using System;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class PhysicalArrayBuilder<T> : BaseArrayBuilder where T : struct
    {
        public virtual IBufferBuilder ValuesBuffer => Buffers[1];

        public PhysicalArrayBuilder(
            IArrowType dataType,
            IBufferBuilder validity, IBufferBuilder values
            ) : this(dataType, new IBufferBuilder[] { validity, values })
        {
        }

        public PhysicalArrayBuilder(
            IArrowType dataType, IBufferBuilder[] buffers,
            IDataBuilder[] children = null, IDataBuilder dictionary = null
            ) : base(dataType, buffers, children, dictionary)
        {
        }

        public virtual PhysicalArrayBuilder<T> AppendValue(T value)
        {
            base.AppendValue();
            ValuesBuffer.AppendStruct(value);
            return this;
        }

        public virtual PhysicalArrayBuilder<T> AppendValues(ReadOnlySpan<T> values)
        {
            base.AppendValues(values.Length);
            ValuesBuffer.AppendStructs(values);
            return this;
        }
    }

    public class PhysicalOffsetArrayBuilder<T> : PhysicalArrayBuilder<T> where T : struct
    {
        public IBufferBuilder OffsetsBuffer => Buffers[0];
        public override IBufferBuilder ValuesBuffer => Buffers[1];

        public PhysicalOffsetArrayBuilder(
            IArrowType dataType,
            IBufferBuilder validity, IBufferBuilder offsets, IBufferBuilder values
            ) : this(dataType, new IBufferBuilder[] { validity, offsets, values })
        {
        }

        public PhysicalOffsetArrayBuilder(
            IArrowType dataType, IBufferBuilder[] buffers,
            IDataBuilder[] children = null, IDataBuilder dictionary = null
            ) : base(dataType, buffers, children, dictionary)
        {
            OffsetsBuffer.AppendStruct(Length);
        }

        public new PhysicalOffsetArrayBuilder<T> AppendValue(T value)
        {
            base.AppendValue(value);
            OffsetsBuffer.AppendStruct(Length);
            return this;
        }

        public new PhysicalOffsetArrayBuilder<T> AppendValues(ReadOnlySpan<T> values)
        {
            base.AppendValues(values);
            OffsetsBuffer.AppendStruct(Length);
            return this;
        }
    }
}
