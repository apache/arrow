using System.Linq;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class StructArrayBuilder : ArrayBuilder
    {
        public StructArrayBuilder(NestedType dataType, int capacity = 8)
            : base(
                  dataType,
                  new IValueBufferBuilder[] { new ValueBufferBuilder<bool>(capacity) },
                  dataType.Fields.Select(field => ArrayBuilderFactory.Make(field.DataType)).ToArray()
                  )
        {
        }

        public TBuilder GetBuilderAs<TBuilder>(int index) where TBuilder : ArrayBuilder => Children[index] as TBuilder;

        // Append Valididty
        public virtual StructArrayBuilder Append()
        {
            AppendValid();
            return this;
        }
        public override IArrayBuilder AppendNull() => AppendNull(true);
        public virtual StructArrayBuilder AppendNull(bool recursive = true)
        {
            base.AppendNull();

            foreach (IArrayBuilder child in Children)
                child.AppendNull();

            return this;
        }

        public override IArrayBuilder AppendNulls(int count) => AppendNulls(count, true);
        public virtual StructArrayBuilder AppendNulls(int count, bool recursive = true)
        {
            base.AppendNulls(count);

            foreach (IArrayBuilder child in Children)
                child.AppendNulls(count);

            return this;
        }
    }

    public class StructArrayBuilder<T> : StructArrayBuilder where T : struct
    {
        public StructArrayBuilder(int capacity = 8) : base(CStructType<T>.Default as NestedType, capacity)
        {
        }
    }
}

