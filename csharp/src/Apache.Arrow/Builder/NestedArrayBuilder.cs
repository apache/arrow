using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Memory;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class NestedArrayBuilder : ArrayBuilder
    {
        public NestedArrayBuilder(NestedType dataType, int capacity = 8)
            : base(
                  dataType,
                  new IValueBufferBuilder[] { new ValueBufferBuilder<bool>(capacity) },
                  dataType.Fields.Select(field => ArrayBuilderFactory.MakeBuilder(field.DataType)).ToArray()
                  )
        {
        }

        internal NestedArrayBuilder(ListType dataType, int capacity = 8)
            : base(
                  dataType,
                  new IValueBufferBuilder[] { new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder<int>(capacity) },
                  dataType.Fields.Select(field => ArrayBuilderFactory.MakeBuilder(field.DataType)).ToArray()
                  )
        {
        }

        public virtual Status Append() => AppendValid();
        public TBuilder GetBuilderAs<TBuilder>(int index) where TBuilder : ArrayBuilder => Children[index].As<TBuilder>();
    }

    public class ListArrayBuilder : NestedArrayBuilder
    {
        public new ListType DataType { get; }
        public int CurrentOffset { get; internal set; }

        public ListArrayBuilder(ListType dataType, int capacity = 8) : base(dataType, capacity)
        {
            DataType = dataType;
            CurrentOffset = 0;
            OffsetsBuffer.AppendValue(CurrentOffset);
        }

        public IPrimitiveBufferBuilder<int> OffsetsBuffer => Buffers[1] as IPrimitiveBufferBuilder<int>;

        // Append Valididty
        public override Status AppendNull()
        {
            OffsetsBuffer.AppendValue(CurrentOffset);
            return base.AppendNull();
        }

        public override Status AppendNulls(int count)
        {
            OffsetsBuffer.AppendValues(CurrentOffset, count);
            return base.AppendNulls(count);
        }

        // Append Value
        public override Status AppendScalar(IScalar value)
        {
            return value switch
            {
                IBaseListScalar list => AppendScalar(list),
                INullableScalar nullable => nullable.IsValid ? AppendScalar(nullable.Value) : AppendNull(),
                _ => throw new ArgumentException($"Cannot append scalar {value} in {this}, must implement IBaseListScalar")
            };
        }

        public Status AppendValue(IBaseListScalar value)
        {
            if (value.IsValid)
            {
                Children[0].AppendArray(value.Array);
                return AppendValid();
            }
            return AppendNull();
        }

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public ListArray Build(MemoryAllocator allocator = default, bool _ = true)
            => new ListArray(FinishInternal(allocator));
    }

    public class StructArrayBuilder : NestedArrayBuilder
    {
        public new StructType DataType { get; }

        public StructArrayBuilder(StructType dataType, int capacity = 8) : base(dataType, capacity)
        {
            DataType = dataType;
        }

        // Append Valididty
        public override Status AppendNull()
        {
            foreach (IArrayBuilder child in Children)
                child.AppendNull();

            return base.AppendNull();
        }

        public override Status AppendNulls(int count)
        {
            foreach (IArrayBuilder child in Children)
                child.AppendNulls(count);

            return base.AppendNulls(count);
        }

        public override Status AppendScalar(IScalar value)
            => value.IsValid ? AppendValue((IStructScalar)value) : AppendNull();
        public Status AppendValue(IStructScalar value)
        {
            if (value.IsValid)
            {
                for (int i = 0; i < Children.Length; i++)
                {
                    var childValue = value.Fields[i];

                    if (childValue == null)
                        Children[i].AppendNull();
                    else
                        Children[i].AppendScalar(childValue);
                }
                return AppendValid();
            }
            return AppendNull();
        }

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);
        public async override Task<IArrowArray> BuildAsync(MemoryAllocator allocator = default)
            => await BuildAsync(allocator);

        public StructArray Build(MemoryAllocator allocator = default, bool _ = true)
            => new StructArray(FinishInternal(allocator));

        public async Task<StructArray> BuildAsync(MemoryAllocator allocator = default, bool _ = true)
            => new StructArray(await FinishInternalAsync(allocator));

        public RecordBatch BuildRecordBatch(MemoryAllocator allocator = default, IEnumerable<KeyValuePair<string, string>> metadata = null)
            => new(new Schema(DataType.Fields, metadata), Build(allocator).Fields, Length);

        public async Task<RecordBatch> BuildRecordBatchAsync(MemoryAllocator allocator = default, IEnumerable<KeyValuePair<string, string>> metadata = null)
        {
            var built = await BuildAsync(allocator);
            return new(new Schema(DataType.Fields, metadata), built.Fields, Length);
        }
    }

    public class ListArrayBuilder<T> : ListArrayBuilder where T : struct
    {
        public readonly FixedBinaryArrayBuilder<T> ValueBuilder;

        public ListArrayBuilder(int capacity = 32)
            : base(new ListType(TypeReflection<T>.ArrowType), capacity)
        {
            ValueBuilder = GetBuilderAs<FixedBinaryArrayBuilder<T>>(0);
        }

        public Status AppendValue(ReadOnlySpan<T> value)
        {
            CurrentOffset++;
            OffsetsBuffer.AppendValue(CurrentOffset);
            ValueBuilder.AppendValues(value);
            return AppendValid();
        }

        public Status AppendValues(ICollection<T[]> values)
        {
            Span<T> memory = new T[values.Sum(row => row.Length)];
            Span<int> offsets = new int[values.Count];
            Span<bool> mask = new bool[offsets.Length];
            int offset = 0;
            int i = 0;
            int nullCount = 0;

            foreach (T[] value in values)
            {
                if (value == null)
                {
                    offsets[i] = CurrentOffset;
                    // default is already false
                    // mask[i] = false;
                    nullCount++;
                }
                else
                {
                    // Copy to memory
                    value.CopyTo(memory.Slice(offset, value.Length));

                    offset += value.Length;
                    CurrentOffset += value.Length;

                    // Fill other buffers
                    offsets[i] = CurrentOffset;
                    mask[i] = true;
                }
                i++;
            }

            return AppendValues(memory, offsets, mask, nullCount);
        }

        private Status AppendValues(
            ReadOnlySpan<T> values, ReadOnlySpan<int> offsets, ReadOnlySpan<bool> mask, int nullCount
            )
        {
            // Append Offset
            CurrentOffset = offsets[offsets.Length - 1];
            OffsetsBuffer.AppendValues(offsets);

            // Append Values
            ValueBuilder.AppendValues(values);
            return AppendValidity(mask, nullCount);
        }
    }

    public class StructArrayBuilder<T> : StructArrayBuilder where T : struct
    {
        public StructArrayBuilder(int capacity = 32) : base(TypeReflection<T>.ArrowType as StructType, capacity)
        {
        }
    }
}

