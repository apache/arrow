using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Memory;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public abstract class NestedArrayBuilder : ArrayBuilder
    {
        public NestedArrayBuilder(NestedType dataType, int capacity = 8)
            : base(
                  dataType,
                  new ITypedBufferBuilder[] { new TypedBufferBuilder<bool>(capacity) },
                  dataType.Fields.Select(field => ArrayBuilderFactory.MakeBuilder(field.DataType)).ToArray()
                  )
        {
        }

        internal NestedArrayBuilder(ListType dataType, int capacity = 8)
            : base(
                  dataType,
                  new ITypedBufferBuilder[] { new TypedBufferBuilder<bool>(capacity), new TypedBufferBuilder<int>(capacity) },
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

        public ITypedBufferBuilder<int> OffsetsBuffer => Buffers[1] as ITypedBufferBuilder<int>;

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

        internal override Status AppendPrimitiveValueOffset(ArrayData data)
        {
            if (data.Length == 0)
                return Status.OK;

            // Value Offsets
            var offsets = data.Buffers[1].Span.Slice(data.Offset * 4, (data.Length + 1) * 4).CastTo<int>();
            int currentOffset = CurrentOffset;
            Span<int> newOffsets = new int[data.Length];

            // Element length = array[index + 1] - array[index]
            for (int i = 0; i < data.Length; i++)
            {
                // Add element length to current offset
                currentOffset += offsets[i + 1] - offsets[i];
                newOffsets[i] = currentOffset;
            }

            CurrentOffset = currentOffset;
            OffsetsBuffer.AppendValues(newOffsets);

            return Status.OK;
        }
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
        {
            return value switch
            {
                IStructScalar svalue => AppendScalar(svalue),
                INullableScalar nullable => nullable.IsValid ? AppendScalar(nullable.Value) : AppendNull(),
                _ => throw new ArgumentException($"Cannot append scalar {value} in {this}, must implement IStructScalar")
            };
        }

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

        public Status AppendBatch(RecordBatch batch) => AppendBatch(batch.ArrayList);

        public Status AppendBatch(IReadOnlyCollection<IArrowArray> arrays)
        {
            int length = 0;

            for (int i = 0; i < Children.Length; i++)
            {
                var array = arrays.ElementAt(i);
                Children[i].AppendArray(array);
                length = array.Length;
            }
            return AppendValidity(true, length);
        }

        public async Task<Status> AppendBatchAsync(RecordBatch batch)
            => await AppendBatchAsync(batch.ArrayList);

        public async Task<Status> AppendBatchAsync(IReadOnlyCollection<IArrowArray> arrays)
        {
            Task<Status>[] tasks = new Task<Status>[Children.Length];
            int length = 0;

            for (int i = 0; i < Children.Length; i++)
            {
                int index = i; // Create a local copy of the loop variable
                var array = arrays.ElementAt(i);
                length = array.Length;
                tasks[index] = Children[index].AppendArrayAsync(array);
            }

            await Task.WhenAll(tasks);

            return AppendValidity(true, length);
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

        internal override Status AppendPrimitiveValueOffset(ArrayData data) => Status.OK;
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

