using System.Linq;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests;

public class UnionArrayTests
{
    [Theory]
    [InlineData(UnionMode.Sparse)]
    [InlineData(UnionMode.Dense)]
    public void UnionArray_IsNull(UnionMode mode)
    {
        var fields = new Field[]
        {
            new Field("field0", new Int32Type(), true),
            new Field("field1", new FloatType(), true),
        };
        var typeIds = fields.Select(f => (int) f.DataType.TypeId).ToArray();
        var type = new UnionType(fields, typeIds, mode);

        const int length = 100;
        var nullCount = 0;
        var field0Builder = new Int32Array.Builder();
        var field1Builder = new FloatArray.Builder();
        var typeIdsBuilder = new ArrowBuffer.Builder<byte>();
        var valuesOffsetBuilder = new ArrowBuffer.Builder<int>();
        var expectedNull = new bool[length];

        for (var i = 0; i < length; ++i)
        {
            var isNull = i % 5 == 0;
            expectedNull[i] = isNull;
            nullCount += isNull ? 1 : 0;

            if (i % 2 == 0)
            {
                typeIdsBuilder.Append(0);
                if (mode == UnionMode.Sparse)
                {
                    field1Builder.Append(0.0f);
                }
                else
                {
                    valuesOffsetBuilder.Append(field0Builder.Length);
                }

                if (isNull)
                {
                    field0Builder.AppendNull();
                }
                else
                {
                    field0Builder.Append(i);
                }
            }
            else
            {
                typeIdsBuilder.Append(1);
                if (mode == UnionMode.Sparse)
                {
                    field0Builder.Append(0);
                }
                else
                {
                    valuesOffsetBuilder.Append(field1Builder.Length);
                }

                if (isNull)
                {
                    field1Builder.AppendNull();
                }
                else
                {
                    field1Builder.Append(i * 0.1f);
                }
            }
        }

        var typeIdsBuffer = typeIdsBuilder.Build();
        var valuesOffsetBuffer = valuesOffsetBuilder.Build();
        var children = new IArrowArray[]
        {
            field0Builder.Build(),
            field1Builder.Build()
        };

        UnionArray array = mode == UnionMode.Dense
            ? new DenseUnionArray(type, length, children, typeIdsBuffer, valuesOffsetBuffer, nullCount)
            : new SparseUnionArray(type, length, children, typeIdsBuffer, nullCount);

        for (var i = 0; i < length; ++i)
        {
            Assert.Equal(expectedNull[i], array.IsNull(i));
            Assert.Equal(!expectedNull[i], array.IsValid(i));
        }
    }
}
