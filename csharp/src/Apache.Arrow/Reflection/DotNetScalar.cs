using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public enum DotNetScalarType
    {
        Scalar,
        Iterable,
        NestedStruct
    }

    public readonly ref struct DotNetScalar
    {
        public readonly IArrowType ArrowType;
        public readonly System.Type DotNetType;
        public readonly DotNetScalarType ScalarType;

        public readonly System.Type[] SubTypes;
        public readonly object Value;
        public readonly object[] Values;

        public bool IsValid => Value != null;
        
        public static DotNetScalar Make<T>(T value)
        {
            System.Type type = typeof(T);
            var arrowType = TypeReflection<T>.ArrowType;

            return type switch
            {
                var _ when TypeReflection<T>.NestedStruct => new DotNetScalar(
                    arrowType,
                    type, value,
                    DotNetScalarType.NestedStruct,
                    TypeReflection<T>.PropertyTypes, TypeReflection<T>.PropertyValues(value)
                    ),
                var _ when TypeReflection<T>.Iterable => new DotNetScalar(
                    arrowType,
                    type, value,
                    DotNetScalarType.Iterable,
                    null, ((IEnumerable<object>)value).ToArray()
                    ),
                _ => new DotNetScalar(arrowType, type, value),
            };
        }

        private static DotNetScalar Make(IArrowType arrowType, System.Type type, object value)
        {
            return type switch
            {
                var _ when TypeReflection.IsNestedStruct(type) => new DotNetScalar(
                    arrowType,
                    type, value,
                    DotNetScalarType.NestedStruct,
                    TypeReflection.GetPropertyTypes(type).ToArray(), TypeReflection.PropertyValues(type, value)
                    ),
                var _ when TypeReflection.IsIterable(type) => new DotNetScalar(
                    arrowType,
                    type, value,
                    DotNetScalarType.Iterable,
                    null, ((IEnumerable<object>)value).ToArray()
                    ),
                _ => new DotNetScalar(arrowType, type, value),
            };
        }

        public DotNetScalar(
            IArrowType arrowType,
            System.Type type, object value,
            DotNetScalarType scalarType = DotNetScalarType.Scalar,
            System.Type[] subTypes = null, object[] values = null
            )
        {
            ArrowType = arrowType;
            DotNetType = type;
            ScalarType = scalarType;
            SubTypes = subTypes;
            Value = value;
            Values = values;
        }

        public T ArrowTypeAs<T>() where T : ArrowType => ArrowType as T;

        public DotNetScalar Child(int index)
        {
            return ScalarType switch
            {
                DotNetScalarType.NestedStruct =>
                    Make(ArrowTypeAs<StructType>().Fields[index].DataType, SubTypes[index], Values[index]),
                _ => throw new NotSupportedException("Cannot get child on not nested struct DotNetScalar"),
            };
        }

        public T ValueAs<T>() => (T)Value;
        public ReadOnlySpan<byte> AsBytes()
        {
            switch (ScalarType)
            {
                case DotNetScalarType.Iterable:
                    var span = new byte[Values.Length];

                    for (int i = 0; i < span.Length; i++)
                        span[i] = (byte)Values[i];

                    return span;
                default:
                    throw new NotSupportedException("Cannot convert to bytes");
            }
        }
    }
}
