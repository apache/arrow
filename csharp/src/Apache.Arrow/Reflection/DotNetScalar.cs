using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public ref struct DotNetScalarArray
    {
        public readonly IArrowType ArrowType;
        public readonly System.Type DotNetType;
        public readonly bool Nullable;
        public readonly IEnumerable<object> Values;

        public static DotNetScalarArray Make<T>(IEnumerable<T> values)
        {
            var type = typeof(T);
            System.Type child = System.Nullable.GetUnderlyingType(type);

            return child != null ?
                new DotNetScalarArray(CStructType<T>.Default, child, values.Select(value => (object)value), true) :
                new DotNetScalarArray(CStructType<T>.Default, type, values.Select(value => (object)value), false);
        }

        public DotNetScalarArray(IArrowType arrowType, System.Type type, IEnumerable<object> values, bool nullable)
        {
            ArrowType = arrowType;
            DotNetType = type;
            Values = values;
            Nullable = nullable;
        }

        public IEnumerable<T> ValuesAs<T>() => Values.Select(value => (T)value);
    }
}
