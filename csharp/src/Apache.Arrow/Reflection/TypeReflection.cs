using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow.Reflection
{
    public class TypeReflection
    {
        internal static IArrowType GetArrowType(System.Type type)
            => new Field.Builder().DataType(type).CurrentType;

#if NETCOREAPP2_0_OR_GREATER
        internal static IEnumerable<PropertyInfo> GetProperties(System.Type type) => type
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.GetIndexParameters().Length == 0);

        internal static bool IsIterable(System.Type type)
            => typeof(IEnumerable).IsAssignableFrom(type) && type != typeof(string);
        internal static bool IsNestedStruct(System.Type type)
            => type.IsValueType && !type.IsEnum && !type.IsPrimitive;
#else
        internal static IEnumerable<PropertyInfo> GetProperties(System.Type type)
            => throw new NotSupportedException("Cannot get properties, need to run on .net core >= 2.0");
#endif
    }

    public class TypeReflection<T> : TypeReflection
    {
        internal static readonly Type DotNetType = typeof(T);
        public static readonly IArrowType ArrowType = GetArrowType(DotNetType);
    }
}
