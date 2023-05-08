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

        internal static IEnumerable<Type> GetPropertyTypes(System.Type type)
            => GetProperties(type).Select(p => p.PropertyType);

        internal static IEnumerable<MethodInfo> GetGetters(System.Type type)
            => GetProperties(type).Select(p => p.GetGetMethod());

        internal static bool IsIterable(System.Type type) => typeof(IEnumerable).IsAssignableFrom(type) && type != typeof(string);
        internal static bool IsNestedStruct(System.Type type)
            => type.IsValueType && !type.IsEnum && !type.IsPrimitive;
#else
        internal static IEnumerable<PropertyInfo> GetProperties(System.Type type)
            => throw new NotSupportedException("Cannot get properties, need to run on .net core >= 2.0");

        internal static IEnumerable<Type> GetPropertyTypes(System.Type type)
            => throw new NotSupportedException("Cannot get properties, need to run on .net core >= 2.0");

        public static IEnumerable<MethodInfo> GetGetters(System.Type type)
            => throw new NotSupportedException("Cannot get getters, need to run on .net core >= 2.0");

        internal static bool IsIterable(System.Type type) => false;

        internal static bool IsNestedStruct(System.Type type) => false;
#endif
        internal static object[] PropertyValues(System.Type type, object value)
            => PropertyValues(value, GetGetters(type).ToArray());

        internal static object[] PropertyValues(object value, MethodInfo[] getters)
        {
            var values = new object[getters.Length];

            for (int i = 0; i < values.Length; i++)
                values[i] = getters[i].Invoke(value, new object[] { });

            return values;
        }
    }

    public class TypeReflection<T> : TypeReflection
    {
        internal static readonly Type DotNetType = typeof(T);
        public static readonly IArrowType ArrowType = GetArrowType(DotNetType);
        internal static readonly PropertyInfo[] Properties = GetProperties(DotNetType).ToArray();
        internal static readonly Type[] PropertyTypes = Properties.Select(p => p.PropertyType).ToArray();
        internal static readonly MethodInfo[] Getters = GetGetters(DotNetType).ToArray();

        public static readonly bool Iterable = IsIterable(DotNetType);
        public static readonly bool NestedStruct = IsNestedStruct(DotNetType);

        public static object[] PropertyValues(object value) => PropertyValues(value, Getters);
    }
}
