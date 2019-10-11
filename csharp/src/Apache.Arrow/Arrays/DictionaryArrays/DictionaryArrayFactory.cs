using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow.Arrays.DictionaryArrays
{
    public static class DictionaryArrayFactory
    {
        public static IDictionaryArray BuildArray(ArrayData data, int uniqueValuesCount)
        {
            data.EnsureDataType(ArrowTypeId.Dictionary);
            var dictType = (DictionaryType) data.DataType;
            if (dictType == null)
            {
                throw  new ArgumentException($"Cannot infer enclosed type as data.DataType doesn't inherit from {typeof(DictionaryType)}." +
                                             $" Is of type {data.DataType.GetType()}");
            }

            switch (dictType.ContainedTypeId)
            {
                case ArrowTypeId.UInt8:
                    return new UInt8DictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.Int8:
                    return new Int8DictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.UInt16:
                    return new UInt16DictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.Int16:
                    return new Int16DictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.UInt32:
                    return new UInt32DictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.Int32:
                    return new Int32DictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.UInt64:
                    return new UInt64DictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.Int64:
                    return new Int64DictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.Float:
                    return new FloatDictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.Double:
                    return new DoubleDictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.String:
                    return new StringDictionaryArray(data, uniqueValuesCount);
                case ArrowTypeId.Binary:
                case ArrowTypeId.Timestamp:
                case ArrowTypeId.List:
                case ArrowTypeId.Struct:
                case ArrowTypeId.Union:
                case ArrowTypeId.Date64:
                case ArrowTypeId.Date32:
                case ArrowTypeId.Decimal:
                case ArrowTypeId.Dictionary:
                case ArrowTypeId.FixedSizedBinary:
                case ArrowTypeId.HalfFloat:
                case ArrowTypeId.Interval:
                case ArrowTypeId.Map:
                case ArrowTypeId.Time32:
                case ArrowTypeId.Time64:
                case ArrowTypeId.Boolean:
                default:
                    throw new NotSupportedException($"An ArrowDictionaryArray cannot be built for type {data.DataType.TypeId}.");
            }

        }
    }
}
