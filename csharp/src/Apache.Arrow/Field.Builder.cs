// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public partial class Field
    {
        public class Builder
        {
            private Dictionary<string, string> _metadata;
            private string _name;
            private IArrowType _type;
            private bool _nullable;

            public Builder()
            {
                _name = string.Empty;
                _type = NullType.Default;
                _nullable = true;
            }

            public Builder Name(string value)
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    throw new ArgumentNullException(nameof(value));
                }

                _name = value;
                return this;
            }

            public Builder DataType(IArrowType type)
            {
                _type = type ?? NullType.Default;
                return this;
            }

            public Builder DataType(Type valueType, string timezone = null)
            {
                Type[] genericArgs;

                switch (valueType)
                {
                    // Binary
                    case var _ when valueType == typeof(byte) || valueType == typeof(byte?)
                        || valueType == typeof(IEnumerable<byte>):
                        DataType(new BinaryType());
                        break;
                    // Boolean
                    case var _ when valueType == typeof(bool):
                        DataType(new BooleanType());
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(bool?):
                        DataType(new BooleanType());
                        break;
                    // String
                    case var _ when valueType == typeof(string):
                        DataType(new StringType());
                        break;
                    // Integers
                    case var _ when valueType == typeof(short):
                        DataType(new Int16Type());
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(short?):
                        DataType(new Int16Type());
                        break;
                    case var _ when valueType == typeof(int):
                        DataType(new Int32Type());
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(int?):
                        DataType(new Int32Type());
                        break;
                    case var _ when valueType == typeof(long):
                        DataType(new Int64Type());
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(long?):
                        DataType(new Int64Type());
                        break;
                    // Unisgned Integers
                    case var _ when valueType == typeof(ushort):
                        DataType(new UInt16Type());
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(ushort?):
                        DataType(new UInt16Type());
                        break;
                    case var _ when valueType == typeof(uint):
                        DataType(new UInt32Type());
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(uint?):
                        DataType(new UInt32Type());
                        break;
                    case var _ when valueType == typeof(ulong):
                        DataType(new UInt64Type());
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(ulong?):
                        DataType(new UInt64Type());
                        break;
                    // Decimal
                    case var _ when valueType == typeof(decimal):
                        DataType(new Decimal128Type(38, 18));
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(decimal?):
                        DataType(new Decimal128Type(38, 18));
                        break;
                    // Double
                    case var _ when valueType == typeof(double):
                        DataType(new DoubleType());
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(double?):
                        DataType(new DoubleType());
                        break;
                    // DateTime
                    case var _ when valueType == typeof(DateTime) || valueType == typeof(DateTimeOffset):
                        DataType(new TimestampType(TimeUnit.Nanosecond, timezone));
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(DateTime?) || valueType == typeof(DateTimeOffset?):
                        DataType(new TimestampType(TimeUnit.Nanosecond, timezone));
                        break;
                    // Time
                    case var _ when valueType == typeof(TimeSpan):
                        DataType(new Time64Type(TimeUnit.Nanosecond));
                        Nullable(false);
                        break;
                    case var _ when valueType == typeof(TimeSpan?):
                        DataType(new Time64Type(TimeUnit.Nanosecond));
                        break;
#if NETCOREAPP
                    // Dictionary: IDictionary<?,?>
                    case var dict when typeof(IDictionary).IsAssignableFrom(valueType):
                        genericArgs = dict.GetGenericArguments();

                        try
                        {
                            DataType(new DictionaryType(
                                new Builder().DataType(genericArgs[0], timezone)._type,
                                new Builder().DataType(genericArgs[1], timezone)._type,
                                false
                            ));
                        }
                        catch (ArgumentException)
                        {
                            // throw new ArgumentException($"{nameof(indexType)} must be integer");
                            DataType(new StructType(
                                new Field[]
                                {
                                    new Builder().Name("key").DataType(genericArgs[0], timezone).Build(),
                                    new Builder().Name("value").DataType(genericArgs[1], timezone).Build(),
                                }
                            ));
                        }

                        break;
                    // IEnumerable: List, Array, ...
                    case var list when typeof(IEnumerable).IsAssignableFrom(valueType):
                        Type elementType = list.GetGenericArguments()[0];

                        DataType(new ListType(new Builder().Name("item").DataType(elementType, timezone).Build()));
                        break;
                    // Struct like: get all properties
                    case var struct_ when (valueType.IsValueType && !valueType.IsEnum && !valueType.IsPrimitive):
                        FieldInfo[] fieldInfos = struct_.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

                        DataType(new StructType(
                            fieldInfos
                                .Select(field => new Builder().Name(field.Name).DataType(field.FieldType, timezone).Build())
                                .ToArray()
                        ));
                        break;
# endif
                    // Error
                    default:
                        throw new InvalidCastException($"Cannot convert System.Type<{valueType}> to ArrowType");
                }
                return this;
            }

            public Builder Nullable(bool value)
            {
                _nullable = value;
                return this;
            }

            public Builder Metadata(string key, string value)
            {
                if (string.IsNullOrWhiteSpace(key))
                {
                    throw new ArgumentNullException(nameof(key));
                }

                _metadata ??= new Dictionary<string, string>();

                _metadata[key] = value;
                return this;
            }

            public Builder Metadata(IEnumerable<KeyValuePair<string, string>> dictionary)
            {
                if (dictionary == null)
                {
                    throw new ArgumentNullException(nameof(dictionary));
                }
                foreach (KeyValuePair<string, string> entry in dictionary)
                {
                    Metadata(entry.Key, entry.Value);
                }
                return this;
            }

            public Field Build()
            {
                return new Field(_name, _type, _nullable, _metadata);
            }
        }
    }
}
