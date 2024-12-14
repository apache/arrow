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

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Apache.Arrow
{
    public partial class Schema : IRecordType
    {
        [Obsolete("Use `FieldsList` or `FieldsLookup` instead")]
        public IReadOnlyDictionary<string, Field> Fields => _fieldsDictionary;
        private readonly Dictionary<string, Field> _fieldsDictionary;

        public IReadOnlyList<Field> FieldsList => _fieldsList;
        private readonly List<Field> _fieldsList;

        public ILookup<string, Field> FieldsLookup { get; }
        private readonly ILookup<string, int> _fieldsIndexLookup;

        public IReadOnlyDictionary<string, string> Metadata { get; }

        public bool HasMetadata => Metadata != null && Metadata.Count > 0;

        public Field this[int index] => GetFieldByIndex(index);

        public Field this[string name] => GetFieldByName(name);

        public Schema(
            IEnumerable<Field> fields,
            IEnumerable<KeyValuePair<string, string>> metadata)
            : this(
                fields?.ToList() ?? throw new ArgumentNullException(nameof(fields)),
                metadata?.ToDictionary(kv => kv.Key, kv => kv.Value),
                false)
        {
        }

        internal Schema(List<Field> fieldsList, IReadOnlyDictionary<string, string> metadata, bool copyCollections)
        {
            Debug.Assert(fieldsList != null);
            Debug.Assert(copyCollections == false, "This internal constructor is to not copy the collections.");

            _fieldsList = fieldsList;
            FieldsLookup = _fieldsList.ToLookup(f => f.Name);
            _fieldsDictionary = FieldsLookup.ToDictionary(g => g.Key, g => g.First());

            Metadata = metadata;

            _fieldsIndexLookup = _fieldsList
                .Select((x, idx) => (Name: x.Name, Index: idx))
                .ToLookup(x => x.Name, x => x.Index, StringComparer.CurrentCulture);
        }

        public Field GetFieldByIndex(int i) => _fieldsList[i];

        public Field GetFieldByName(string name) => FieldsLookup[name].FirstOrDefault();

        public int GetFieldIndex(string name, StringComparer comparer)
        {
            IEqualityComparer<string> equalityComparer = (IEqualityComparer<string>)comparer;
            return GetFieldIndex(name, equalityComparer);
        }

        public int GetFieldIndex(string name, IEqualityComparer<string> comparer = default)
        {
            if (comparer == null || comparer.Equals(StringComparer.CurrentCulture))
            {
                return _fieldsIndexLookup[name].First();
            }

            for (var i = 0; i < _fieldsList.Count; ++i)
            {
                if (comparer.Equals(_fieldsList[i].Name, name))
                {
                    return i;
                }
            }

            throw new InvalidOperationException();
        }

        public Schema RemoveField(int fieldIndex)
        {
            if (fieldIndex < 0 || fieldIndex >= _fieldsList.Count)
            {
                throw new ArgumentException("Invalid fieldIndex", nameof(fieldIndex));
            }

            IList<Field> fields = Utility.DeleteListElement(_fieldsList, fieldIndex);

            return new Schema(fields, Metadata);
        }

        public Schema InsertField(int fieldIndex, Field newField)
        {
            newField = newField ?? throw new ArgumentNullException(nameof(newField));
            if (fieldIndex < 0 || fieldIndex > _fieldsList.Count)
            {
                throw new ArgumentException(nameof(fieldIndex), $"Invalid fieldIndex {fieldIndex} passed in to Schema.AddField");
            }

            IList<Field> fields = Utility.AddListElement(_fieldsList, fieldIndex, newField);

            return new Schema(fields, Metadata);
        }

        public Schema SetField(int fieldIndex, Field newField)
        {
            if (fieldIndex < 0 || fieldIndex >= _fieldsList.Count)
            {
                throw new ArgumentException($"Invalid fieldIndex {fieldIndex} passed in to Schema.SetColumn");
            }

            IList<Field> fields = Utility.SetListElement(_fieldsList, fieldIndex, newField);

            return new Schema(fields, Metadata);
        }

        public void Accept(IArrowTypeVisitor visitor)
        {
            if (visitor is IArrowTypeVisitor<Schema> schemaVisitor)
            {
                schemaVisitor.Visit(this);
            }
            else if (visitor is IArrowTypeVisitor<IRecordType> interfaceVisitor)
            {
                interfaceVisitor.Visit(this);
            }
            else
            {
                visitor.Visit(this);
            }
        }

        public override string ToString() => $"{nameof(Schema)}: Num fields={_fieldsList.Count}, Num metadata={Metadata?.Count ?? 0}";

        int IRecordType.FieldCount => _fieldsList.Count;
        string IArrowType.Name => "RecordBatch";
        ArrowTypeId IArrowType.TypeId => ArrowTypeId.RecordBatch;
        bool IArrowType.IsFixedWidth => false;
    }
}
