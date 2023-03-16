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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Apache.Arrow
{
    public partial class Schema
    {
        [Obsolete("Use `FieldsList` or `FieldsLookup` instead")]
        public IReadOnlyDictionary<string, Field> Fields => _fieldsDictionary;
        private readonly Dictionary<string, Field> _fieldsDictionary;

        public IReadOnlyList<Field> FieldsList => _fieldsList;
        private readonly List<Field> _fieldsList;

        public ILookup<string, Field> FieldsLookup { get; }

        public IReadOnlyDictionary<string, string> Metadata { get; }

        public bool HasMetadata => Metadata != null && Metadata.Count > 0;

        public Field this[int index] => GetFieldByIndex(index);

        public Field this[string name] => GetFieldByName(name);

        public Schema(
            IEnumerable<Field> fields,
            IEnumerable<KeyValuePair<string, string>> metadata)
        {
            if (fields is null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            _fieldsList = fields.ToList();
            FieldsLookup = _fieldsList.ToLookup(f => f.Name);
            _fieldsDictionary = FieldsLookup.ToDictionary(g => g.Key, g => g.First());

            Metadata = metadata?.ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        internal Schema(List<Field> fieldsList, IReadOnlyDictionary<string, string> metadata, bool copyCollections)
        {
            Debug.Assert(fieldsList != null);
            Debug.Assert(copyCollections == false, "This internal constructor is to not copy the collections.");

            _fieldsList = fieldsList;
            FieldsLookup = _fieldsList.ToLookup(f => f.Name);
            _fieldsDictionary = FieldsLookup.ToDictionary(g => g.Key, g => g.First());

            Metadata = metadata;
        }

        public Field GetFieldByIndex(int i) => _fieldsList[i];

        public Field GetFieldByName(string name) => FieldsLookup[name].FirstOrDefault();

        public int GetFieldIndex(string name, StringComparer comparer = default)
        {
            comparer ??= StringComparer.CurrentCulture;

            return _fieldsList.IndexOf(_fieldsList.First(x => comparer.Compare(x.Name, name) == 0));
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
    }
}
