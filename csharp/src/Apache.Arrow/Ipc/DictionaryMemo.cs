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
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ipc
{
    class DictionaryMemo
    {
        private readonly Dictionary<long, IArrowArray> _idToDictionary;
        private readonly Dictionary<long, IArrowType> _idToValueType;
        private readonly Dictionary<Field, long> _fieldToId;

        public DictionaryMemo()
        {
            _idToDictionary = new Dictionary<long, IArrowArray>();
            _idToValueType = new Dictionary<long, IArrowType>();
            _fieldToId = new Dictionary<Field, long>();
        }

        public int DictionaryCount => _fieldToId.Count;
        public int LoadedDictionaryCount => _idToDictionary.Count;

        public IArrowType GetDictionaryType(long id)
        {
            if (!_idToValueType.TryGetValue(id, out IArrowType type))
            {
                throw new ArgumentException($"Dictionary with id {id} not found");
            }
            return type;
        }

        public IArrowArray GetDictionary(long id)
        {
            if (!_idToDictionary.TryGetValue(id, out IArrowArray dictionary))
            {
                throw new ArgumentException($"Dictionary with id {id} not found");
            }
            return dictionary;
        }

        public void AddField(long id, Field field)
        {
            if (_fieldToId.ContainsKey(field))
            {
                throw new ArgumentException($"Field {field.Name} is already in Memo");
            }

            if (field.DataType.TypeId != ArrowTypeId.Dictionary)
            {
                throw new ArgumentException($"Field type is not DictionaryType: Name={field.Name}, {field.DataType.Name}");
            }

            IArrowType valueType = ((DictionaryType)field.DataType).ValueType;

            if (_idToValueType.TryGetValue(id, out IArrowType valueTypeInDic))
            {
                if (valueType != valueTypeInDic)
                {
                    throw new ArgumentException($"Field type {field.DataType.Name} does not match the existing type {valueTypeInDic})");
                }
            }
            else
            {
                _idToValueType.Add(id, valueType);
            }

            _fieldToId.Add(field, id);
        }

        public long GetId(Field field)
        {
            if (!_fieldToId.TryGetValue(field, out long id))
            {
                throw new ArgumentException($"Field with name {field.Name} not found");
            }
            return id;
        }

        public long GetOrAssignId(Field field)
        {
            if (!_fieldToId.TryGetValue(field, out long id))
            {
                id = _fieldToId.Count;
                AddField(id, field);
            }
            return id;
        }

        public void AddOrReplaceDictionary(long id, IArrowArray dictionary)
        {
            _idToDictionary[id] = dictionary;
        }

        public void AddDeltaDictionary(long id, IArrowArray deltaDictionary, MemoryAllocator allocator = default)
        {
            IArrowArray currentDictionary = _idToDictionary[id];
            IArrowArray dictionary = ArrowArrayConcatenator.Concatenate(new List<IArrowArray>{ currentDictionary, deltaDictionary }, allocator);
            AddOrReplaceDictionary(id, dictionary);
        }
    }
}
