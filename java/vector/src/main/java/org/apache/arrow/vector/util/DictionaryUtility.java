/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;


public class DictionaryUtility {

  /**
   * Convert field and child fields that have a dictionary encoding to message format, so fields
   * have the dictionary type
   *
   * NOTE: in the message format, fields have the dictionary type
   * in the memory format, they have the index type
   */
  public static Field toMessageFormat(Field field, DictionaryProvider provider, Set<Long> dictionaryIdsUsed) {
    DictionaryEncoding encoding = field.getDictionary();
    List<Field> children = field.getChildren();

    if (encoding == null && children.isEmpty()) {
      return field;
    }

    List<Field> updatedChildren = new ArrayList<>(children.size());
    for (Field child : children) {
      updatedChildren.add(toMessageFormat(child, provider, dictionaryIdsUsed));
    }

    ArrowType type;
    if (encoding == null) {
      type = field.getType();
    } else {
      long id = encoding.getId();
      Dictionary dictionary = provider.lookup(id);
      if (dictionary == null) {
        throw new IllegalArgumentException("Could not find dictionary with ID " + id);
      }
      type = dictionary.getVectorType();

      dictionaryIdsUsed.add(id);
    }

    return new Field(field.getName(), new FieldType(field.isNullable(), type, encoding, field.getMetadata()), updatedChildren);
  }

  /**
   * Convert field and child fields that have a dictionary encoding to message format, so fields
   * have the index type
   */
  public static Field toMemoryFormat(Field field, BufferAllocator allocator, Map<Long, Dictionary> dictionaries) {
    DictionaryEncoding encoding = field.getDictionary();
    List<Field> children = field.getChildren();

    if (encoding == null && children.isEmpty()) {
      return field;
    }

    List<Field> updatedChildren = new ArrayList<>(children.size());
    for (Field child : children) {
      updatedChildren.add(toMemoryFormat(child, allocator, dictionaries));
    }

    ArrowType type;
    if (encoding == null) {
      type = field.getType();
    } else {
      // re-type the field for in-memory format
      type = encoding.getIndexType();
      if (type == null) {
        type = new ArrowType.Int(32, true);
      }
      // get existing or create dictionary vector
      if (!dictionaries.containsKey(encoding.getId())) {
        // create a new dictionary vector for the values
        String dictName = "DICT" + encoding.getId();
        Field dictionaryField = new Field(dictName, new FieldType(false, field.getType(), null, null), children);
        FieldVector dictionaryVector = dictionaryField.createVector(allocator);
        dictionaries.put(encoding.getId(), new Dictionary(dictionaryVector, encoding));
      }
    }

    return new Field(field.getName(), new FieldType(field.isNullable(), type, encoding, field.getMetadata()), updatedChildren);
  }
}
