/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

/** Utility methods for working with Dictionaries used in Dictionary encodings. */
public class DictionaryUtility {
  private DictionaryUtility() {}

  /**
   * Convert field and child fields that have a dictionary encoding to message format, so fields
   * have the dictionary type.
   *
   * <p>NOTE: in the message format, fields have the dictionary type in the memory format, they have
   * the index type
   */
  public static Field toMessageFormat(
      Field field, DictionaryProvider provider, Set<Long> dictionaryIdsUsed) {
    if (!needConvertToMessageFormat(field)) {
      return field;
    }
    DictionaryEncoding encoding = field.getDictionary();
    List<Field> children;

    ArrowType type;
    if (encoding == null) {
      type = field.getType();
      children = field.getChildren();
    } else {
      long id = encoding.getId();
      Dictionary dictionary = provider.lookup(id);
      if (dictionary == null) {
        throw new IllegalArgumentException("Could not find dictionary with ID " + id);
      }
      type = dictionary.getVectorType();
      children = dictionary.getVector().getField().getChildren();

      dictionaryIdsUsed.add(id);
    }

    final List<Field> updatedChildren = new ArrayList<>(children.size());
    for (Field child : children) {
      updatedChildren.add(toMessageFormat(child, provider, dictionaryIdsUsed));
    }

    return new Field(
        field.getName(),
        new FieldType(field.isNullable(), type, encoding, field.getMetadata()),
        updatedChildren);
  }

  /**
   * Checks if it is required to convert the field to message format.
   *
   * @param field the field to check.
   * @return true if a conversion is required, and false otherwise.
   */
  public static boolean needConvertToMessageFormat(Field field) {
    DictionaryEncoding encoding = field.getDictionary();

    if (encoding != null) {
      // when encoding is not null, the type must be determined from the
      // dictionary, so conversion must be performed.
      return true;
    }

    List<Field> children = field.getChildren();
    for (Field child : children) {
      if (needConvertToMessageFormat(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Convert field and child fields that have a dictionary encoding to memory format, so fields have
   * the index type.
   */
  public static Field toMemoryFormat(
      Field field, BufferAllocator allocator, Map<Long, Dictionary> dictionaries) {
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
    List<Field> fieldChildren = null;
    if (encoding == null) {
      type = field.getType();
      fieldChildren = updatedChildren;
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
        Field dictionaryField =
            new Field(
                dictName,
                new FieldType(field.isNullable(), field.getType(), null, null),
                updatedChildren);
        FieldVector dictionaryVector = dictionaryField.createVector(allocator);
        dictionaries.put(encoding.getId(), new Dictionary(dictionaryVector, encoding));
      }
    }

    return new Field(
        field.getName(),
        new FieldType(field.isNullable(), type, encoding, field.getMetadata()),
        fieldChildren);
  }
}
