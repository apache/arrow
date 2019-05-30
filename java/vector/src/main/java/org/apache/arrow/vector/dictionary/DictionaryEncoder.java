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

package org.apache.arrow.vector.dictionary;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Encoder/decoder for Dictionary encoded {@link ValueVector}. Dictionary encoding produces an
 * integer {@link ValueVector}. Each entry in the Vector is index into the dictionary which can hold
 * values of any type.
 */
public class DictionaryEncoder {

  // TODO recursively examine fields?

  /**
   * Dictionary encodes a vector with a provided dictionary. The dictionary must contain all values in the vector.
   *
   * @param vector     vector to encode
   * @param dictionary dictionary used for encoding
   * @return dictionary encoded vector
   */
  public static ValueVector encode(ValueVector vector, Dictionary dictionary) {
    validateType(vector.getMinorType());
    // load dictionary values into a hashmap for lookup
    Map<Object, Integer> lookUps = new HashMap<>(dictionary.getVector().getValueCount());
    for (int i = 0; i < dictionary.getVector().getValueCount(); i++) {
      // for primitive array types we need a wrapper that implements equals and hashcode appropriately
      lookUps.put(dictionary.getVector().getObject(i), i);
    }

    Field valueField = vector.getField();
    FieldType indexFieldType = new FieldType(valueField.isNullable(), dictionary.getEncoding().getIndexType(),
        dictionary.getEncoding(), valueField.getMetadata());
    Field indexField = new Field(valueField.getName(), indexFieldType, null);

    // vector to hold our indices (dictionary encoded values)
    FieldVector indices = indexField.createVector(vector.getAllocator());

    // use reflection to pull out the set method
    // TODO implement a common interface for int vectors
    Method setter = null;
    for (Class<?> c : Arrays.asList(int.class, long.class)) {
      try {
        setter = indices.getClass().getMethod("setSafe", int.class, c);
        break;
      } catch (NoSuchMethodException e) {
        // ignore
      }
    }
    if (setter == null) {
      throw new IllegalArgumentException("Dictionary encoding does not have a valid int type:" + indices.getClass());
    }

    int count = vector.getValueCount();

    indices.allocateNew();

    try {
      for (int i = 0; i < count; i++) {
        Object value = vector.getObject(i);
        if (value != null) { // if it's null leave it null
          // note: this may fail if value was not included in the dictionary
          Object encoded = lookUps.get(value);
          if (encoded == null) {
            throw new IllegalArgumentException("Dictionary encoding not defined for value:" + value);
          }
          setter.invoke(indices, i, encoded);
        }
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException("IllegalAccessException invoking vector mutator set():", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("InvocationTargetException invoking vector mutator set():", e.getCause());
    }

    indices.setValueCount(count);

    return indices;
  }

  /**
   * Decodes a dictionary encoded array using the provided dictionary.
   *
   * @param indices    dictionary encoded values, must be int type
   * @param dictionary dictionary used to decode the values
   * @return vector with values restored from dictionary
   */
  public static ValueVector decode(ValueVector indices, Dictionary dictionary) {
    int count = indices.getValueCount();
    ValueVector dictionaryVector = dictionary.getVector();
    int dictionaryCount = dictionaryVector.getValueCount();
    // copy the dictionary values into the decoded vector
    TransferPair transfer = dictionaryVector.getTransferPair(indices.getAllocator());
    transfer.getTo().allocateNewSafe();
    for (int i = 0; i < count; i++) {
      Object index = indices.getObject(i);
      if (index != null) {
        int indexAsInt = ((Number) index).intValue();
        if (indexAsInt > dictionaryCount) {
          throw new IllegalArgumentException("Provided dictionary does not contain value for index " + indexAsInt);
        }
        transfer.copyValueSafe(indexAsInt, i);
      }
    }
    // TODO do we need to worry about the field?
    ValueVector decoded = transfer.getTo();
    decoded.setValueCount(count);
    return decoded;
  }

  private static void validateType(MinorType type) {
    // byte arrays don't work as keys in our dictionary map - we could wrap them with something to
    // implement equals and hashcode if we want that functionality
    if (type == MinorType.VARBINARY || type == MinorType.FIXEDSIZEBINARY || type == MinorType.LIST ||
        type == MinorType.STRUCT || type == MinorType.UNION) {
      throw new IllegalArgumentException("Dictionary encoding for complex types not implemented: type " + type);
    }
  }
}
