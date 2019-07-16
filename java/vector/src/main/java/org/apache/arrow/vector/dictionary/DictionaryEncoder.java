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

import org.apache.arrow.vector.BaseBinaryVector;
import org.apache.arrow.vector.BaseIntVector;
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
    DictionaryEncodeHashMap<Object> lookUps = new DictionaryEncodeHashMap<>(dictionary.getVector().getValueCount());

    boolean binaryType = isBinaryType(vector.getMinorType());

    for (int i = 0; i < dictionary.getVector().getValueCount(); i++) {
      Object key = binaryType ? ((BaseBinaryVector) dictionary.getVector()).getByteArrayWrapper(i) :
          dictionary.getVector().getObject(i);
      lookUps.put(key, i);
    }

    Field valueField = vector.getField();
    FieldType indexFieldType = new FieldType(valueField.isNullable(), dictionary.getEncoding().getIndexType(),
        dictionary.getEncoding(), valueField.getMetadata());
    Field indexField = new Field(valueField.getName(), indexFieldType, null);

    // vector to hold our indices (dictionary encoded values)
    FieldVector createdVector = indexField.createVector(vector.getAllocator());
    if (! (createdVector instanceof BaseIntVector)) {
      throw new IllegalArgumentException("Dictionary encoding does not have a valid int type:" +
          createdVector.getClass());
    }

    BaseIntVector indices = (BaseIntVector) createdVector;
    indices.allocateNew();

    int count = vector.getValueCount();

    for (int i = 0; i < count; i++) {
      Object value = binaryType ? ((BaseBinaryVector) vector).getByteArrayWrapper(i) : vector.getObject(i);
      if (value != null) { // if it's null leave it null
        // note: this may fail if value was not included in the dictionary
        int encoded = lookUps.get(value);
        if (encoded == -1) {
          throw new IllegalArgumentException("Dictionary encoding not defined for value:" + value);
        }
        indices.setWithPossibleTruncate(i, encoded);
      }
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

  private static boolean isBinaryType(MinorType type) {
    if (type == MinorType.VARBINARY || type == MinorType.FIXEDSIZEBINARY) {
      return true;
    }
    return false;
  }

  private static void validateType(MinorType type) {
    if (type == MinorType.UNION) {
      throw new IllegalArgumentException("Dictionary encoding not implemented for current type: " + type);
    }
  }
}
