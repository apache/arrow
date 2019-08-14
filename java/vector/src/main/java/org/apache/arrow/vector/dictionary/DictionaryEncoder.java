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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Encoder/decoder for Dictionary encoded {@link ValueVector}. Dictionary encoding produces an
 * integer {@link ValueVector}. Each entry in the Vector is index into the dictionary which can hold
 * values of any type.
 */
public class DictionaryEncoder {

  private final DictionaryHashTable hashTable;
  private final Dictionary dictionary;
  private final BufferAllocator allocator;

  //TODO pass custom hasher.

  /**
   * Construct an instance.
   */
  public DictionaryEncoder(Dictionary dictionary, BufferAllocator allocator) {
    this.dictionary = dictionary;
    this.allocator = allocator;
    hashTable = new DictionaryHashTable(dictionary.getVector());
  }

  /**
   * Dictionary encodes a vector with a provided dictionary. The dictionary must contain all values in the vector.
   *
   * @param vector vector to encode
   * @param dictionary dictionary used for encoding
   * @return dictionary encoded vector
   */
  public static ValueVector encode(ValueVector vector, Dictionary dictionary) {
    DictionaryEncoder encoder = new DictionaryEncoder(dictionary, vector.getAllocator());
    return encoder.encode(vector);
  }

  /**
   * Decodes a dictionary encoded array using the provided dictionary.
   *
   * @param indices dictionary encoded values, must be int type
   * @param dictionary dictionary used to decode the values
   * @return vector with values restored from dictionary
   */
  public static ValueVector decode(ValueVector indices, Dictionary dictionary) {
    DictionaryEncoder encoder = new DictionaryEncoder(dictionary, indices.getAllocator());
    return encoder.decode(indices);
  }

  /**
   * Encodes a vector with the built hash table in this encoder.
   */
  public ValueVector encode(ValueVector vector) {

    Field valueField = vector.getField();
    FieldType indexFieldType = new FieldType(valueField.isNullable(), dictionary.getEncoding().getIndexType(),
        dictionary.getEncoding(), valueField.getMetadata());
    Field indexField = new Field(valueField.getName(), indexFieldType, null);

    // vector to hold our indices (dictionary encoded values)
    FieldVector createdVector = indexField.createVector(allocator);
    if (! (createdVector instanceof BaseIntVector)) {
      throw new IllegalArgumentException("Dictionary encoding does not have a valid int type:" +
          createdVector.getClass());
    }

    BaseIntVector indices = (BaseIntVector) createdVector;
    indices.allocateNew();

    int count = vector.getValueCount();

    for (int i = 0; i < count; i++) {
      if (!vector.isNull(i)) { // if it's null leave it null
        // note: this may fail if value was not included in the dictionary
        //int encoded = lookUps.get(value);
        int encoded = hashTable.getIndex(i, vector);
        if (encoded == -1) {
          throw new IllegalArgumentException("Dictionary encoding not defined for value:" + vector.getObject(i));
        }
        indices.setWithPossibleTruncate(i, encoded);
      }
    }

    indices.setValueCount(count);

    return indices;
  }

  /**
   * Decodes a vector with the built hash table in this encoder.
   */
  public ValueVector decode(ValueVector indices) {
    int count = indices.getValueCount();
    ValueVector dictionaryVector = dictionary.getVector();
    int dictionaryCount = dictionaryVector.getValueCount();
    // copy the dictionary values into the decoded vector
    TransferPair transfer = dictionaryVector.getTransferPair(allocator);
    transfer.getTo().allocateNewSafe();

    BaseIntVector baseIntVector = (BaseIntVector) indices;
    for (int i = 0; i < count; i++) {
      if (!baseIntVector.isNull(i)) {
        int indexAsInt = (int) baseIntVector.getValueAsLong(i);
        if (indexAsInt > dictionaryCount) {
          throw new IllegalArgumentException("Provided dictionary does not contain value for index " + indexAsInt);
        }
        transfer.copyValueSafe(indexAsInt, i);
      }
    }
    ValueVector decoded = transfer.getTo();
    decoded.setValueCount(count);
    return decoded;
  }
}
