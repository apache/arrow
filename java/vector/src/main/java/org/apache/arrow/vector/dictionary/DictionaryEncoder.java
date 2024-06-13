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
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.memory.util.hash.SimpleHasher;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
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

  /** Construct an instance. */
  public DictionaryEncoder(Dictionary dictionary, BufferAllocator allocator) {
    this(dictionary, allocator, SimpleHasher.INSTANCE);
  }

  /** Construct an instance. */
  public DictionaryEncoder(
      Dictionary dictionary, BufferAllocator allocator, ArrowBufHasher hasher) {
    this.dictionary = dictionary;
    this.allocator = allocator;
    hashTable = new DictionaryHashTable(dictionary.getVector(), hasher);
  }

  /**
   * Dictionary encodes a vector with a provided dictionary. The dictionary must contain all values
   * in the vector.
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
    return decode(indices, dictionary, indices.getAllocator());
  }

  /**
   * Decodes a dictionary encoded array using the provided dictionary.
   *
   * @param indices dictionary encoded values, must be int type
   * @param dictionary dictionary used to decode the values
   * @param allocator allocator the decoded values use
   * @return vector with values restored from dictionary
   */
  public static ValueVector decode(
      ValueVector indices, Dictionary dictionary, BufferAllocator allocator) {
    int count = indices.getValueCount();
    ValueVector dictionaryVector = dictionary.getVector();
    int dictionaryCount = dictionaryVector.getValueCount();
    // copy the dictionary values into the decoded vector
    TransferPair transfer = dictionaryVector.getTransferPair(allocator);
    transfer.getTo().allocateNewSafe();
    try {
      BaseIntVector baseIntVector = (BaseIntVector) indices;
      retrieveIndexVector(baseIntVector, transfer, dictionaryCount, 0, count);
      ValueVector decoded = transfer.getTo();
      decoded.setValueCount(count);
      return decoded;
    } catch (Exception e) {
      AutoCloseables.close(e, transfer.getTo());
      throw e;
    }
  }

  /**
   * Get the indexType according to the dictionary vector valueCount.
   *
   * @param valueCount dictionary vector valueCount.
   * @return index type.
   */
  @SuppressWarnings("ComparisonOutOfRange")
  public static ArrowType.Int getIndexType(int valueCount) {
    Preconditions.checkArgument(valueCount >= 0);
    if (valueCount <= Byte.MAX_VALUE) {
      return new ArrowType.Int(8, true);
    } else if (valueCount <= Character.MAX_VALUE) {
      return new ArrowType.Int(16, true);
    } else if (valueCount <= Integer.MAX_VALUE) { // this comparison will always evaluate to true
      return new ArrowType.Int(32, true);
    } else {
      return new ArrowType.Int(64, true);
    }
  }

  /**
   * Populates indices between start and end with the encoded values of vector.
   *
   * @param vector the vector to encode
   * @param indices the index vector
   * @param encoding the hash table for encoding
   * @param start the start index
   * @param end the end index
   */
  static void buildIndexVector(
      ValueVector vector, BaseIntVector indices, DictionaryHashTable encoding, int start, int end) {

    for (int i = start; i < end; i++) {
      if (!vector.isNull(i)) {
        // if it's null leave it null
        // note: this may fail if value was not included in the dictionary
        int encoded = encoding.getIndex(i, vector);
        if (encoded == -1) {
          throw new IllegalArgumentException(
              "Dictionary encoding not defined for value:" + vector.getObject(i));
        }
        indices.setWithPossibleTruncate(i, encoded);
      }
    }
  }

  /**
   * Retrieve values to target vector from index vector.
   *
   * @param indices the index vector
   * @param transfer the {@link TransferPair} to copy dictionary data into target vector.
   * @param dictionaryCount the value count of dictionary vector.
   * @param start the start index
   * @param end the end index
   */
  static void retrieveIndexVector(
      BaseIntVector indices, TransferPair transfer, int dictionaryCount, int start, int end) {
    for (int i = start; i < end; i++) {
      if (!indices.isNull(i)) {
        int indexAsInt = (int) indices.getValueAsLong(i);
        if (indexAsInt > dictionaryCount) {
          throw new IllegalArgumentException(
              "Provided dictionary does not contain value for index " + indexAsInt);
        }
        transfer.copyValueSafe(indexAsInt, i);
      }
    }
  }

  /** Encodes a vector with the built hash table in this encoder. */
  public ValueVector encode(ValueVector vector) {

    Field valueField = vector.getField();
    FieldType indexFieldType =
        new FieldType(
            valueField.isNullable(),
            dictionary.getEncoding().getIndexType(),
            dictionary.getEncoding(),
            valueField.getMetadata());
    Field indexField = new Field(valueField.getName(), indexFieldType, null);

    // vector to hold our indices (dictionary encoded values)
    FieldVector createdVector = indexField.createVector(allocator);
    if (!(createdVector instanceof BaseIntVector)) {
      throw new IllegalArgumentException(
          "Dictionary encoding does not have a valid int type:" + createdVector.getClass());
    }

    BaseIntVector indices = (BaseIntVector) createdVector;
    indices.allocateNew();
    try {
      buildIndexVector(vector, indices, hashTable, 0, vector.getValueCount());
      indices.setValueCount(vector.getValueCount());
      return indices;
    } catch (Exception e) {
      AutoCloseables.close(e, indices);
      throw e;
    }
  }

  /**
   * Decodes a vector with the dictionary in this encoder.
   *
   * <p>{@link DictionaryEncoder#decode(ValueVector, Dictionary, BufferAllocator)} should be used
   * instead if only decoding is required as it can avoid building the {@link DictionaryHashTable}
   * which only makes sense when encoding.
   */
  public ValueVector decode(ValueVector indices) {
    return decode(indices, dictionary, allocator);
  }
}
