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

import java.io.Closeable;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.MurmurHasher;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * A dictionary implementation for continuous encoding of data in a dictionary and
 * index vector as opposed to the {@link Dictionary} that encodes a complete vector.
 * Supports delta or replacement encoding.
 */
public class BatchedDictionary implements Closeable, BaseDictionary {

  private final DictionaryEncoding encoding;

  private final BaseVariableWidthVector dictionary;

  private final BaseIntVector indexVector;

  private final DictionaryHashTable hashTable;

  private int deltaIndex;

  private int dictionaryIndex;

  private boolean oneTimeEncoding;

  private boolean wasReset;

  private boolean vectorsProvided;

  /**
   * Creates a dictionary and index vector with the respective types. The dictionary vector
   * will be named "{name}-dictionary".
   * <p>
   * To use this dictionary, provide the dictionary vector to a {@link DictionaryProvider},
   * add the {@link #getIndexVector()} to the {@link org.apache.arrow.vector.VectorSchemaRoot}
   * and call the {@link #setSafe(int, byte[], int, int)} or other set methods.
   *
   * @param name A name for the vector and dictionary.
   * @param encoding The dictionary encoding to use.
   * @param dictionaryType The type of the dictionary data.
   * @param indexType The type of the encoded dictionary index.
   * @param allocator The allocator to use.
   */
  public BatchedDictionary(
      String name,
      DictionaryEncoding encoding,
      ArrowType dictionaryType,
      ArrowType indexType,
      BufferAllocator allocator
  ) {
    this(name, encoding, dictionaryType, indexType, allocator, "-dictionary");
  }

  /**
   * Creates a dictionary index vector with the respective types.
   *
   * @param name A name for the vector and dictionary.
   * @param encoding The dictionary encoding to use.
   * @param dictionaryType The type of the dictionary data.
   * @param indexType The type of the encoded dictionary index.
   * @param allocator The allocator to use.
   * @param suffix A non-null suffix to append to the name of the dictionary.
   */
  public BatchedDictionary(
      String name,
      DictionaryEncoding encoding,
      ArrowType dictionaryType,
      ArrowType indexType,
      BufferAllocator allocator,
      String suffix
  ) {
    this(name, encoding, dictionaryType, indexType, allocator, suffix, false);
  }

  /**
   * Creates a dictionary index vector with the respective types.
   *
   * @param name A name for the vector and dictionary.
   * @param encoding The dictionary encoding to use.
   * @param dictionaryType The type of the dictionary data.
   * @param indexType The type of the encoded dictionary index.
   * @param allocator The allocator to use.
   * @param suffix A non-null suffix to append to the name of the dictionary.
   * @param oneTimeEncoding A mode where the entries can be added to the dictionary until
   *                        the first stream batch is written. After that, any new entries
   *                        to the dictionary will throw an exception.
   */
  public BatchedDictionary(
      String name,
      DictionaryEncoding encoding,
      ArrowType dictionaryType,
      ArrowType indexType,
      BufferAllocator allocator,
      String suffix,
      boolean oneTimeEncoding
  ) {
    this.encoding = encoding;
    this.oneTimeEncoding = oneTimeEncoding;
    if (dictionaryType.getTypeID() != ArrowType.ArrowTypeID.Utf8 &&
        dictionaryType.getTypeID() != ArrowType.ArrowTypeID.Binary) {
      throw new IllegalArgumentException("Dictionary must be a superclass of 'BaseVariableWidthVector' " +
          "such as 'VarCharVector'.");
    }
    if (indexType.getTypeID() != ArrowType.ArrowTypeID.Int) {
      throw new IllegalArgumentException("Index vector must be a superclass type of 'BaseIntVector' " +
          "such as 'IntVector' or 'Uint4Vector'.");
    }
    FieldVector vector = new FieldType(false, dictionaryType, null)
        .createNewSingleVector(name + suffix, allocator, null);
    dictionary = (BaseVariableWidthVector) vector;
    vector = new FieldType(true, indexType, encoding)
        .createNewSingleVector(name, allocator, null);
    indexVector = (BaseIntVector) vector;
    hashTable = new DictionaryHashTable();
  }

  /**
   * Creates a dictionary that will populate the provided vectors with data. Useful if
   * dictionaries need to be children of a parent vector.
   * <b>WARNING:</b> The caller is responsible for closing the provided vectors.
   *
   * @param dictionary The dictionary to hold the original data.
   * @param indexVector The index to store the encoded offsets.
   */
  public BatchedDictionary(
      FieldVector dictionary,
      FieldVector indexVector
  ) {
    this(dictionary, indexVector, false);
  }

  /**
   * Creates a dictionary that will populate the provided vectors with data. Useful if
   * dictionaries need to be children of a parent vector.
   * <b>WARNING:</b> The caller is responsible for closing the provided vectors.
   *
   * @param dictionary The dictionary to hold the original data.
   * @param indexVector The index to store the encoded offsets.
   * @param oneTimeEncoding A mode where the entries can be added to the dictionary until
   *                        the first stream batch is written. After that, any new entries
   *                        to the dictionary will throw an exception.
   */
  public BatchedDictionary(
      FieldVector dictionary,
      FieldVector indexVector,
      boolean oneTimeEncoding
  ) {
    this.encoding = dictionary.getField().getDictionary();
    this.oneTimeEncoding = oneTimeEncoding;
    vectorsProvided = true;
    if (!(BaseVariableWidthVector.class.isAssignableFrom(dictionary.getClass()))) {
      throw new IllegalArgumentException("Dictionary must be a superclass of 'BaseVariableWidthVector' " +
          "such as 'VarCharVector'.");
    }
    if (dictionary.getField().isNullable()) {
      throw new IllegalArgumentException("Dictionary must be non-nullable.");
    }
    this.dictionary = (BaseVariableWidthVector) dictionary;
    if (!(BaseIntVector.class.isAssignableFrom(indexVector.getClass()))) {
      throw new IllegalArgumentException("Index vector must be a superclass type of 'BaseIntVector' " +
          "such as 'IntVector' or 'Uint4Vector'.");
    }
    this.indexVector = (BaseIntVector) indexVector;
    hashTable = new DictionaryHashTable();
  }

  /**
   * The index vector.
   */
  public FieldVector getIndexVector() {
    return indexVector;
  }

  @Override
  public FieldVector getVector() {
    return dictionary;
  }

  @Override
  public ArrowType getVectorType() {
    return dictionary.getField().getType();
  }

  @Override
  public DictionaryEncoding getEncoding() {
    return encoding;
  }

  /**
   * Considers the entire byte array as the dictionary value. If the value is null,
   * a null will be written to the index.
   *
   * @param index the value to change
   * @param value the value to write.
   */
  public void setSafe(int index, byte[] value) {
    if (value == null) {
      setNull(index);
      return;
    }
    setSafe(index, value, 0, value.length);
  }

  /**
   * Encodes the given range in the dictionary. If the value is null, a null will be
   * written to the index.
   *
   * @param index the value to change
   * @param value the value to write.
   * @param offset An offset into the value array.
   * @param len The length of the value to write.
   */
  public void setSafe(int index, byte[] value, int offset, int len) {
    if (value == null || len == 0) {
      setNull(index);
      return;
    }
    int di = getIndex(value, offset, len);
    indexVector.setWithPossibleTruncate(index, di);
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index the value to change
   */
  public void setNull(int index) {
    indexVector.setNull(index);
  }

  @Override
  public void close() throws IOException {
    if (!vectorsProvided) {
      dictionary.close();
      indexVector.close();
    }
  }

  @Override
  public int mark() {
    dictionary.setValueCount(dictionaryIndex);
    // not setting the index vector value count. That will happen when the user calls
    // VectorSchemaRoot#setRowCount().
    if (wasReset && oneTimeEncoding && !encoding.isDelta()) {
      return 0;
    }
    return dictionaryIndex;
  }

  @Override
  public void reset() {
    wasReset = true;
    if (!oneTimeEncoding) {
      dictionaryIndex = 0;
      dictionary.reset();
    }
    indexVector.reset();
    if (!oneTimeEncoding && !encoding.isDelta()) {
      // replacement mode.
      deltaIndex = 0;
      hashTable.clear();
    }
  }

  private int getIndex(byte[] value, int offset, int len) {
    int hash = MurmurHasher.hashCode(value, offset, len, 0);
    int i = hashTable.getIndex(hash);
    if (i >= 0) {
      return i;
    } else {
      if (wasReset && oneTimeEncoding && !encoding.isDelta()) {
        throw new IllegalStateException("Dictionary was reset, not delta encoded and configured for onetime encoding.");
      }
      hashTable.addEntry(hash, deltaIndex);
      dictionary.setSafe(dictionaryIndex++, value, offset, len);
      return deltaIndex++;
    }
  }

  @VisibleForTesting
  DictionaryHashTable getHashTable() {
    return hashTable;
  }
}
