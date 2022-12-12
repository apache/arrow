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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.memory.util.hash.SimpleHasher;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Sub fields encoder/decoder for Dictionary encoded {@link StructVector}.
 * Notes that child vectors within struct vector can either be dictionary encodable or not.
 */
public class StructSubfieldEncoder {

  private final BufferAllocator allocator;

  private final DictionaryProvider.MapDictionaryProvider provider;
  private final Map<Long, DictionaryHashTable> dictionaryIdToHashTable;

  /**
   * Construct an instance.
   */
  public StructSubfieldEncoder(BufferAllocator allocator, DictionaryProvider.MapDictionaryProvider provider) {
    this (allocator, provider, SimpleHasher.INSTANCE);
  }

  /**
   * Construct an instance.
   */
  public StructSubfieldEncoder(
      BufferAllocator allocator,
      DictionaryProvider.MapDictionaryProvider provider,
      ArrowBufHasher hasher) {

    this.allocator = allocator;
    this.provider = provider;

    this.dictionaryIdToHashTable = new HashMap<>();

    provider.getDictionaryIds().forEach(id ->
        dictionaryIdToHashTable.put(id, new DictionaryHashTable(provider.lookup(id).getVector(), hasher)));
  }

  private static FieldVector getChildVector(StructVector vector, int index) {
    return vector.getChildrenFromFields().get(index);
  }

  private static StructVector cloneVector(StructVector vector, BufferAllocator allocator) {

    final FieldType fieldType = vector.getField().getFieldType();
    StructVector cloned = (StructVector) fieldType.createNewSingleVector(
        vector.getField().getName(), allocator, /*schemaCallback=*/null);

    final ArrowFieldNode fieldNode = new ArrowFieldNode(vector.getValueCount(), vector.getNullCount());
    cloned.loadFieldBuffers(fieldNode, vector.getFieldBuffers());

    return cloned;
  }

  /**
   * Dictionary encodes subfields for complex vector with a provided dictionary.
   * The dictionary must contain all values in the sub fields vector.
   * @param vector vector to encode
   * @param columnToDictionaryId the mappings between child vector index and dictionary id. A null dictionary
   *        id indicates the child vector is not encodable.
   * @return dictionary encoded vector
   */
  public StructVector encode(StructVector vector, Map<Integer, Long> columnToDictionaryId) {
    final int valueCount = vector.getValueCount();
    final int childCount = vector.getChildrenFromFields().size();

    List<Field> childrenFields = new ArrayList<>();

    // initialize child fields.
    for (int i = 0; i < childCount; i++) {
      FieldVector childVector = getChildVector(vector, i);
      Long dictionaryId = columnToDictionaryId.get(i);
      // A null dictionaryId indicates the child vector shouldn't be encoded.
      if (dictionaryId == null) {
        childrenFields.add(childVector.getField());
      } else {
        Dictionary dictionary = provider.lookup(dictionaryId);
        Preconditions.checkNotNull(dictionary, "Dictionary not found with id:" + dictionaryId);
        FieldType indexFieldType = new FieldType(childVector.getField().isNullable(),
            dictionary.getEncoding().getIndexType(), dictionary.getEncoding());
        childrenFields.add(new Field(childVector.getField().getName(), indexFieldType, /*children=*/null));
      }
    }

    // clone list vector and initialize data vector
    StructVector encoded = cloneVector(vector, allocator);
    try {
      encoded.initializeChildrenFromFields(childrenFields);
      encoded.setValueCount(valueCount);

      for (int index = 0; index < childCount; index++) {
        FieldVector childVector = getChildVector(vector, index);
        FieldVector encodedChildVector = getChildVector(encoded, index);
        Long dictionaryId = columnToDictionaryId.get(index);
        if (dictionaryId != null) {
          BaseIntVector indices = (BaseIntVector) encodedChildVector;
          DictionaryEncoder.buildIndexVector(childVector, indices, dictionaryIdToHashTable.get(dictionaryId),
                  0, valueCount);
        } else {
          childVector.makeTransferPair(encodedChildVector).splitAndTransfer(0, valueCount);
        }
      }

      return encoded;
    } catch (Exception e) {
      AutoCloseables.close(e, encoded);
      throw e;
    }
  }

  /**
   * Decodes a dictionary subfields encoded vector using the provided dictionary.
   *
   * {@link StructSubfieldEncoder#decode(StructVector, DictionaryProvider.MapDictionaryProvider, BufferAllocator)}
   * should be used instead if only decoding is required as it can avoid building the {@link DictionaryHashTable}
   * which only makes sense when encoding.
   *
   * @param vector dictionary encoded vector, its child vector must be int type
   * @return vector with values restored from dictionary
   */
  public StructVector decode(StructVector vector) {
    return decode(vector, provider, allocator);
  }

  /**
   * Decodes a dictionary subfields encoded vector using the provided dictionary.
   *
   * @param vector dictionary encoded vector, its data vector must be int type
   * @param provider  dictionary provider used to decode the values
   * @param allocator allocator the decoded values use
   * @return vector with values restored from dictionary
   */
  public static StructVector decode(StructVector vector,
                                    DictionaryProvider.MapDictionaryProvider provider,
                                    BufferAllocator allocator) {
    final int valueCount = vector.getValueCount();
    final int childCount = vector.getChildrenFromFields().size();

    // clone list vector and initialize child vectors
    StructVector decoded = cloneVector(vector, allocator);
    try {
      List<Field> childFields = new ArrayList<>();
      for (int i = 0; i < childCount; i++) {
        FieldVector childVector = getChildVector(vector, i);
        Dictionary dictionary = getChildVectorDictionary(childVector, provider);
        // childVector is not encoded.
        if (dictionary == null) {
          childFields.add(childVector.getField());
        } else {
          childFields.add(dictionary.getVector().getField());
        }
      }
      decoded.initializeChildrenFromFields(childFields);
      decoded.setValueCount(valueCount);

      for (int index = 0; index < childCount; index++) {
        // get child vector
        FieldVector childVector = getChildVector(vector, index);
        FieldVector decodedChildVector = getChildVector(decoded, index);
        Dictionary dictionary = getChildVectorDictionary(childVector, provider);
        if (dictionary == null) {
          childVector.makeTransferPair(decodedChildVector).splitAndTransfer(0, valueCount);
        } else {
          TransferPair transfer = dictionary.getVector().makeTransferPair(decodedChildVector);
          BaseIntVector indices = (BaseIntVector) childVector;

          DictionaryEncoder.retrieveIndexVector(indices, transfer, valueCount, 0, valueCount);
        }
      }

      return decoded;
    } catch (Exception e) {
      AutoCloseables.close(e, decoded);
      throw e;
    }
  }

  /**
   * Get the child vector dictionary, return null if not dictionary encoded.
   */
  private static Dictionary getChildVectorDictionary(FieldVector childVector,
                                                     DictionaryProvider.MapDictionaryProvider provider) {
    DictionaryEncoding dictionaryEncoding = childVector.getField().getDictionary();
    if (dictionaryEncoding != null) {
      Dictionary dictionary = provider.lookup(dictionaryEncoding.getId());
      Preconditions.checkNotNull(dictionary, "Dictionary not found with id:" + dictionary);
      return dictionary;
    }
    return null;
  }
}
