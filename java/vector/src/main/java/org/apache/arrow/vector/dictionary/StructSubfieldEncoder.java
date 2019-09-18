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

import static org.apache.arrow.vector.dictionary.DictionaryEncoder.cloneVector;
import static org.apache.arrow.vector.dictionary.DictionaryEncoder.getChildVector;
import static org.apache.arrow.vector.dictionary.DictionaryEncoder.getChildVectorDictionary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Sub fields encoder/decoder for Dictionary encoded {@link StructVector}.
 * Notes that child vectors within struct vector can either be dictionary encodeable or not.
 */
public class StructSubfieldEncoder {

  protected final BufferAllocator allocator;

  protected final DictionaryProvider.MapDictionaryProvider provider;
  protected final Map<Long, DictionaryHashTable> dictionaryIdToHashTable;

  /**
   * Construct an instance.
   */
  public StructSubfieldEncoder(
      BufferAllocator allocator,
      DictionaryProvider.MapDictionaryProvider provider) {

    this.allocator = allocator;
    this.provider = provider;

    this.dictionaryIdToHashTable = new HashMap<>();

    provider.getDictionaryIds().forEach(id ->
        dictionaryIdToHashTable.put(id, new DictionaryHashTable(provider.lookup(id).getVector())));
  }

  /**
   * Dictionary encodes subfields for complex vector with a provided dictionary.
   * The dictionary must contain all values in the sub fields vector.
   * @param vector vector to encode
   * @param columnToDictionaryId the mappings between child vector index and dictionary id. A null dictionary
   *        id indicates the child vector is not encodable.
   * @return dictionary encoded vector
   */
  public FieldVector encode(FieldVector vector, Map<Integer, Long> columnToDictionaryId) {
    checkVectorType(vector);
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
    FieldVector encoded = cloneVector(vector, allocator);
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
  }

  /**
   * Decodes a dictionary subfields encoded vector using the provided dictionary.
   * @param vector dictionary encoded vector, its child vector must be int type
   * @return vector with values restored from dictionary
   */
  public FieldVector decode(FieldVector vector) {
    checkVectorType(vector);

    final int valueCount = vector.getValueCount();
    final int childCount = vector.getChildrenFromFields().size();

    // clone list vector and initialize child vectors
    FieldVector decoded = cloneVector(vector, allocator);
    List<Field> childFields = new ArrayList<>();
    for (int i = 0; i < childCount; i++) {
      FieldVector childVector = getChildVector(vector, i);
      Dictionary dictionary = getChildVectorDictionary(provider, childVector);
      // childVector is not encoded.
      if (dictionary == null) {
        childFields.add(childVector.getField());
      } else {
        // union child vector name should always be MinorType.name
        Field field = Field.nullable(childVector.getField().getName(), dictionary.getVectorType());
        childFields.add(field);
      }
    }
    decoded.initializeChildrenFromFields(childFields);
    decoded.setValueCount(valueCount);

    for (int index = 0; index < childCount; index++) {
      // get child vector
      FieldVector childVector = getChildVector(vector, index);
      FieldVector decodedChildVector = getChildVector(decoded, index);
      Dictionary dictionary = getChildVectorDictionary(provider, childVector);
      if (dictionary == null) {
        childVector.makeTransferPair(decodedChildVector).splitAndTransfer(0, valueCount);
      } else {
        TransferPair transfer = dictionary.getVector().makeTransferPair(decodedChildVector);
        BaseIntVector indices = (BaseIntVector) childVector;

        DictionaryEncoder.retrieveIndexVector(indices, transfer, valueCount, 0, valueCount);
      }
    }

    return decoded;
  }

  protected void checkVectorType(FieldVector vector) {
    Preconditions.checkArgument(vector instanceof StructVector);
  }
}
