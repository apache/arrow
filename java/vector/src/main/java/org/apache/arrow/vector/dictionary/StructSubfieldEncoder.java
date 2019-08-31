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
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Sub fields encoder/decoder for Dictionary encoded {@link org.apache.arrow.vector.complex.NonNullableStructVector}.
 */
public class StructSubfieldEncoder {

  private final List<DictionaryHashTable> hashTables;
  private final Dictionary dictionary;
  private final BufferAllocator allocator;

  /**
   * Construct an instance.
   */
  public StructSubfieldEncoder(Dictionary dictionary, BufferAllocator allocator) {
    this.dictionary = dictionary;
    this.allocator = allocator;
    NonNullableStructVector dictVector = (NonNullableStructVector) dictionary.getVector();
    hashTables = new ArrayList<>();
    for (int i = 0; i < dictVector.getChildFieldNames().size(); i++) {
      hashTables.add(new DictionaryHashTable(getChildVector(dictVector, i)));
    }
  }

  private FieldVector getChildVector(NonNullableStructVector vector, int index) {
    return vector.getChildrenFromFields().get(index);
  }

  private NonNullableStructVector cloneVector(NonNullableStructVector vector) {

    final FieldType fieldType = vector.getField().getFieldType();
    NonNullableStructVector cloned = (NonNullableStructVector) fieldType.createNewSingleVector(
        vector.getField().getName(), allocator, null);

    if (vector instanceof StructVector) {
      final ArrowFieldNode fieldNode = new ArrowFieldNode(vector.getValueCount(), vector.getNullCount());
      ((StructVector) cloned).loadFieldBuffers(fieldNode, ((StructVector) vector).getFieldBuffers());
    }

    return cloned;
  }

  /**
   * Dictionary encodes subfields for complex vector with a provided dictionary.
   * The dictionary must contain all values in the sub fields vector.
   * @param vector vector to encode
   * @return dictionary encoded vector
   */
  public NonNullableStructVector encode(NonNullableStructVector vector) {
    final int valueCount = vector.getValueCount();
    final int childCount = vector.getChildrenFromFields().size();

    FieldType indexFieldType = new FieldType(vector.getField().isNullable(),
        dictionary.getEncoding().getIndexType(), dictionary.getEncoding(), vector.getField().getMetadata());

    List<Field> childrenFields = new ArrayList<>();
    for (int i = 0; i < childCount; i++) {
      childrenFields.add(new Field(vector.getChildrenFromFields().get(i).getField().getName(),
          indexFieldType,null));
    }

    // clone list vector and initialize data vector
    NonNullableStructVector encoded = cloneVector(vector);
    encoded.initializeChildrenFromFields(childrenFields);
    encoded.setValueCount(valueCount);

    for (int index = 0; index < childCount; index++) {
      BaseIntVector indices = (BaseIntVector) getChildVector(encoded, index);
      ValueVector dataVector = getChildVector(vector, index);

      DictionaryEncoder.buildIndexVector(dataVector, indices, hashTables.get(index), 0, valueCount);
    }

    return encoded;
  }

  /**
   * Decodes a dictionary subfields encoded vector using the provided dictionary.
   * @param vector dictionary encoded vector, its child vector must be int type
   * @return vector with values restored from dictionary
   */
  public NonNullableStructVector decode(NonNullableStructVector vector) {

    final int valueCount = vector.getValueCount();
    final int childCount = vector.getChildrenFromFields().size();

    NonNullableStructVector dictionaryVector = (NonNullableStructVector) dictionary.getVector();
    int dictionaryValueCount = getChildVector(dictionaryVector, 0).getValueCount();

    // clone list vector and initialize child vectors
    NonNullableStructVector decoded = cloneVector(vector);
    List<Field> childFields = new ArrayList<>();
    for (int i = 0; i < childCount; i++) {
      childFields.add(getChildVector(dictionaryVector, i).getField());
    }
    decoded.initializeChildrenFromFields(childFields);
    decoded.setValueCount(valueCount);

    for (int index = 0; index < childCount; index++) {
      // get child vector
      ValueVector dataVector = getChildVector(decoded, index);

      TransferPair transfer = getChildVector(dictionaryVector, index).makeTransferPair(dataVector);
      BaseIntVector indices = (BaseIntVector) getChildVector(vector, index);

      DictionaryEncoder.retrieveIndexVector(indices, transfer, dictionaryValueCount, 0, valueCount);
    }

    return decoded;
  }
}
