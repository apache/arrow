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

import java.util.Collections;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Sub fields encoder/decoder for Dictionary encoded {@link BaseListVector}.
 */
public class ListSubfieldEncoder {

  private final DictionaryHashTable hashTable;
  private final Dictionary dictionary;
  private final BufferAllocator allocator;

  /**
   * Construct an instance.
   */
  public ListSubfieldEncoder(Dictionary dictionary, BufferAllocator allocator) {
    this.dictionary = dictionary;
    this.allocator = allocator;
    BaseListVector dictVector = (BaseListVector) dictionary.getVector();
    hashTable = new DictionaryHashTable(getDataVector(dictVector));
  }

  private FieldVector getDataVector(BaseListVector vector) {
    return vector.getChildrenFromFields().get(0);
  }

  private BaseListVector cloneVector(BaseListVector vector) {

    final FieldType fieldType = vector.getField().getFieldType();
    BaseListVector cloned = (BaseListVector) fieldType.createNewSingleVector(vector.getField().getName(),
        allocator, /*schemaCallBack=*/null);

    final ArrowFieldNode fieldNode = new ArrowFieldNode(vector.getValueCount(), vector.getNullCount());
    cloned.loadFieldBuffers(fieldNode, vector.getFieldBuffers());

    return cloned;
  }

  /**
   * Dictionary encodes subfields for complex vector with a provided dictionary.
   * The dictionary must contain all values in the sub fields vector.
   * @param vector vector to encode
   * @return dictionary encoded vector
   */
  public BaseListVector encodeListSubField(BaseListVector vector) {
    final int valueCount = vector.getValueCount();

    FieldType indexFieldType = new FieldType(vector.getField().isNullable(),
        dictionary.getEncoding().getIndexType(), dictionary.getEncoding(), vector.getField().getMetadata());
    Field valueField = new Field(vector.getField().getName(), indexFieldType,null);

    // clone list vector and initialize data vector
    BaseListVector encoded = cloneVector(vector);
    encoded.initializeChildrenFromFields(Collections.singletonList(valueField));
    BaseIntVector indices = (BaseIntVector) getDataVector(encoded);

    ValueVector dataVector = getDataVector(vector);
    for (int i = 0; i < valueCount; i++) {
      if (!vector.isNull(i)) {
        int start = vector.getElementStartIndex(i);
        int end = vector.getElementEndIndex(i);

        DictionaryEncoder.buildIndexVector(dataVector, indices, hashTable, start, end);
      }
    }

    return encoded;
  }

  /**
   * Decodes a dictionary subfields encoded vector using the provided dictionary.
   * @param vector dictionary encoded vector, its data vector must be int type
   * @return vector with values restored from dictionary
   */
  public BaseListVector decodeListSubField(BaseListVector vector) {

    int valueCount = vector.getValueCount();
    BaseListVector dictionaryVector = (BaseListVector) dictionary.getVector();
    int dictionaryValueCount = getDataVector(dictionaryVector).getValueCount();

    // clone list vector and initialize data vector
    BaseListVector decoded = cloneVector(vector);
    Field dataVectorField = getDataVector(dictionaryVector).getField();
    decoded.initializeChildrenFromFields(Collections.singletonList(dataVectorField));

    // get data vector
    ValueVector dataVector = getDataVector(decoded);

    TransferPair transfer = getDataVector(dictionaryVector).makeTransferPair(dataVector);
    BaseIntVector indices = (BaseIntVector) getDataVector(vector);

    for (int i = 0; i < valueCount; i++) {

      if (!vector.isNull(i)) {
        int start = vector.getElementStartIndex(i);
        int end = vector.getElementEndIndex(i);

        DictionaryEncoder.retrieveIndexVector(indices, transfer, dictionaryValueCount, start, end);
      }
    }
    return decoded;
  }
}
