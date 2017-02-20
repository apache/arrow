/*******************************************************************************

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.arrow.vector.complex;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.Dictionary;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DictionaryVector implements FieldVector {

  private final FieldVector indices;
  private final Dictionary dictionary;

  public DictionaryVector(FieldVector indices, Dictionary dictionary) {
    this.indices = indices;
    this.dictionary = dictionary;
  }

  /**
   * Dictionary encodes a vector with a provided dictionary. The dictionary must contain all values in the vector.
   *
   * @param vector vector to encode
   * @param dictionary dictionary used for encoding
   * @return dictionary encoded vector
   */
  public static DictionaryVector encode(ValueVector vector, Dictionary dictionary) {
    validateType(vector.getMinorType());
    // load dictionary values into a hashmap for lookup
    ValueVector.Accessor dictionaryAccessor = dictionary.getVector().getAccessor();
    Map<Object, Integer> lookUps = new HashMap<>(dictionaryAccessor.getValueCount());
    for (int i = 0; i < dictionaryAccessor.getValueCount(); i++) {
      // for primitive array types we need a wrapper that implements equals and hashcode appropriately
      lookUps.put(dictionaryAccessor.getObject(i), i);
    }

    // vector to hold our indices (dictionary encoded values)
    NullableIntVector indices = new NullableIntVector(vector.getField().getName(), vector.getAllocator());
    NullableIntVector.Mutator mutator = indices.getMutator();

    ValueVector.Accessor accessor = vector.getAccessor();
    int count = accessor.getValueCount();

    indices.allocateNew(count);

    for (int i = 0; i < count; i++) {
      Object value = accessor.getObject(i);
      if (value != null) { // if it's null leave it null
        // note: this may fail if value was not included in the dictionary
        mutator.set(i, lookUps.get(value));
      }
    }
    mutator.setValueCount(count);

    return new DictionaryVector(indices, dictionary);
  }

  /**
   * Decodes a dictionary encoded array using the provided dictionary.
   *
   * @param indices dictionary encoded values, must be int type
   * @param dictionary dictionary used to decode the values
   * @return vector with values restored from dictionary
   */
  public static ValueVector decode(ValueVector indices, Dictionary dictionary) {
    ValueVector.Accessor accessor = indices.getAccessor();
    int count = accessor.getValueCount();
    ValueVector dictionaryVector = dictionary.getVector();
    // copy the dictionary values into the decoded vector
    TransferPair transfer = dictionaryVector.getTransferPair(indices.getAllocator());
    transfer.getTo().allocateNewSafe();
    for (int i = 0; i < count; i++) {
      Object index = accessor.getObject(i);
      if (index != null) {
        transfer.copyValueSafe(((Number) index).intValue(), i);
      }
    }

    ValueVector decoded = transfer.getTo();
    decoded.getMutator().setValueCount(count);
    return decoded;
  }

  private static void validateType(MinorType type) {
    // byte arrays don't work as keys in our dictionary map - we could wrap them with something to
    // implement equals and hashcode if we want that functionality
    if (type == MinorType.VARBINARY || type == MinorType.LIST || type == MinorType.MAP || type == MinorType.UNION) {
      throw new IllegalArgumentException("Dictionary encoding for complex types not implemented");
    }
  }

  public ValueVector getIndexVector() { return indices; }

  public Dictionary getDictionary() { return dictionary; }

  @Override
  public Field getField() {
    Field field = indices.getField();
    return new Field(field.getName(), field.isNullable(), field.getType(), dictionary.getEncoding(), field.getChildren());
  }

  // note: dictionary vector is not closed, as it may be shared
  @Override
  public void close() { indices.close(); }

  @Override
  public MinorType getMinorType() { return indices.getMinorType(); }

  @Override
  public void allocateNew() throws OutOfMemoryException { indices.allocateNew(); }

  @Override
  public boolean allocateNewSafe() { return indices.allocateNewSafe(); }

  @Override
  public BufferAllocator getAllocator() { return indices.getAllocator();  }

  @Override
  public void setInitialCapacity(int numRecords) { indices.setInitialCapacity(numRecords); }

  @Override
  public int getValueCapacity() { return indices.getValueCapacity(); }

  @Override
  public int getBufferSize() { return indices.getBufferSize(); }

  @Override
  public int getBufferSizeFor(int valueCount) { return indices.getBufferSizeFor(valueCount); }

  @Override
  public Iterator<ValueVector> iterator() {
    return indices.iterator();
  }

  @Override
  public void clear() { indices.clear(); }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) { return indices.getTransferPair(allocator); }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) { return indices.getTransferPair(ref, allocator); }

  @Override
  public TransferPair makeTransferPair(ValueVector target) { return indices.makeTransferPair(target); }

  @Override
  public Accessor getAccessor() { return indices.getAccessor(); }

  @Override
  public Mutator getMutator() { return indices.getMutator(); }

  @Override
  public FieldReader getReader() { return indices.getReader(); }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) { return indices.getBuffers(clear); }

  @Override
  public void initializeChildrenFromFields(List<Field> children) { indices.initializeChildrenFromFields(children); }

  @Override
  public List<FieldVector> getChildrenFromFields() { return indices.getChildrenFromFields(); }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    indices.loadFieldBuffers(fieldNode, ownBuffers);
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() { return indices.getFieldBuffers(); }

  @Override
  public List<BufferBacked> getFieldInnerVectors() { return indices.getFieldInnerVectors(); }
}
