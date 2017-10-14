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

package org.apache.arrow.vector;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.VarCharReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

import java.nio.ByteBuffer;

public class NullableVarCharVector extends BaseNullableVariableWidthVector {
   private static final org.slf4j.Logger logger =
           org.slf4j.LoggerFactory.getLogger(NullableIntVector.class);
   private final FieldReader reader;

   public NullableVarCharVector(String name, BufferAllocator allocator) {
      this(name, FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), allocator);
   }

   public NullableVarCharVector(String name, FieldType fieldType, BufferAllocator allocator) {
      super(name, allocator, fieldType);
      reader = new VarCharReaderImpl(NullableVarCharVector.this);
   }

   @Override
   protected org.slf4j.Logger getLogger() {
      return logger;
   }

   @Override
   public FieldReader getReader(){
      return reader;
   }

   @Override
   public Types.MinorType getMinorType() {
      return Types.MinorType.VARCHAR;
   }


   /******************************************************************
    *                                                                *
    *          vector value getter methods                           *
    *                                                                *
    ******************************************************************/


   /**
    * Get the variable length element at specified index as byte array.
    *
    * @param index   position of element to get
    * @return array of bytes for non-null element, null otherwise
    */
   public byte[] get(int index) {
      assert index >= 0;
      if(isSet(index) == 0) {
         throw new IllegalStateException("Value at index is null");
      }
      final int startOffset = getstartOffset(index);
      final int dataLength =
              offsetBuffer.getInt((index + 1) * OFFSET_WIDTH) - startOffset;
      final byte[] result = new byte[dataLength];
      valueBuffer.getBytes(startOffset, result, 0, dataLength);
      return result;
   }

   /**
    * Get the variable length element at specified index as Text.
    *
    * @param index   position of element to get
    * @return Text object for non-null element, null otherwise
    */
   public Text getObject(int index) {
      Text result = new Text();
      byte[] b;
      try {
         b = get(index);
      } catch (IllegalStateException e) {
         return null;
      }
      result.set(b);
      return result;
   }

   /**
    * Get the variable length element at specified index as Text.
    *
    * @param index   position of element to get
    * @return greater than 0 length for non-null element, 0 otherwise
    */
   public int getValueLength(int index) {
      assert index >= 0;
      if(isSet(index) == 0) {
         return 0;
      }
      final int startOffset = getstartOffset(index);
      final int dataLength =
              offsetBuffer.getInt((index + 1) * OFFSET_WIDTH) - startOffset;
      return dataLength;
   }

   /**
    * Get the variable length element at specified index and sets the state
    * in provided holder.
    *
    * @param index   position of element to get
    * @param holder  data holder to be populated by this function
    */
   public void get(int index, NullableVarCharHolder holder){
      assert index >= 0;
      if(isSet(index) == 0) {
         holder.isSet = 0;
         return;
      }
      final int startOffset = getstartOffset(index);
      final int dataLength =
              offsetBuffer.getInt((index + 1) * OFFSET_WIDTH) - startOffset;
      holder.isSet = 1;
      holder.start = startOffset;
      holder.end = dataLength;
      holder.buffer = valueBuffer;
   }



   /******************************************************************
    *                                                                *
    *          vector value setter methods                           *
    *                                                                *
    ******************************************************************/



   public void copyFrom(int fromIndex, int thisIndex, NullableVarCharVector from) {
      fillHoles(thisIndex);
      if (from.isSet(fromIndex) != 0) {
         set(thisIndex, from.get(fromIndex));
         lastSet = thisIndex;
      }
   }

   public void copyFromSafe(int fromIndex, int thisIndex, NullableVarCharVector from) {
      fillEmpties(thisIndex);
      if (from.isSet(fromIndex) != 0) {
         setSafe(thisIndex, from.get(fromIndex));
         lastSet = thisIndex;
      }
   }


   /**
    * Set the variable length element at the specified index to the supplied
    * byte array. This is same as using {@link #set(int, byte[], int, int)}
    * with start as 0 and length as value.length
    *
    * @param index   position of the element to set
    * @param value   array of bytes to write
    */
   public void set(int index, byte[] value) {
      assert index >= 0;
      fillHoles(index);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setBytes(index, value, 0, value.length);
      lastSet = index;
   }

   /**
    * Same as {@link #set(int, byte[])} except that it handles the
    * case where index and length of new element are beyond the existing
    * capacity of the vector.
    *
    * @param index   position of the element to set
    * @param value   array of bytes to write
    */
   public void setSafe(int index, byte[] value) {
      assert index >= 0;
      fillEmpties(index);
      handleSafe(index, value.length);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setBytes(index, value, 0, value.length);
      lastSet = index;
   }

   /**
    * Set the variable length element at the specified index to the supplied
    * byte array.
    *
    * @param index   position of the element to set
    * @param value   array of bytes to write
    * @param start   start index in array of bytes
    * @param length  length of data in array of bytes
    */
   public void set(int index, byte[] value, int start, int length) {
      assert index >= 0;
      fillHoles(index);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setBytes(index, value, start, length);
      lastSet = index;
   }

   /**
    * Same as {@link #set(int, byte[], int, int)} except that it handles the
    * case where index and length of new element are beyond the existing
    * capacity of the vector.
    *
    * @param index   position of the element to set
    * @param value   array of bytes to write
    * @param start   start index in array of bytes
    * @param length  length of data in array of bytes
    */
   public void setSafe(int index, byte[] value, int start, int length) {
      assert index >= 0;
      fillEmpties(index);
      handleSafe(index, length);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setBytes(index, value, start, length);
      lastSet = index;
   }

   /**
    * Set the variable length element at the specified index to the
    * content in supplied ByteBuffer
    *
    * @param index   position of the element to set
    * @param value   ByteBuffer with data
    * @param start   start index in ByteBuffer
    * @param length  length of data in ByteBuffer
    */
   public void set(int index, ByteBuffer value, int start, int length) {
      assert index >= 0;
      fillHoles(index);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      final int startOffset = getstartOffset(index);
      offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
      valueBuffer.setBytes(startOffset, value, start, length);
      lastSet = index;
   }

   /**
    * Same as {@link #set(int, ByteBuffer, int, int)} except that it handles the
    * case where index and length of new element are beyond the existing
    * capacity of the vector.
    *
    * @param index   position of the element to set
    * @param value   ByteBuffer with data
    * @param start   start index in ByteBuffer
    * @param length  length of data in ByteBuffer
    */
   public void setSafe(int index, ByteBuffer value, int start, int length) {
      assert index >= 0;
      fillEmpties(index);
      handleSafe(index, length);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      final int startOffset = getstartOffset(index);
      offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
      valueBuffer.setBytes(startOffset, value, start, length);
      lastSet = index;
   }

   /**
    * Set the variable length element at the specified index to the data
    * buffer supplied in the holder
    *
    * @param index   position of the element to set
    * @param holder  holder that carries data buffer.
    */
   public void set(int index, VarCharHolder holder) {
      assert index >= 0;
      fillHoles(index);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      final int dataLength = holder.end - holder.start;
      final int startOffset = getstartOffset(index);
      offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
      valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
      lastSet = index;
   }

   /**
    * Same as {@link #set(int, VarCharHolder)} except that it handles the
    * case where index and length of new element are beyond the existing
    * capacity of the vector.
    *
    * @param index   position of the element to set
    * @param holder  holder that carries data buffer.
    */
   public void setSafe(int index, VarCharHolder holder) {
      assert index >= 0;
      final int dataLength = holder.end - holder.start;
      fillEmpties(index);
      handleSafe(index, dataLength);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      final int startOffset = getstartOffset(index);
      offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
      valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
      lastSet = index;
   }

   /**
    * Set the variable length element at the specified index to the data
    * buffer supplied in the holder
    *
    * @param index   position of the element to set
    * @param holder  holder that carries data buffer.
    */
   public void set(int index, NullableVarCharHolder holder) {
      assert index >= 0;
      fillHoles(index);
      BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
      final int dataLength = holder.end - holder.start;
      final int startOffset = getstartOffset(index);
      offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
      valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
      lastSet = index;
   }

   /**
    * Same as {@link #set(int, NullableVarCharHolder)} except that it handles the
    * case where index and length of new element are beyond the existing
    * capacity of the vector.
    *
    * @param index   position of the element to set
    * @param holder  holder that carries data buffer.
    */
   public void setSafe(int index, NullableVarCharHolder holder) {
      assert index >= 0;
      final int dataLength = holder.end - holder.start;
      fillEmpties(index);
      handleSafe(index, dataLength);
      BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
      final int startOffset = getstartOffset(index);
      offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
      valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
      lastSet = index;
   }

   /**
    * Sets the value length for an element.
    *
    * @param index   position of the element to set
    * @param length  length of the element
    */
   public void setValueLengthSafe(int index, int length) {
      assert index >= 0;
      handleSafe(index, length);
      final int startOffset = getstartOffset(index);
      offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
   }

   /**
    * Set the element at the given index to null.
    *
    * @param index   position of element
    */
   public void setNull(int index){
      handleSafe(index, 0);
      /* not really needed to set the bit to 0 as long as
       * the buffer always starts from 0.
       */
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
   }

   public void set(int index, int isSet, int startField, int endField, ArrowBuf bufferField ) {
      assert index >= 0;
      fillHoles(index);
      BitVectorHelper.setValidityBit(validityBuffer, index, isSet);
      final int startOffset = offsetBuffer.getInt(index * OFFSET_WIDTH);
      offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + endField);
      final ArrowBuf bb = bufferField.slice(startField, endField);
      valueBuffer.setBytes(startOffset, bb);
      lastSet = index;
   }

   public void setSafe(int index, int isSet, int startField, int endField, ArrowBuf bufferField ) {
      assert index >= 0;
      handleSafe(index, endField);
      set(index, isSet, startField, endField, bufferField);
   }


   /******************************************************************
    *                                                                *
    *                      vector transfer                           *
    *                                                                *
    ******************************************************************/

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator){
      return new TransferImpl(ref, allocator);
   }

   @Override
   public TransferPair makeTransferPair(ValueVector to) {
      return new TransferImpl((NullableVarCharVector)to);
   }

   private class TransferImpl implements TransferPair {
      NullableVarCharVector to;

      public TransferImpl(String ref, BufferAllocator allocator){
         to = new NullableVarCharVector(ref, field.getFieldType(), allocator);
      }

      public TransferImpl(NullableVarCharVector to){
         this.to = to;
      }

      @Override
      public NullableVarCharVector getTo(){
         return to;
      }

      @Override
      public void transfer(){
         transferTo(to);
      }

      @Override
      public void splitAndTransfer(int startIndex, int length) {
         splitAndTransferTo(startIndex, length, to);
      }

      @Override
      public void copyValueSafe(int fromIndex, int toIndex) {
         to.copyFromSafe(fromIndex, toIndex, NullableVarCharVector.this);
      }
   }
}
