/**
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
 */

package org.apache.arrow.vector;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * NullableTimeStampVector is an abstract interface for fixed width vector (8 bytes)
 * of timestamp values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public abstract class NullableTimeStampVector extends BaseNullableFixedWidthVector {
   protected static final byte TYPE_WIDTH = 8;

   /**
    * Instantiate a NullableTimeStampVector. This doesn't allocate any memory for
    * the data in vector.
    * @param name name of the vector
    * @param fieldType type of Field materialized by this vector
    * @param allocator allocator for memory management.
    */
   public NullableTimeStampVector(String name, FieldType fieldType, BufferAllocator allocator) {
      super(name, allocator, fieldType, TYPE_WIDTH);
   }


   /******************************************************************
    *                                                                *
    *          vector value retrieval methods                        *
    *                                                                *
    ******************************************************************/

   /**
    * Get the element at the given index from the vector.
    *
    * @param index   position of element
    * @return element at given index
    */
   public long get(int index) throws IllegalStateException {
      if(isSet(index) == 0) {
         throw new IllegalStateException("Value at index is null");
      }
      return valueBuffer.getLong(index * TYPE_WIDTH);
   }

   /**
    * Copy a cell value from a particular index in source vector to a particular
    * position in this vector
    * @param fromIndex position to copy from in source vector
    * @param thisIndex position to copy to in this vector
    * @param from source vector
    */
   public void copyFrom(int fromIndex, int thisIndex, NullableTimeStampVector from) {
      if (from.isSet(fromIndex) != 0) {
         set(thisIndex, from.get(fromIndex));
      }
   }

   /**
    * Same as {@link #copyFromSafe(int, int, NullableTimeStampVector)} except that
    * it handles the case when the capacity of the vector needs to be expanded
    * before copy.
    * @param fromIndex position to copy from in source vector
    * @param thisIndex position to copy to in this vector
    * @param from source vector
    */
   public void copyFromSafe(int fromIndex, int thisIndex, NullableTimeStampVector from) {
      handleSafe(thisIndex);
      copyFrom(fromIndex, thisIndex, from);
   }


   /******************************************************************
    *                                                                *
    *          vector value setter methods                           *
    *                                                                *
    ******************************************************************/


   protected void setValue(int index, long value) {
      valueBuffer.setLong(index * TYPE_WIDTH, value);
   }

   /**
    * Set the element at the given index to the given value.
    *
    * @param index   position of element
    * @param value   value of element
    */
   public void set(int index, long value) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setValue(index, value);
   }

   /**
    * Same as {@link #set(int, long)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param value   value of element
    */
   public void setSafe(int index, long value) {
      handleSafe(index);
      set(index, value);
   }

   /**
    * Set the element at the given index to null.
    *
    * @param index   position of element
    */
   public void setNull(int index){
      handleSafe(index);
      /* not really needed to set the bit to 0 as long as
       * the buffer always starts from 0.
       */
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
   }

   /**
    * Store the given value at a particular position in the vector. isSet indicates
    * whether the value is NULL or not.
    * @param index position of the new value
    * @param isSet 0 for NULL value, 1 otherwise
    * @param value element value
    */
   public void set(int index, int isSet, long value) {
      if (isSet > 0) {
         set(index, value);
      } else {
         BitVectorHelper.setValidityBit(validityBuffer, index, 0);
      }
   }

   /**
    * Same as {@link #set(int, int, long)} except that it handles the case
    * when index is greater than or equal to current value capacity of the
    * vector.
    * @param index position of the new value
    * @param isSet 0 for NULL value, 1 otherwise
    * @param value element value
    */
   public void setSafe(int index, int isSet, long value) {
      handleSafe(index);
      set(index, isSet, value);
   }


   /******************************************************************
    *                                                                *
    *          helper routines currently                             *
    *          used in JsonFileReader and                            *
    *          JsonFileWriter                                        *
    *                                                                *
    ******************************************************************/


   /**
    * Given a data buffer, this method sets the element value at a particular
    * position. Reallocates the buffer if needed.
    *
    * This method should not be used externally.
    *
    * @param buffer data buffer
    * @param allocator allocator
    * @param valueCount number of elements in the vector
    * @param index position of the new element
    * @param value element value
    * @return data buffer
    */
   public static ArrowBuf set(ArrowBuf buffer, BufferAllocator allocator,
                              int valueCount, int index, long value) {
      if (buffer == null) {
         buffer = allocator.buffer(valueCount * TYPE_WIDTH);
      }
      buffer.setLong(index * TYPE_WIDTH, value);
      if (index == (valueCount - 1)) {
         buffer.writerIndex(valueCount * TYPE_WIDTH);
      }

      return buffer;
   }

   /**
    * Given a data buffer, get the value stored at a particular position
    * in the vector.
    *
    * This method should not be used externally.
    *
    * @param buffer data buffer
    * @param index position of the element.
    * @return value stored at the index.
    */
   public static long get(final ArrowBuf buffer, final int index) {
      return buffer.getLong(index * TYPE_WIDTH);
   }


   /******************************************************************
    *                                                                *
    *                      vector transfer                           *
    *                                                                *
    ******************************************************************/


   public class TransferImpl implements TransferPair {
      NullableTimeStampVector to;

      public TransferImpl(NullableTimeStampVector to) {
         this.to = to;
      }

      @Override
      public NullableTimeStampVector getTo(){
         return to;
      }

      @Override
      public void transfer() {
         transferTo(to);
      }

      @Override
      public void splitAndTransfer(int startIndex, int length) {
         splitAndTransferTo(startIndex, length, to);
      }

      @Override
      public void copyValueSafe(int fromIndex, int toIndex) {
         to.copyFromSafe(fromIndex, toIndex, NullableTimeStampVector.this);
      }
   }
}