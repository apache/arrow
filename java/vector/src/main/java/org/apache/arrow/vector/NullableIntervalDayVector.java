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
import org.apache.arrow.vector.complex.impl.IntervalDayReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.joda.time.Period;

/**
 * NullableIntervalDayVector implements a fixed width vector (8 bytes) of
 * integer values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public class NullableIntervalDayVector extends BaseNullableFixedWidthVector {
   private static final byte TYPE_WIDTH = 8;
   private static final byte MILLISECOND_OFFSET = 4;
   private final FieldReader reader;

   public NullableIntervalDayVector(String name, BufferAllocator allocator) {
      this(name, FieldType.nullable(Types.MinorType.INTERVALDAY.getType()),
              allocator);
   }

   public NullableIntervalDayVector(String name, FieldType fieldType, BufferAllocator allocator) {
      super(name, allocator, fieldType, TYPE_WIDTH);
      reader = new IntervalDayReaderImpl(NullableIntervalDayVector.this);
   }

   @Override
   public FieldReader getReader(){
      return reader;
   }

   @Override
   public Types.MinorType getMinorType() {
      return Types.MinorType.INTERVALDAY;
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
   public ArrowBuf get(int index) throws IllegalStateException {
      if(isSet(index) == 0) {
         return null;
      }
      return valueBuffer.slice(index * TYPE_WIDTH, TYPE_WIDTH);
   }

   /**
    * Get the element at the given index from the vector and
    * sets the state in holder. If element at given index
    * is null, holder.isSet will be zero.
    *
    * @param index   position of element
    */
   public void get(int index, NullableIntervalDayHolder holder){
      if(isSet(index) == 0) {
         holder.isSet = 0;
         return;
      }
      final int startIndex = index * TYPE_WIDTH;
      holder.isSet = 1;
      holder.days = valueBuffer.getInt(startIndex);
      holder.milliseconds = valueBuffer.getInt(startIndex + MILLISECOND_OFFSET);
   }

   /**
    * Same as {@link #get(int)}.
    *
    * @param index   position of element
    * @return element at given index
    */
   public Period getObject(int index) {
      if (isSet(index) == 0) {
         return null;
      } else {
         final int startIndex = index * TYPE_WIDTH;
         final int days = valueBuffer.getInt(startIndex);
         final int milliseconds = valueBuffer.getInt(startIndex + MILLISECOND_OFFSET);
         final Period p = new Period();
         return p.plusDays(days).plusMillis(milliseconds);
      }
   }

   public StringBuilder getAsStringBuilder(int index) {
      if (isSet(index) == 0) {
         return null;
      }else{
         return getAsStringBuilderHelper(index);
      }
   }

   private StringBuilder getAsStringBuilderHelper(int index) {
      final int startIndex = index * TYPE_WIDTH;

      final int  days = valueBuffer.getInt(startIndex);
      int millis = valueBuffer.getInt(startIndex + MILLISECOND_OFFSET);

      final int hours = millis / (org.apache.arrow.vector.util.DateUtility.hoursToMillis);
      millis = millis % (org.apache.arrow.vector.util.DateUtility.hoursToMillis);

      final int minutes = millis / (org.apache.arrow.vector.util.DateUtility.minutesToMillis);
      millis = millis % (org.apache.arrow.vector.util.DateUtility.minutesToMillis);

      final int seconds = millis / (org.apache.arrow.vector.util.DateUtility.secondsToMillis);
      millis = millis % (org.apache.arrow.vector.util.DateUtility.secondsToMillis);

      final String dayString = (Math.abs(days) == 1) ? " day " : " days ";

      return(new StringBuilder().
              append(days).append(dayString).
              append(hours).append(":").
              append(minutes).append(":").
              append(seconds).append(".").
              append(millis));
   }

   public void copyFrom(int fromIndex, int thisIndex, NullableIntervalDayVector from) {
      if (from.isSet(fromIndex) != 0) {
         BitVectorHelper.setValidityBitToOne(validityBuffer, thisIndex);
         from.valueBuffer.getBytes(fromIndex * TYPE_WIDTH, this.valueBuffer,
                 thisIndex * TYPE_WIDTH, TYPE_WIDTH);
      }
   }

   public void copyFromSafe(int fromIndex, int thisIndex, NullableIntervalDayVector from) {
      handleSafe(thisIndex);
      copyFrom(fromIndex, thisIndex, from);
   }


   /******************************************************************
    *                                                                *
    *          vector value setter methods                           *
    *                                                                *
    ******************************************************************/


   /**
    * Set the element at the given index to the given value.
    *
    * @param index   position of element
    * @param value   value of element
    */
   public void set(int index, ArrowBuf value) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      valueBuffer.setBytes(index * TYPE_WIDTH, value, 0, TYPE_WIDTH);
   }

   /**
    * Set the element at the given index to the given value.
    *
    * @param index          position of element
    * @param days           days for the interval
    * @param milliseconds   milliseconds for the interval
    */
   public void set(int index, int days, int milliseconds){
      final int offsetIndex = index * TYPE_WIDTH;
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      valueBuffer.setInt(offsetIndex, days);
      valueBuffer.setInt((offsetIndex + MILLISECOND_OFFSET), milliseconds);
   }

   /**
    * Set the element at the given index to the value set in data holder.
    * If the value in holder is not indicated as set, element in the
    * at the given index will be null.
    *
    * @param index   position of element
    * @param holder  nullable data holder for value of element
    */
   public void set(int index, NullableIntervalDayHolder holder) throws IllegalArgumentException {
      if(holder.isSet < 0) {
         throw new IllegalArgumentException();
      }
      else if(holder.isSet > 0) {
         set(index, holder.days, holder.milliseconds);
      }
      else {
         BitVectorHelper.setValidityBit(validityBuffer, index, 0);
      }
   }

   /**
    * Set the element at the given index to the value set in data holder.
    *
    * @param index   position of element
    * @param holder  data holder for value of element
    */
   public void set(int index, IntervalDayHolder holder){
      set(index, holder.days, holder.milliseconds);
   }

   /**
    * Same as {@link #set(int, ArrowBuf)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param value   value of element
    */
   public void setSafe(int index, ArrowBuf value) {
      handleSafe(index);
      set(index, value);
   }

   /**
    * Same as {@link #set(int, int, int)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index          position of element
    * @param days           days for the interval
    * @param milliseconds   milliseconds for the interval
    */
   public void setSafe(int index, int days, int milliseconds) {
      handleSafe(index);
      set(index, days, milliseconds);
   }

   /**
    * Same as {@link #set(int, NullableIntervalDayHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  nullable data holder for value of element
    */
   public void setSafe(int index, NullableIntervalDayHolder holder) throws IllegalArgumentException {
      handleSafe(index);
      set(index, holder);
   }

   /**
    * Same as {@link #set(int, IntervalDayHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  data holder for value of element
    */
   public void setSafe(int index, IntervalDayHolder holder){
      handleSafe(index);
      set(index, holder);
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

   public void set(int index, int isSet, int days, int milliseconds) {
      if (isSet > 0) {
         set(index, days, milliseconds);
      } else {
         BitVectorHelper.setValidityBit(validityBuffer, index, 0);
      }
   }

   public void setSafe(int index, int isSet, int days, int milliseconds) {
      handleSafe(index);
      set(index, isSet, days, milliseconds);
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
      return new TransferImpl((NullableIntervalDayVector)to);
   }

   private class TransferImpl implements TransferPair {
      NullableIntervalDayVector to;

      public TransferImpl(String ref, BufferAllocator allocator){
         to = new NullableIntervalDayVector(ref, field.getFieldType(), allocator);
      }

      public TransferImpl(NullableIntervalDayVector to){
         this.to = to;
      }

      @Override
      public NullableIntervalDayVector getTo(){
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
         to.copyFromSafe(fromIndex, toIndex, NullableIntervalDayVector.this);
      }
   }
}