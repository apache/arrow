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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

public abstract class BaseNullableFixedWidthVector extends BaseValueVector
        implements FixedWidthVector, FieldVector {
   private final byte typeWidth;

   private int valueAllocationSizeInBytes;
   private int validityAllocationSizeInBytes;

   protected final Field field;
   private int allocationMonitor;
   protected ArrowBuf validityBuffer;
   protected ArrowBuf valueBuffer;
   protected int valueCount;

   public BaseNullableFixedWidthVector(final String name, final BufferAllocator allocator,
                                       FieldType fieldType, final byte typeWidth) {
      super(name, allocator);
      this.typeWidth = typeWidth;
      valueAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * typeWidth;
      validityAllocationSizeInBytes = getSizeFromCount(INITIAL_VALUE_ALLOCATION);
      field = new Field(name, fieldType, null);
      valueCount = 0;
      allocationMonitor = 0;
      validityBuffer = allocator.getEmpty();
      valueBuffer = allocator.getEmpty();
   }

   /* TODO:
    * Determine how writerIndex and readerIndex need to be used. Right now we
    * are setting the writerIndex and readerIndex in the call to getFieldBuffers
    * using the valueCount -- this assumes that the caller of getFieldBuffers
    * on the vector has already invoked setValueCount.
    *
    * Do we need to set them during vector transfer and splitAndTransfer?
    */

   /* TODO:
    *
    * see if getNullCount() can be made faster -- O(1)
    */

   /* TODO:
    * Once the entire hierarchy has been refactored, move common functions
    * like getNullCount(), splitAndTransferValidityBuffer to top level
    * base class BaseValueVector.
    *
    * Along with this, some class members (validityBuffer) can also be
    * abstracted out to top level base class.
    *
    * Right now BaseValueVector is the top level base class for other
    * vector types in ValueVector hierarchy and those vectors have not
    * yet been refactored so moving things to the top class as of now
    * is not a good idea.
    */

   /* TODO:
    * See if we need logger -- unnecessary object probably
    */

   protected abstract org.slf4j.Logger getLogger();

   @Override
   public Mutator getMutator() {
      throw new  UnsupportedOperationException("Mutator is not needed to write into vector");
   }

   @Override
   public Accessor getAccessor() {
      throw new UnsupportedOperationException("Accessor is not needed to read from vector");
   }

   @Override
   public long getValidityBufferAddress() {
      return (validityBuffer.memoryAddress());
   }

   @Override
   public long getDataBufferAddress() {
      return (valueBuffer.memoryAddress());
   }

   @Override
   public long getOffsetBufferAddress() {
      throw new UnsupportedOperationException("not supported for fixed-width vectors");
   }

   @Override
   public ArrowBuf getValidityBuffer() {
      return validityBuffer;
   }

   @Override
   public ArrowBuf getDataBuffer() {
      return valueBuffer;
   }

   @Override
   public ArrowBuf getOffsetBuffer() {
      throw new UnsupportedOperationException("not supported for fixed-width vectors");
   }

   @Override
   public void setInitialCapacity(int valueCount) {
      final long size = (long)valueCount * typeWidth;
      if (size > MAX_ALLOCATION_SIZE) {
         throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
      }
      valueAllocationSizeInBytes = (int)size;
      validityAllocationSizeInBytes = getSizeFromCount(valueCount);
   }

   @Override
   public int getValueCapacity(){
      return Math.min(getValueBufferValueCapacity(), getValidityBufferValueCapacity());
   }

   /* for test purposes */
   private int getValueBufferValueCapacity() {
      return (int)((valueBuffer.capacity() * 1.0)/typeWidth);
   }

   /* for test purposes */
   private int getValidityBufferValueCapacity() {
      return (int)(validityBuffer.capacity() * 8L);
   }

   /* number of bytes for the validity buffer for the given valueCount */
   protected int getSizeFromCount(int valueCount) {
      return (int) Math.ceil(valueCount / 8.0);
   }

   @Override
   public void zeroVector() {
      initValidityBuffer();
      initValueBuffer();
   }

   private void initValidityBuffer() {
      validityBuffer.setZero(0, validityBuffer.capacity());
   }

   private void initValueBuffer() {
      valueBuffer.setZero(0, valueBuffer.capacity());
   }

   public void reset() {
      zeroVector();
   }

   @Override
   public void close() { clear(); }

   @Override
   public void clear() {
      validityBuffer = releaseBuffer(validityBuffer);
      valueBuffer = releaseBuffer(valueBuffer);
   }

   /* used to step down the memory allocation */
   protected void incrementAllocationMonitor() {
      if (allocationMonitor < 0) {
         allocationMonitor = 0;
      }
      allocationMonitor++;
   }

   /* used to step up the memory allocation */
   protected void decrementAllocationMonitor() {
      if (allocationMonitor > 0) {
         allocationMonitor = 0;
      }
      allocationMonitor--;
   }

   @Override
   public void allocateNew() {
      if(!allocateNewSafe()){
         throw new OutOfMemoryException("Failure while allocating memory.");
      }
   }

   public boolean allocateNewSafe() {
      long curAllocationSizeValue = valueAllocationSizeInBytes;
      long curAllocationSizeValidity = validityAllocationSizeInBytes;

      if (curAllocationSizeValue > MAX_ALLOCATION_SIZE) {
         throw new OversizedAllocationException("Requested amount of memory exceeds limit");
      }

      /* we are doing a new allocation -- release the current buffers */
      clear();

      try{
         allocateBytes(curAllocationSizeValue, curAllocationSizeValidity);
      } catch (Exception e) {
         getLogger().error("ERROR: Failure in allocateNewSafe");
         getLogger().error(e.getMessage());
         clear();
         return false;
      }

      return true;
   }

   public void allocateNew(int valueCount) {
      long valueBufferSize = valueCount * typeWidth;
      long validityBufferSize = getSizeFromCount(valueCount);

      if (allocationMonitor > 10) {
         /* step down the default memory allocation since we have observed
          * multiple times that provisioned value capacity was much larger than
          * actually needed. see setValueCount for more details.
          */
         valueBufferSize = Math.max(8, valueBufferSize / 2);
         validityBufferSize = Math.max(8, validityBufferSize / 2);
         allocationMonitor = 0;
      } else if (allocationMonitor < -2) {
         valueBufferSize = valueBufferSize * 2L;
         validityBufferSize = validityBufferSize * 2L;
         allocationMonitor = 0;
      }

      if (valueBufferSize > MAX_ALLOCATION_SIZE) {
         throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
      }

      /* we are doing a new allocation -- release the current buffers */
      clear();

      try {
         allocateBytes(valueBufferSize, validityBufferSize);
      } catch(Exception e) {
         getLogger().error("ERROR: Failure in allocateNew");
         getLogger().error(e.getMessage());
         clear();
         throw e;
      }
   }

   /**
    * Actual memory allocation is done by this function. All the calculations
    * and knowledge about what size to allocate is upto the callers of this
    * method.
    * Callers appropriately handle errors if memory allocation fails here.
    * Callers should also take care of determining that desired size is
    * within the bounds of max allocation allowed and any other error
    * conditions.
    */
   private void allocateBytes(final long valueBufferSize, final long validityBufferSize) {
      /* allocate data buffer */
      int curSize = (int)valueBufferSize;
      valueBuffer = allocator.buffer(curSize);
      valueBuffer.readerIndex(0);
      valueAllocationSizeInBytes = curSize;

      /* allocate validity buffer */
      allocateValidityBuffer((int)validityBufferSize);
      initValidityBuffer();
   }

   /*
    * during splitAndTransfer, if we splitting from a random position within a byte,
    * we can't just slice the source buffer so we have to explicitly allocate the
    * validityBuffer of the target vector. This is unlike the databuffer which we can
    * always slice for the target vector.
    */
   private void allocateValidityBuffer(final int validityBufferSize) {
      validityBuffer = allocator.buffer(validityBufferSize);
      validityBuffer.readerIndex(0);
      validityAllocationSizeInBytes = validityBufferSize;
      initValidityBuffer();
   }

   @Override
   public int getBufferSizeFor(final int count) {
      if (count == 0) { return 0; }
      return (count * typeWidth) + getSizeFromCount(count);
   }

   @Override
   public int getBufferSize() {
      if (valueCount == 0) { return 0; }
      return (valueCount * typeWidth) + getSizeFromCount(valueCount);
   }

   @Override
   public Field getField() {
      return field;
   }

   @Override
   public ArrowBuf[] getBuffers(boolean clear) {
      final ArrowBuf[] buffers = new ArrowBuf[2];
      buffers[0] = validityBuffer;
      buffers[1] = valueBuffer;
      if (clear) {
         for (final ArrowBuf buffer:buffers) {
            buffer.retain(1);
         }
         clear();
      }
      return buffers;
   }

   @Override
   public void reAlloc() {
      valueBuffer = reallocBufferHelper(valueBuffer, true);
      validityBuffer = reallocBufferHelper(validityBuffer, false);
   }

   private ArrowBuf reallocBufferHelper(ArrowBuf buffer, final boolean dataBuffer) {
      final int currentBufferCapacity = buffer.capacity();
      long baseSize  = (dataBuffer ? valueAllocationSizeInBytes
                                   : validityAllocationSizeInBytes);

      if (baseSize < (long)currentBufferCapacity) {
         baseSize = (long)currentBufferCapacity;
      }

      long newAllocationSize = baseSize * 2L;
      newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);

      if (newAllocationSize > MAX_ALLOCATION_SIZE) {
         throw new OversizedAllocationException("Unable to expand the buffer");
      }

      getLogger().debug("Reallocating vector [{}]. # of bytes: [{}] -> [{}]",
              name, (dataBuffer ? valueAllocationSizeInBytes : validityAllocationSizeInBytes),
              newAllocationSize);

      final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
      newBuf.setBytes(0, buffer, 0, currentBufferCapacity);
      final int halfNewCapacity = newBuf.capacity() / 2;
      newBuf.setZero(halfNewCapacity, halfNewCapacity);
      buffer.release(1);
      buffer = newBuf;
      if (dataBuffer) {
         valueAllocationSizeInBytes = (int)newAllocationSize;
      }
      else {
         validityAllocationSizeInBytes = (int)newAllocationSize;
      }

      return buffer;
   }

   @Override
   public List<BufferBacked> getFieldInnerVectors() { throw new UnsupportedOperationException(); }

   @Override
   public void initializeChildrenFromFields(List<Field> children) {
      if (!children.isEmpty()) {
         throw new IllegalArgumentException("primitive type vector can not have children");
      }
   }

   @Override
   public List<FieldVector> getChildrenFromFields() {
      return Collections.emptyList();
   }

   @Override
   public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
      if (ownBuffers.size() != 2) {
         throw new IllegalArgumentException("Illegal buffer count, expected " + 2 + ", got: " + ownBuffers.size());
      }

      ArrowBuf bitBuffer = ownBuffers.get(0);
      ArrowBuf dataBuffer = ownBuffers.get(1);

      validityBuffer.release();
      validityBuffer = bitBuffer.retain(allocator);
      valueBuffer.release();
      valueBuffer = dataBuffer.retain(allocator);

      valueCount = fieldNode.getLength();

      valueAllocationSizeInBytes = valueBuffer.capacity();
      validityAllocationSizeInBytes = validityBuffer.capacity();
   }

   public List<ArrowBuf> getFieldBuffers() {
      List<ArrowBuf> result = new ArrayList<>(2);

      validityBuffer.readerIndex(0);
      validityBuffer.writerIndex(getSizeFromCount(valueCount));
      valueBuffer.readerIndex(0);
      valueBuffer.writerIndex(valueCount * typeWidth);

      result.add(validityBuffer);
      result.add(valueBuffer);

      return result;
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
      return getTransferPair(ref, allocator);
   }

   @Override
   public TransferPair getTransferPair(BufferAllocator allocator){
      return getTransferPair(name, allocator);
   }

   public abstract TransferPair getTransferPair(String ref, BufferAllocator allocator);

   public void transferTo(BaseNullableFixedWidthVector target){
      compareTypes(target, "transferTo");
      target.clear();
      target.validityBuffer = validityBuffer.transferOwnership(target.allocator).buffer;
      target.valueBuffer = valueBuffer.transferOwnership(target.allocator).buffer;
      target.valueCount = valueCount;
      clear();
   }

   public void splitAndTransferTo(int startIndex, int length,
                                  BaseNullableFixedWidthVector target) {
      compareTypes(target, "splitAndTransferTo");
      target.clear();
      splitAndTransferValidityBuffer(startIndex, length, target);
      splitAndTransferValueBuffer(startIndex, length, target);
      target.setValueCount(length);
   }

   private void splitAndTransferValueBuffer(int startIndex, int length,
                                            BaseNullableFixedWidthVector target) {
      final int startPoint = startIndex * typeWidth;
      final int sliceLength = length * typeWidth;
      target.valueBuffer = valueBuffer.slice(startPoint, sliceLength).transferOwnership(target.allocator).buffer;
   }

   private void splitAndTransferValidityBuffer(int startIndex, int length,
                                               BaseNullableFixedWidthVector target) {
      assert startIndex + length <= valueCount;
      int firstByteSource = BitVectorHelper.byteIndex(startIndex);
      int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
      int byteSizeTarget = getSizeFromCount(length);
      int offset = startIndex % 8;

      if (length > 0) {
         if (offset == 0) {
            // slice
            if (target.validityBuffer != null) {
               target.validityBuffer.release();
            }
            target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
            target.validityBuffer.retain(1);
         }
         else {
            /* Copy data
             * When the first bit starts from the middle of a byte (offset != 0),
             * copy data from src BitVector.
             * Each byte in the target is composed by a part in i-th byte,
             * another part in (i+1)-th byte.
             */
            target.allocateValidityBuffer(byteSizeTarget);

            for (int i = 0; i < byteSizeTarget - 1; i++) {
               byte b1 = getBitsFromCurrentByte(this.validityBuffer, firstByteSource + i, offset);
               byte b2 = getBitsFromNextByte(this.validityBuffer, firstByteSource + i + 1, offset);

               target.validityBuffer.setByte(i, (b1 + b2));
            }

            /* Copying the last piece is done in the following manner:
             * if the source vector has 1 or more bytes remaining, we copy
             * the last piece as a byte formed by shifting data
             * from the current byte and the next byte.
             *
             * if the source vector has no more bytes remaining
             * (we are at the last byte), we copy the last piece as a byte
             * by shifting data from the current byte.
             */
            if((firstByteSource + byteSizeTarget - 1) < lastByteSource) {
               byte b1 = getBitsFromCurrentByte(this.validityBuffer,
                       firstByteSource + byteSizeTarget - 1, offset);
               byte b2 = getBitsFromNextByte(this.validityBuffer,
                       firstByteSource + byteSizeTarget, offset);

               target.validityBuffer.setByte(byteSizeTarget - 1, b1 + b2);
            }
            else {
               byte b1 = getBitsFromCurrentByte(this.validityBuffer,
                       firstByteSource + byteSizeTarget - 1, offset);
               target.validityBuffer.setByte(byteSizeTarget - 1, b1);
            }
         }
      }
   }

   private static byte getBitsFromCurrentByte(ArrowBuf data, int index, int offset) {
      return (byte)((data.getByte(index) & 0xFF) >>> offset);
   }

   private static byte getBitsFromNextByte(ArrowBuf data, int index, int offset) {
      return (byte)((data.getByte(index) << (8 - offset)));
   }


   /******************************************************************
    *                                                                *
    *          common getters and setters                            *
    *                                                                *
    ******************************************************************/


   /**
    * Get the number of elements that are null in the vector
    *
    * @return the number of null elements.
    */
   public int getNullCount() {
      int count = 0;
      final int sizeInBytes = getSizeFromCount(valueCount);

      for (int i = 0; i < sizeInBytes; ++i) {
         final byte byteValue = validityBuffer.getByte(i);
         /* Java uses two's complement binary representation, hence 11111111_b which is -1
          * when converted to Int will have 32bits set to 1. Masking the MSB and then
          * adding it back solves the issue.
          */
         count += Integer.bitCount(byteValue & 0x7F) - (byteValue >> 7);
      }
      int nullCount = (sizeInBytes * 8) - count;
      /* if the valueCount is not a multiple of 8,
       * the bits on the right were counted as null bits.
       */
      int remainder = valueCount % 8;
      nullCount -= remainder == 0 ? 0 : 8 - remainder;
      return nullCount;
   }


   /**
    * Get the value count of vector. This will always be zero unless
    * {@link #setValueCount(int)} has been called prior to calling this.
    *
    * @return valueCount for the vector
    */
   public int getValueCount(){
      return valueCount;
   }


   /**
    * Set value count for the vector.
    *
    * @param valueCount  value count to set
    */
   public void setValueCount(int valueCount) {
      this.valueCount = valueCount;
      final int currentValueCapacity = getValueCapacity();
      while (valueCount > getValueCapacity()) {
         reAlloc();
      }
      /*
       * We are trying to understand the pattern of memory allocation.
       * If initially, the user did vector.allocateNew(), we would have
       * allocated memory of default size (4096 * type width).
       * Later on user invokes setValueCount(count).
       *
       * If the existing value capacity is twice as large as the
       * valueCount, we know that we over-provisioned memory in the
       * first place when default memory allocation was done because user
       * really needs a much less value count in the vector.
       *
       * We record this by bumping up the allocationMonitor. If this pattern
       * happens for certain number of times and allocationMonitor
       * reaches the threshold (internal hardcoded) value, subsequent
       * call to allocateNew() will take care of stepping down the
       * default memory allocation size.
       *
       * Another case would be under-provisioning the initial memory and
       * thus going through a lot of realloc(). Here the goal is to
       * see if we can minimize the number of reallocations. Again the
       * state is recorded in allocationMonitor by decrementing it
       * (negative value). If a threshold is hit, realloc will try to
       * allocate more memory in order to possibly avoid a future realloc.
       * This case is also applicable to setSafe() methods which can trigger
       * a realloc() and thus we record the state there as well.
       */
      if (valueCount > 0) {
         if (currentValueCapacity >= (valueCount * 2)) {
            incrementAllocationMonitor();
         } else if (currentValueCapacity <= (valueCount/2)) {
            decrementAllocationMonitor();
         }
      }
   }


   /**
    * Check if the given index is within the current value capacity
    * of the vector
    *
    * @param index  position to check
    * @return true if index is within the current value capacity
    */
   public boolean isSafe(int index) {
      return index < getValueCapacity();
   }


   /**
    * Check if element at given index is null.
    *
    * @param index  position of element
    * @return true if element at given index is null, false otherwise
    */
   public boolean isNull(int index) {
      return (isSet(index) == 0);
   }


   /**
    * Same as {@link #isNull(int)}.
    *
    * @param index  position of element
    * @return 1 if element at given index is not null, 0 otherwise
    */
   public int isSet(int index) {
      final int byteIndex = index >> 3;
      final byte b = validityBuffer.getByte(byteIndex);
      final int bitIndex = index & 7;
      return Long.bitCount(b & (1L << bitIndex));
   }

   public void setIndexDefined(int index) {
      handleSafe(index);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
   }

   public void set(int index, byte[] value, int start, int length) {
      throw new UnsupportedOperationException();
   }

   public void setSafe(int index, byte[] value, int start, int length) {
      throw new UnsupportedOperationException();
   }

   public void set(int index, ByteBuffer value, int start, int length) {
      throw new UnsupportedOperationException();
   }

   public void setSafe(int index, ByteBuffer value, int start, int length) {
      throw new UnsupportedOperationException();
   }


   /******************************************************************
    *                                                                *
    *                helper methods for setters                      *
    *                                                                *
    ******************************************************************/



   protected void handleSafe(int index) {
      while (index >= getValueCapacity()) {
         decrementAllocationMonitor();
         reAlloc();
      }
   }
}