package org.apache.arrow.vector.dictionary;

import com.google.common.base.Preconditions;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;

public abstract class BaseVariableWidthVector implements VariableWidthVector {
   private static final int DEFAULT_RECORD_BYTE_COUNT = 8;
   public static final String MAX_ALLOCATION_SIZE_PROPERTY = "arrow.vector.max_allocation_bytes";
   public static final int MAX_ALLOCATION_SIZE = Integer.getInteger(MAX_ALLOCATION_SIZE_PROPERTY, Integer.MAX_VALUE);
   private static final int INITIAL_BYTE_COUNT = 4096 * DEFAULT_RECORD_BYTE_COUNT;
   private static final int MIN_BYTE_COUNT = 4096;

   private ArrowBuf data;
   private final BufferAllocator allocator;
   private final String name;
   private int allocationSizeInBytes = INITIAL_BYTE_COUNT;
   private int allocationMonitor = 0;
   private final UInt4Vector offsetVector;
   private final UInt4Vector.Accessor offsetAccessor;

   public BaseVariableWidthVector (String name, BufferAllocator allocator) {
      this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
      this.name = name;
      this.data = allocator.getEmpty();
      this.offsetVector = new UInt4Vector("offset-vector", allocator);
      offsetAccessor = offsetVector.getAccessor();
   }

   @Override
   public Field getField() {
      throw new UnsupportedOperationException("this operation is supported only for field vectors");
   }

   @Override
   public FieldReader getReader(){
      throw new UnsupportedOperationException("this operation is supported only for field vectors");
   }

   @Override
   public ArrowBuf getValidityBuffer() {
      throw new UnsupportedOperationException("this operation is not supported for non-nullable vectors");
   }

   @Override
   public ArrowBuf getDataBuffer() { return data; }

   @Override
   public ArrowBuf getOffsetBuffer() { return offsetVector.getDataBuffer(); }

   @Override
   public ArrowBuf[] getBuffers(boolean clear) {
      final ArrowBuf[] buffers = ObjectArrays.concat(offsetVector.getBuffers(false), getBuffersHelper(false), ArrowBuf.class);
      if (clear) {
         // does not make much sense but we have to retain buffers even when clear is set. refactor this interface.
         for (final ArrowBuf buffer:buffers) {
            buffer.retain(1);
         }
         clear();
      }
      return buffers;
   }

   private ArrowBuf[] getBuffersHelper(boolean clear) {
      ArrowBuf[] out;
      if (getBufferSize() == 0) {
         out = new ArrowBuf[0];
      } else {
         out = new ArrowBuf[] {data};
         data.readerIndex(0);
         if (clear) {
            data.retain(1);
         }
      }
      if (clear) {
         clear();
      }
      return out;
   }

   @Override
   public int getBufferSize(){
      if (getAccessor().getValueCount() == 0) {
         return 0;
      }
      return offsetVector.getBufferSize() + data.writerIndex();
   }

   @Override
   public int getBufferSizeFor(final int valueCount) {
      if (valueCount == 0) {
         return 0;
      }

      final int idx = offsetVector.getAccessor().get(valueCount);
      return offsetVector.getBufferSizeFor(valueCount + 1) + idx;
   }

   @Override
   public int getValueCapacity(){
      return Math.max(offsetVector.getValueCapacity() - 1, 0);
   }

   @Override
   public int getByteCapacity(){
      return data.capacity();
   }

   @Override
   public int getCurrentSizeInBytes() {
      return offsetVector.getAccessor().get(getAccessor().getValueCount());
   }

   /**
    * Return the number of bytes contained in the current var len byte vector.
    * @return the number of bytes contained in the current var len byte vector
    */
   public int getVarByteLength(){
      final int valueCount = getAccessor().getValueCount();
      if(valueCount == 0) {
         return 0;
      }
      return offsetVector.getAccessor().get(valueCount);
   }

   public UInt4Vector getOffsetVector(){
      return offsetVector;
   }

   public void reAlloc() {
      long baseSize = allocationSizeInBytes;
      final int currentBufferCapacity = data.capacity();
      if (baseSize < (long)currentBufferCapacity) {
         baseSize = (long)currentBufferCapacity;
      }
      long newAllocationSize = baseSize * 2L;
      newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);

      if (newAllocationSize > MAX_ALLOCATION_SIZE)  {
         throw new OversizedAllocationException("Unable to expand the buffer. Max allowed buffer size is reached.");
      }

      final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
      newBuf.setBytes(0, data, 0, currentBufferCapacity);
      data.release();
      data = newBuf;
      allocationSizeInBytes = (int)newAllocationSize;
   }

   @Override
   public void allocateNew() {
      if(!allocateNewSafe()){
         throw new OutOfMemoryException("Failure while allocating buffer.");
      }
   }

   public void decrementAllocationMonitor() {
      if (allocationMonitor > 0) {
         allocationMonitor = 0;
      }
      --allocationMonitor;
   }
}
