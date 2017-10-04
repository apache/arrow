package org.apache.arrow.vector;


import io.netty.util.internal.PlatformDependent;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

public class PrototypeIntVectorNonCodegen extends BaseFixedWidthVector {
   private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IntVector.class);
   public static final int TYPE_WIDTH = 4;

   private final Accessor accessor;
   private final Mutator mutator;

   public PrototypeIntVectorNonCodegen (String name, BufferAllocator allocator) {
      super (name, allocator, TYPE_WIDTH);
      this.accessor = new Accessor();
      this.mutator = new Mutator();
   }

   @Override
   public Types.MinorType getMinorType() {
      return Types.MinorType.INT;
   }

   public TransferPair getTransferPair(BufferAllocator allocator){
      return new TransferImpl(name, allocator);
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator){
      return new TransferImpl(ref, allocator);
   }

   @Override
   public TransferPair getTransferPair(String name, BufferAllocator allocator, CallBack callBack) {
      return getTransferPair(name, allocator);
   }

   @Override
   public TransferPair makeTransferPair(ValueVector to) {
      return new TransferImpl((PrototypeIntVectorNonCodegen)to);
   }

   private class TransferImpl implements TransferPair{
      private PrototypeIntVectorNonCodegen to;

      public TransferImpl(String name, BufferAllocator allocator){
         to = new PrototypeIntVectorNonCodegen(name, allocator);
      }

      public TransferImpl(PrototypeIntVectorNonCodegen to) {
         this.to = to;
      }

      @Override
      public PrototypeIntVectorNonCodegen getTo(){
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
         to.copyFromSafe(fromIndex, toIndex, PrototypeIntVectorNonCodegen.this);
      }
   }

   public void copyFrom(int fromIndex, int thisIndex, PrototypeIntVectorNonCodegen from){
      data.setInt(thisIndex * 4, from.data.getInt(fromIndex * 4));
   }

   public void copyFromSafe(int fromIndex, int thisIndex, PrototypeIntVectorNonCodegen from){
      while(thisIndex >= getValueCapacity()) {
         reAlloc();
      }
      copyFrom(fromIndex, thisIndex, from);
   }

   public Accessor getAccessor() { return accessor; }

   public Mutator getMutator() { return mutator; }

   public final class Accessor extends BaseFixedWidthVector.BaseAccessor {
      @Override
      public int getValueCount() {
         return data.writerIndex() / 4;
      }

      /* need to do bound checking */
      public int get(int index) {
         return PlatformDependent.getInt(data.memoryAddress() + (index * 4));
      }

      public long getTwoAsLong(int index) {
         return data.getLong(index * 4);
      }

      @Override
      public Integer getObject(int index) {
         return get(index);
      }

      public int getPrimitiveObject(int index) {
         return get(index);
      }

      public void get(int index, IntHolder holder){
         holder.value = PlatformDependent.getInt(data.memoryAddress() + (index * 4));
      }

      public void get(int index, NullableIntHolder holder){
         holder.isSet = 1;
         holder.value = PlatformDependent.getInt(data.memoryAddress() + (index * 4));
      }
   }

   /**
    * Int.Mutator implements a mutable vector of fixed width values.  Elements in the
    * vector are accessed by position from the logical start of the vector.  Values should be pushed
    * onto the vector sequentially, but may be randomly accessed.
    *   The width of each element is 4 byte(s)
    *   The equivalent Java primitive is 'int'
    */
   public final class Mutator extends BaseFixedWidthVector.BaseMutator {

      private Mutator(){};

      /**
       * Set the element at the given index to the given value.  Note that widths smaller than
       * 32 bits are handled by the ArrowBuf interface.
       *
       * @param index   position of the bit to set
       * @param value   value to set
       */

      public void set(int index, int value) {
         data.setInt(index * 4, value);
      }

      public void setSafe(int index, int value) {
         while(index >= getValueCapacity()) {
            reAlloc();
         }
         set(index, value);
      }

      protected void set(int index, IntHolder holder){
         data.setInt(index * 4, holder.value);
      }

      public void setSafe(int index, IntHolder holder){
         while(index >= getValueCapacity()) {
            reAlloc();
         }
         set(index, holder);
      }

      protected void set(int index, NullableIntHolder holder){
         data.setInt(index * 4, holder.value);
      }

      public void setSafe(int index, NullableIntHolder holder){
         while(index >= getValueCapacity()) {
            reAlloc();
         }
         set(index, holder);
      }

      @Override
      public void generateTestData(int size) {
         setValueCount(size);
         boolean even = true;
         final int valueCount = getAccessor().getValueCount();
         for(int i = 0; i < valueCount; i++, even = !even) {
            if(even){
               set(i, Integer.MIN_VALUE);
            }else{
               set(i, Integer.MAX_VALUE);
            }
         }
      }

      public void generateTestDataAlt(int size) {
         setValueCount(size);
         boolean even = true;
         final int valueCount = getAccessor().getValueCount();
         for(int i = 0; i < valueCount; i++, even = !even) {
            if(even){
               set(i, 1);
            }else{
               set(i, 0);
            }
         }
      }

      @Override
      public void setValueCount(int valueCount) {
         final int currentValueCapacity = getValueCapacity();
         final int idx = (4 * valueCount);
         while(valueCount > getValueCapacity()) {
            reAlloc();
         }
         if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
            incrementAllocationMonitor();
         } else if (allocationMonitor > 0) {
            allocationMonitor = 0;
         }
         VectorTrimmer.trim(data, idx);
         data.writerIndex(valueCount * 4);
      }
   }
}
