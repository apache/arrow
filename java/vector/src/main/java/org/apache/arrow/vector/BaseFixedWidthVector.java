package org.apache.arrow.vector;

import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.OversizedAllocationException;

import java.util.Collections;
import java.util.Iterator;

public abstract class BaseFixedWidthVector implements FixedWidthVector {
    public static final String MAX_ALLOCATION_SIZE_PROPERTY = "arrow.vector.max_allocation_bytes";
    public static final int MAX_ALLOCATION_SIZE = Integer.getInteger(MAX_ALLOCATION_SIZE_PROPERTY, Integer.MAX_VALUE);
    public static final int INITIAL_VALUE_ALLOCATION = 4096;

    protected ArrowBuf data;
    protected final String name;
    protected int allocationMonitor;

    private final BufferAllocator allocator;
    private final int typeWidth;
    private int allocationSizeInBytes;

    public BaseFixedWidthVector(final String name, final BufferAllocator allocator, final int typeWidth ) {
        this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
        this.name = name;
        this.data = allocator.getEmpty();
        this.typeWidth = typeWidth;
        this.allocationSizeInBytes = INITIAL_VALUE_ALLOCATION * typeWidth;
        this.allocationMonitor = 0;
    }

    @Override
    public BufferAllocator getAllocator() { return  allocator; }

    @Override
    public void clear() {
        data.release();
        data = allocator.getEmpty();
    }

    @Override
    public void close() {
        clear();
    }

    @Override
    public Field getField() {
        throw new UnsupportedOperationException("this operation is supported only for field vectors");
    }

    @Override
    public FieldReader getReader(){
        throw new UnsupportedOperationException("this operation is supported only for field vectors");
    }

    public void reset() {
        allocationMonitor = 0;
        zeroVector();
    }

    @Override
    public int getValueCapacity(){
        return (int) (data.capacity() *1.0 / typeWidth);
    }

    @Override
    public ArrowBuf[] getBuffers(boolean clear) {
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
    public int getBufferSize() {
        if (getAccessor().getValueCount() == 0) {
            return 0;
        }
        return data.writerIndex();
    }

    @Override
    public int getBufferSizeFor(final int valueCount) {
        if (valueCount == 0) {
            return 0;
        }
        return valueCount * typeWidth;
    }

    @Override
    public void setInitialCapacity(final int valueCount) {
        final long size = 1L * valueCount * typeWidth;
        if (size > MAX_ALLOCATION_SIZE) {
            throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
        }
        allocationSizeInBytes = (int)size;
    }

    @Override
    public void allocateNew() {
        if(!allocateNewSafe()){
            throw new OutOfMemoryException("Failure while allocating buffer.");
        }
    }

    @Override
    public boolean allocateNewSafe() {
        long curAllocationSize = allocationSizeInBytes;
        if (allocationMonitor > 10) {
            curAllocationSize = Math.max(8, curAllocationSize / 2);
            allocationMonitor = 0;
        } else if (allocationMonitor < -2) {
            curAllocationSize = allocationSizeInBytes * 2L;
            allocationMonitor = 0;
        }

        try{
            allocateBytes(curAllocationSize);
        } catch (RuntimeException ex) {
            return false;
        }
        return true;
    }

    /**
     * Allocate a new buffer that supports setting at least the provided number of values. May actually be sized bigger
     * depending on underlying buffer rounding size. Must be called prior to using the ValueVector.
     *
     * Note that the maximum number of values a vector can allocate is Integer.MAX_VALUE / value width.
     *
     * @param valueCount the number of values to allocate for
     * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the new buffer
     */
    @Override
    public void allocateNew(final int valueCount) {
        allocateBytes(valueCount * typeWidth);
    }

    private void allocateBytes(final long size) {
        if (size > MAX_ALLOCATION_SIZE) {
            throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
        }

        final int curSize = (int)size;
        clear();
        data = allocator.buffer(curSize);
        data.readerIndex(0);
        allocationSizeInBytes = curSize;
    }

    /**
     * Allocate new buffer with double capacity, and copy data into the new buffer. Replace vector's buffer with new buffer, and release old one
     *
     * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the new buffer
     */
    public void reAlloc() {
        long baseSize  = allocationSizeInBytes;
        final int currentBufferCapacity = data.capacity();
        if (baseSize < (long)currentBufferCapacity) {
            baseSize = (long)currentBufferCapacity;
        }
        long newAllocationSize = baseSize * 2L;
        newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);

        if (newAllocationSize > MAX_ALLOCATION_SIZE) {
            throw new OversizedAllocationException("Unable to expand the buffer. Max allowed buffer size is reached.");
        }

        final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
        newBuf.setBytes(0, data, 0, currentBufferCapacity);
        final int halfNewCapacity = newBuf.capacity() / 2;
        newBuf.setZero(halfNewCapacity, halfNewCapacity);
        newBuf.writerIndex(data.writerIndex());
        data.release(1);
        data = newBuf;
        allocationSizeInBytes = (int)newAllocationSize;
    }

    @Override
    public void zeroVector() {
        data.setZero(0, data.capacity());
    }

    public void decrementAllocationMonitor() {
        if (allocationMonitor > 0) {
            allocationMonitor = 0;
        }
        --allocationMonitor;
    }

    public void incrementAllocationMonitor() {
        ++allocationMonitor;
    }

    @Override
    public ArrowBuf getValidityBuffer() {
        throw new UnsupportedOperationException("this operation is not supported for non-nullable vectors");
    }

    @Override
    public ArrowBuf getDataBuffer() {
        return data;
    }

    @Override
    public ArrowBuf getOffsetBuffer() {
        throw new UnsupportedOperationException("this operation is not supported for fixed width vectors");
    }

    public void transferTo(BaseFixedWidthVector target){
        compareTypes(target, "transferTo");
        target.clear();
        target.data = data.transferOwnership(target.allocator).buffer;
        target.data.writerIndex(data.writerIndex());
        clear();
    }

    public void splitAndTransferTo(int startIndex, int length, BaseFixedWidthVector target) {
        compareTypes(target, "splitAndTransferTo");
        final int startPoint = startIndex * typeWidth;
        final int sliceLength = length * typeWidth;
        target.clear();
        target.data = data.slice(startPoint, sliceLength).transferOwnership(target.allocator).buffer;
        target.data.writerIndex(sliceLength);
    }

    private void compareTypes(BaseFixedWidthVector target, String caller) {
        if (this.getMinorType() != target.getMinorType()) {
            throw new UnsupportedOperationException(caller + " should be used between vectors of exact same type");
        }
    }

    @Override
    public Iterator<ValueVector> iterator() {
        return Collections.emptyIterator();
    }

    public abstract static class BaseAccessor implements ValueVector.Accessor {
        @Override
        public boolean isNull(int index) {
            return false;
        }

        @Override
        public int getNullCount() { return 0; }
    }

    public abstract static class BaseMutator implements ValueVector.Mutator {
        @Override
        public void generateTestData(int values) { }

        @Override
        public void reset() { }
    }
}
