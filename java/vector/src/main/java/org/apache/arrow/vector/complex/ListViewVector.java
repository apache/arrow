package org.apache.arrow.vector.complex;

import static java.util.Collections.singletonList;

import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

public class ListViewVector extends BaseRepeatedValueViewVector implements PromotableVector {

  protected ArrowBuf validityBuffer;
  protected UnionListReader reader;
  private CallBack callBack;
  protected Field field;
  protected int validityAllocationSizeInBytes;

  /**
   * The maximum index that is actually set.
   */
  protected int lastSet;

  public static ListViewVector empty(String name, BufferAllocator allocator) {
    return new ListViewVector(name, allocator, FieldType.nullable(ArrowType.List.INSTANCE), null);
  }

  public ListViewVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    this(new Field(name, fieldType, null), allocator, callBack);
  }

  public  ListViewVector(Field field, BufferAllocator allocator, CallBack callBack) {
    super(field.getName(), allocator, callBack);
    this.validityBuffer = allocator.getEmpty();
    this.field = field;
    this.callBack = callBack;
    this.validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
    this.lastSet = -1;
  }



  @Override
  public void initializeChildrenFromFields(List<Field> children) {

  }

  @Override
  public void setInitialCapacity(int numRecords) {

  }

  @Override
  public void setInitialCapacity(int numRecords, double density) {

  }

  @Override
  public void setInitialTotalCapacity(int numRecords, int totalNumberOfElements) {
    super.setInitialTotalCapacity(numRecords, totalNumberOfElements);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return singletonList(getDataVector());
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {

  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return null;
  }

  @Override
  public void exportCDataBuffers(List<ArrowBuf> buffers, ArrowBuf buffersPtr, long nullValue) {

  }

  @Override
  public void allocateNew() throws OutOfMemoryException {

  }

  @Override
  public boolean allocateNewSafe() {
    boolean success = false;
    return success;
  }

  @Override
  public void reAlloc() {

  }

  @Override
  public void copyFromSafe(int inIndex, int outIndex, ValueVector from) {
  }

  @Override
  public void copyFrom(int inIndex, int outIndex, ValueVector from) {

  }

  @Override
  public FieldVector getDataVector() {
    return vector;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return getTransferPair(ref, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return getTransferPair(field, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getValidityBufferAddress() {
    return validityBuffer.memoryAddress();
  }

  @Override
  public long getDataBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOffsetBufferAddress() {
    return offsetBuffer.memoryAddress();
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    return validityBuffer;
  }

  @Override
  public ArrowBuf getDataBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    return offsetBuffer;
  }

  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    return 0;
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected FieldReader getReaderImpl() {
    throw new UnsupportedOperationException();
  }

  @Override
  public UnionListReader getReader() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBufferSize() {
    return 0;
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    return 0;
  }

  @Override
  public Field getField() { return null; }

  @Override
  public MinorType getMinorType() {
    return MinorType.LIST;
  }

  @Override
  public void clear() {
  }

  @Override
  public void reset() {
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return new ArrowBuf[0];
  }

  @Override
  public List<?> getObject(int index) {
    return null;
  }

  @Override
  public boolean isNull(int index) {
    return false;
  }

  @Override
  public boolean isEmpty(int index) {
    return false;
  }

  @Override
  public int getNullCount() {
    return 0;
  }

  @Override
  public int getValueCapacity() {
    return 0;
  }

  @Override
  public void setNull(int index) {

  }

  @Override
  public int startNewValue(int index) {
    return 0;
  }

  @Override
  public void setValueCount(int valueCount) {

  }

  @Override
  public int getElementStartIndex(int index) {
    return 0;
  }

  @Override
  public int getElementEndIndex(int index) {
    return 0;
  }

  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType type) {
    return null;
  }

  @Override
  public UnionVector promoteToUnion() {
    return null;
  }

  @Deprecated
  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }


}
