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

package org.apache.arrow.c;

import static org.apache.arrow.c.NativeUtil.NULL;
import static org.apache.arrow.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.DataSizeRoundingUtil;

/**
 * Import buffers from a C Data Interface struct.
 */
class BufferImportTypeVisitor implements ArrowType.ArrowTypeVisitor<List<ArrowBuf>>, AutoCloseable {
  private final BufferAllocator allocator;
  private final ReferenceCountedArrowArray underlyingAllocation;
  private final ArrowFieldNode fieldNode;
  private final long[] buffers;
  private final List<ArrowBuf> imported;

  BufferImportTypeVisitor(BufferAllocator allocator, ReferenceCountedArrowArray underlyingAllocation,
                          ArrowFieldNode fieldNode, long[] buffers) {
    this.allocator = allocator;
    this.underlyingAllocation = underlyingAllocation;
    this.fieldNode = fieldNode;
    this.buffers = buffers;
    this.imported = new ArrayList<>();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(imported);
  }

  @VisibleForTesting
  long getBufferPtr(ArrowType type, int index) {
    checkState(
        buffers.length > index,
        "Expected at least %s buffers for type %s, but found %s", index + 1, type, buffers.length);
    if (buffers[index] == NULL) {
      throw new IllegalStateException(String.format("Buffer %s for type %s cannot be null", index, type));
    }
    return buffers[index];
  }

  private ArrowBuf importFixedBits(ArrowType type, int index, long bitsPerSlot) {
    final long bufferPtr = getBufferPtr(type, index);
    final long capacity = DataSizeRoundingUtil.divideBy8Ceil(bitsPerSlot * fieldNode.getLength());
    ArrowBuf buf = underlyingAllocation.unsafeAssociateAllocation(allocator, capacity, bufferPtr);
    this.imported.add(buf);
    return buf;
  }

  private ArrowBuf importFixedBytes(ArrowType type, int index, long bytesPerSlot) {
    final long bufferPtr = getBufferPtr(type, index);
    final long capacity = bytesPerSlot * fieldNode.getLength();
    ArrowBuf buf = underlyingAllocation.unsafeAssociateAllocation(allocator, capacity, bufferPtr);
    this.imported.add(buf);
    return buf;
  }

  private ArrowBuf importOffsets(ArrowType type, long bytesPerSlot) {
    final long bufferPtr = getBufferPtr(type, 1);
    final long capacity = bytesPerSlot * (fieldNode.getLength() + 1);
    ArrowBuf buf = underlyingAllocation.unsafeAssociateAllocation(allocator, capacity, bufferPtr);
    this.imported.add(buf);
    return buf;
  }

  private ArrowBuf importData(ArrowType type, long capacity) {
    final long bufferPtr = getBufferPtr(type, 2);
    ArrowBuf buf = underlyingAllocation.unsafeAssociateAllocation(allocator, capacity, bufferPtr);
    this.imported.add(buf);
    return buf;
  }

  private ArrowBuf maybeImportBitmap(ArrowType type) {
    checkState(
        buffers.length > 0,
        "Expected at least %s buffers for type %s, but found %s", 1, type, buffers.length);
    if (buffers[0] == NULL) {
      return null;
    }
    return importFixedBits(type, 0, /*bitsPerSlot=*/1);
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Null type) {
    checkState(
        buffers.length == 0,
        "Expected %s buffers for type %s, but found %s", 0, type, buffers.length);
    return Collections.emptyList();
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Struct type) {
    return Collections.singletonList(maybeImportBitmap(type));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.List type) {
    return Arrays.asList(maybeImportBitmap(type), importOffsets(type, ListVector.OFFSET_WIDTH));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.LargeList type) {
    return Arrays.asList(maybeImportBitmap(type), importOffsets(type, LargeListVector.OFFSET_WIDTH));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.FixedSizeList type) {
    return Collections.singletonList(maybeImportBitmap(type));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Union type) {
    switch (type.getMode()) {
      case Sparse:
        return Collections.singletonList(importFixedBytes(type, 0, UnionVector.TYPE_WIDTH));
      case Dense:
        return Arrays.asList(importFixedBytes(type, 0, DenseUnionVector.TYPE_WIDTH),
            importFixedBytes(type, 0, DenseUnionVector.OFFSET_WIDTH));
      default:
        throw new UnsupportedOperationException("Importing buffers for type: " + type);
    }
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Map type) {
    return Arrays.asList(maybeImportBitmap(type), importOffsets(type, MapVector.OFFSET_WIDTH));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Int type) {
    return Arrays.asList(maybeImportBitmap(type), importFixedBits(type, 1, type.getBitWidth()));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.FloatingPoint type) {
    switch (type.getPrecision()) {
      case HALF:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, /*bytesPerSlot=*/2));
      case SINGLE:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, Float4Vector.TYPE_WIDTH));
      case DOUBLE:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, Float8Vector.TYPE_WIDTH));
      default:
        throw new UnsupportedOperationException("Importing buffers for type: " + type);
    }
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Utf8 type) {
    try (ArrowBuf offsets = importOffsets(type, VarCharVector.OFFSET_WIDTH)) {
      final int start = offsets.getInt(0);
      final int end = offsets.getInt(fieldNode.getLength() * (long) VarCharVector.OFFSET_WIDTH);
      checkState(
          end >= start,
          "Offset buffer for type %s is malformed: start: %s, end: %s", type, start, end);
      final int len = end - start;
      offsets.getReferenceManager().retain();
      return Arrays.asList(maybeImportBitmap(type), offsets, importData(type, len));
    }
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.LargeUtf8 type) {
    try (ArrowBuf offsets = importOffsets(type, LargeVarCharVector.OFFSET_WIDTH)) {
      final long start = offsets.getLong(0);
      final long end = offsets.getLong(fieldNode.getLength() * (long) LargeVarCharVector.OFFSET_WIDTH);
      checkState(
          end >= start,
          "Offset buffer for type %s is malformed: start: %s, end: %s", type, start, end);
      final long len = end - start;
      offsets.getReferenceManager().retain();
      return Arrays.asList(maybeImportBitmap(type), offsets, importData(type, len));
    }
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Binary type) {
    try (ArrowBuf offsets = importOffsets(type, VarBinaryVector.OFFSET_WIDTH)) {
      final int start = offsets.getInt(0);
      final int end = offsets.getInt(fieldNode.getLength() * (long) VarBinaryVector.OFFSET_WIDTH);
      checkState(
          end >= start,
          "Offset buffer for type %s is malformed: start: %s, end: %s", type, start, end);
      final int len = end - start;
      offsets.getReferenceManager().retain();
      return Arrays.asList(maybeImportBitmap(type), offsets, importData(type, len));
    }
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.LargeBinary type) {
    try (ArrowBuf offsets = importOffsets(type, LargeVarBinaryVector.OFFSET_WIDTH)) {
      final long start = offsets.getLong(0);
      // TODO: need better tests to cover the failure when I forget to multiply by offset width
      final long end = offsets.getLong(fieldNode.getLength() * (long) LargeVarBinaryVector.OFFSET_WIDTH);
      checkState(
          end >= start,
          "Offset buffer for type %s is malformed: start: %s, end: %s", type, start, end);
      final long len = end - start;
      offsets.getReferenceManager().retain();
      return Arrays.asList(maybeImportBitmap(type), offsets, importData(type, len));
    }
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.FixedSizeBinary type) {
    return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, type.getByteWidth()));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Bool type) {
    return Arrays.asList(maybeImportBitmap(type), importFixedBits(type, 1, /*bitsPerSlot=*/1));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Decimal type) {
    return Arrays.asList(maybeImportBitmap(type), importFixedBits(type, 1, type.getBitWidth()));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Date type) {
    switch (type.getUnit()) {
      case DAY:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, DateDayVector.TYPE_WIDTH));
      case MILLISECOND:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, DateMilliVector.TYPE_WIDTH));
      default:
        throw new UnsupportedOperationException("Importing buffers for type: " + type);
    }
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Time type) {
    switch (type.getUnit()) {
      case SECOND:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, TimeSecVector.TYPE_WIDTH));
      case MILLISECOND:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, TimeMilliVector.TYPE_WIDTH));
      case MICROSECOND:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, TimeMicroVector.TYPE_WIDTH));
      case NANOSECOND:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, TimeNanoVector.TYPE_WIDTH));
      default:
        throw new UnsupportedOperationException("Importing buffers for type: " + type);
    }
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Timestamp type) {
    return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, TimeStampVector.TYPE_WIDTH));
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Interval type) {
    switch (type.getUnit()) {
      case YEAR_MONTH:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, IntervalYearVector.TYPE_WIDTH));
      case DAY_TIME:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, IntervalDayVector.TYPE_WIDTH));
      case MONTH_DAY_NANO:
        return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, IntervalMonthDayNanoVector.TYPE_WIDTH));
      default:
        throw new UnsupportedOperationException("Importing buffers for type: " + type);
    }
  }

  @Override
  public List<ArrowBuf> visit(ArrowType.Duration type) {
    return Arrays.asList(maybeImportBitmap(type), importFixedBytes(type, 1, DurationVector.TYPE_WIDTH));
  }
}
