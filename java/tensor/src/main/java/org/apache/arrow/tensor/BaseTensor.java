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

package org.apache.arrow.tensor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Tensor;
import org.apache.arrow.flatbuf.TensorDim;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.google.flatbuffers.FlatBufferBuilder;

/** Base class for fixed-width dense tensors. */
public abstract class BaseTensor implements AutoCloseable {
  BufferAllocator allocator;
  ArrowType type;
  byte typeWidth;
  long[] shape;
  String[] names;
  long[] strides;
  ArrowBuf data;

  protected BaseTensor(BufferAllocator allocator, ArrowType type,
                       byte typeWidth, long[] shape, String[] names, long[] strides) {
    this.allocator = Objects.requireNonNull(allocator);
    this.type = type;
    this.typeWidth = typeWidth;
    this.shape = Objects.requireNonNull(shape);
    this.names = names;
    this.strides = Objects.requireNonNull(strides);
    // TODO: make a defensive copy of arrays

    for (int i = 0; i < strides.length; i++) {
      Preconditions.checkArgument(strides[i] >= typeWidth,
          "%sth stride %s must be at least type width %s", i, strides[i], typeWidth);
    }
    Preconditions.checkArgument(names == null || (shape.length == names.length),
        "Dimensions must match names");

    // Don't allocate until we pass preconditions
    final long bufferSize = minBufferSize(shape, strides);
    this.data = allocator.buffer(bufferSize);
    data.writerIndex(bufferSize);
  }

  /** Compute the minimum buffer size for the given tensor layout. */
  public static long minBufferSize(long[] shape, long[] strides) {
    Preconditions.checkArgument(shape.length == strides.length,
        "Dimensions (%s) must match strides (%s)", shape.length, strides.length);
    long maxStride = 0;
    int maxStrideIndex = 0;
    for (int i = 0; i < strides.length; i++) {
      if (strides[i] > maxStride) {
        maxStride = strides[i];
        maxStrideIndex = i;
      }
    }
    return maxStride * shape[maxStrideIndex];
  }

  /** Compute strides for a row-major tensor. */
  public static long[] rowMajorStrides(byte typeWidth, long[] shape) {
    long[] result = new long[shape.length];
    long stride = typeWidth;
    for (int i = shape.length - 1; i >= 0; i--) {
      result[i] = stride;
      stride *= shape[i];
    }
    return result;
  }

  /** Compute strides for a row-major tensor. */
  public static long[] columnMajorStrides(byte typeWidth, long[] shape) {
    long[] result = new long[shape.length];
    long stride = typeWidth;
    for (int i = 0; i < shape.length; i++) {
      result[i] = stride;
      stride *= shape[i];
    }
    return result;
  }

  protected void checkClosed() {
    if (shape.length > 0 && data.capacity() == 0) {
      throw new IllegalStateException("Operation on closed tensor");
    }
  }

  /** Get the Arrow type of the tensor elements. */
  public ArrowType getType() {
    return type;
  }

  /** Get the type width of the tensor elements. */
  public byte getTypeWidth() {
    return typeWidth;
  }

  /** Get the tensor dimensions. */
  public long[] getShape() {
    return shape;
  }

  /** Get the tensor element names. May be null. Individual names may also be null. */
  public String[] getNames() {
    return names;
  }

  /** Get the tensor strides. */
  public long[] getStrides() {
    return strides;
  }

  /** Get the tensor data buffer. */
  public ArrowBuf getDataBuffer() {
    return data;
  }

  /** Check if the tensor is in row-major (C) layout. */
  public boolean isRowMajor() {
    return Arrays.equals(rowMajorStrides(typeWidth, shape), strides);
  }

  /** Check if the tensor is in column-major (FORTRAN) layout. */
  public boolean isColumnMajor() {
    return Arrays.equals(columnMajorStrides(typeWidth, shape), strides);
  }

  /** Get the number of tensor elements. */
  public long getElementCount() {
    long result = shape.length > 0 ? 1 : 0;
    for (long dim : shape) {
      result *= dim;
    }
    return result;
  }

  /** Get the index into the data buffer for the given index. */
  public int getElementIndex(long[] indices) {
    Preconditions.checkArgument(indices.length == strides.length,
        "Indices (%s) must match strides (%s)", indices.length, strides.length);
    int index = 0;
    for (int i = 0; i < indices.length; i++) {
      long subIndex = indices[i];
      if (subIndex < 0 || subIndex >= shape[i]) {
        throw new IndexOutOfBoundsException(
            String.format("%d th index %d is out of bounds (max %d)", i, subIndex, shape[i]));
      }
      index += subIndex * strides[i];
    }
    return index;
  }

  /** Check if the given tensor has the same layout (dimensions and strides) as this tensor. */
  public boolean hasSameLayoutAs(BaseTensor other) {
    return typeWidth == other.typeWidth && Arrays.equals(shape, other.shape) && Arrays.equals(strides, other.strides);
  }

  /** Set the tensor elements to zero. */
  public void zeroTensor() {
    data.setZero(0, data.capacity());
  }

  /** Transfer the tensor buffer to the target. Afterwards, this tensor will be invalid. */
  public void transferTo(BaseTensor target) {
    if (!hasSameLayoutAs(target)) {
      throw new UnsupportedOperationException("Cannot transfer to tensor of different layout");
    }
    target.clear();
    target.data = data.getReferenceManager().transferOwnership(data, target.allocator).getTransferredBuffer();
    clear();
  }

  /** Get the given tensor element as an object. */
  abstract Object getObject(long[] indices);

  /** Free the tensor buffer. */
  public void clear() {
    data.getReferenceManager().release();
    data = allocator.getEmpty();
  }

  /** Construct the FlatBuffer metadata for this tensor. */
  public int getTensor(FlatBufferBuilder builder) {
    int typeOffset = type.getType(builder);

    int[] nameOffsets = names == null ? null : new int[names.length];
    if (nameOffsets != null) {
      for (int i = 0; i < names.length; i++) {
        nameOffsets[i] = builder.createString(names[i]);
      }
    }
    int[] dimOffsets = new int[shape.length];
    for (int i = 0; i < shape.length; i++) {
      TensorDim.startTensorDim(builder);
      TensorDim.addSize(builder, shape[i]);
      if (nameOffsets != null) {
        TensorDim.addName(builder, nameOffsets[i]);
      }
      dimOffsets[i] = TensorDim.endTensorDim(builder);
    }

    int shapeOffset = Tensor.createShapeVector(builder, dimOffsets);
    int stridesOffset = Tensor.createStridesVector(builder, strides);

    Tensor.startTensor(builder);
    Tensor.addType(builder, typeOffset);
    Tensor.addTypeType(builder, type.getTypeID().getFlatbufID());
    Tensor.addShape(builder, shapeOffset);
    Tensor.addStrides(builder, stridesOffset);
    Tensor.addData(builder, Buffer.createBuffer(builder, 0, data.capacity()));
    return Tensor.endTensor(builder);
  }

  /** Get the IPC messsage-encapsulated metadata for this tensor as a ByteBuffer. */
  public ByteBuffer getAsMessage(IpcOption option) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int tensorOffset = getTensor(builder);
    long bodyLength = minBufferSize(shape, strides);
    return MessageSerializer.serializeMessage(builder, MessageHeader.Tensor, tensorOffset, bodyLength, option);
  }

  /** Serialize this tensor to the given channel. */
  public void write(WriteChannel out, IpcOption option) throws IOException {
    ByteBuffer header = getAsMessage(option);
    MessageSerializer.writeMessageBuffer(out, header.remaining(), header, option);
    out.write(data);
    out.align();
  }

  /** Deserialize a tensor from the given channel. */
  public static BaseTensor read(ReadChannel in, BufferAllocator allocator) throws IOException {
    MessageMetadataResult result = MessageSerializer.readMessage(in);
    if (result == null) {
      throw new IOException("Unexpected end of input when reading Tensor");
    }
    if (result.getMessage().headerType() != MessageHeader.Tensor) {
      throw new IOException("Expected tensor but header was " + result.getMessage().headerType());
    }
    Message message = result.getMessage();
    long bodyLength = result.getMessageBodyLength();
    ArrowBuf bodyBuffer = MessageSerializer.readMessageBody(in, bodyLength, allocator);
    return read(message, bodyBuffer);
  }

  /** Deserialize a tensor from the IPC message and body. */
  public static BaseTensor read(Message message, ArrowBuf body) throws IOException {
    if (message.headerType() != MessageHeader.Tensor) {
      throw new IOException("Expected tensor but header was " + message.headerType());
    }
    Tensor tensorFb = (Tensor) message.header(new Tensor());
    if (tensorFb == null) {
      throw new IOException("Tensor metadata not present in message");
    }
    return read(tensorFb, body);
  }

  /** Deserialize a tensor from the tensor metadata and body. */
  private static BaseTensor read(Tensor tensorFb, ArrowBuf body) {
    long[] dims = new long[tensorFb.shapeLength()];
    String[] names = new String[tensorFb.shapeLength()];
    boolean hasNames = false;
    long[] strides = new long[tensorFb.stridesLength()];
    for (int i = 0; i < strides.length; i++) {
      strides[i] = tensorFb.strides(i);
    }
    TensorDim dim = new TensorDim();
    for (int i = 0; i < dims.length; i++) {
      tensorFb.shape(dim, i);
      if (dim.name() != null) {
        names[i] = dim.name();
        hasNames = true;
      }
      dims[i] = Math.toIntExact(dim.size()); // TODO:
    }
    final BaseTensor result = ArrowType.getTypeForField(tensorFb)
        .accept(new MakeTensorForArrowType(body, dims, hasNames, names, strides));
    result.clear();
    result.data = body;
    return result;
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append(getClass().getSimpleName());
    buf.append('[');
    for (int i = 0; i < shape.length; i++) {
      buf.append(shape[i]);
      if (i < shape.length - 1) {
        buf.append(", ");
      }
    }
    buf.append(']');
    return buf.toString();
  }

  private static class MakeTensorForArrowType extends ArrowType.PrimitiveTypeVisitor<BaseTensor> {
    private final ArrowBuf body;
    private final long[] dims;
    private final boolean hasNames;
    private final String[] names;
    private final long[] strides;

    MakeTensorForArrowType(ArrowBuf body, long[] dims, boolean hasNames, String[] names, long[] strides) {
      this.body = body;
      this.dims = dims;
      this.hasNames = hasNames;
      this.names = names;
      this.strides = strides;
    }

    @Override
    public BaseTensor visit(ArrowType.Null type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.Int type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.FloatingPoint type) {
      switch (type.getPrecision()) {
        case HALF:
          throw new UnsupportedOperationException();
        case SINGLE:
          throw new UnsupportedOperationException();
        case DOUBLE:
          return new Float8Tensor(body.getReferenceManager().getAllocator(), dims, hasNames ? names : null, strides);
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    public BaseTensor visit(ArrowType.Utf8 type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.LargeUtf8 type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.Binary type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.LargeBinary type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.FixedSizeBinary type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.Bool type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.Decimal type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.Date type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.Time type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.Timestamp type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.Interval type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseTensor visit(ArrowType.Duration type) {
      throw new UnsupportedOperationException();
    }
  }
}
