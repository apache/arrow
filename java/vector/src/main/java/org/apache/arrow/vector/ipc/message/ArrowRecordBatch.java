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

package org.apache.arrow.vector.ipc.message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.util.DataSizeRoundingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.flatbuffers.FlatBufferBuilder;

/**
 * POJO representation of a RecordBatch IPC message (https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message).
 */
public class ArrowRecordBatch implements ArrowMessage {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowRecordBatch.class);

  /**
   * Number of records.
   */
  private final int length;

  /**
   * Nodes correspond to the pre-ordered flattened logical schema.
   */
  private final List<ArrowFieldNode> nodes;

  private final List<ArrowBuf> buffers;

  private final ArrowBodyCompression bodyCompression;

  private final List<ArrowBuffer> buffersLayout;

  private boolean closed = false;

  public ArrowRecordBatch(
      int length, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
    this(length, nodes, buffers, NoCompressionCodec.DEFAULT_BODY_COMPRESSION, true);
  }

  public ArrowRecordBatch(
      int length, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers,
      ArrowBodyCompression bodyCompression) {
    this(length, nodes, buffers, bodyCompression, true);
  }

  /**
   * Construct a record batch from nodes.
   *
   * @param length  how many rows in this batch
   * @param nodes   field level info
   * @param buffers will be retained until this recordBatch is closed
   * @param bodyCompression compression info.
   * @param alignBuffers Whether to align buffers to an 8 byte boundary.
   */
  public ArrowRecordBatch(
      int length, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers,
      ArrowBodyCompression bodyCompression, boolean alignBuffers) {
    this(length, nodes, buffers, bodyCompression, alignBuffers, /*retainBuffers*/ true);
  }

  /**
   * Construct a record batch from nodes.
   *
   * @param length  how many rows in this batch
   * @param nodes   field level info
   * @param buffers will be retained until this recordBatch is closed
   * @param bodyCompression compression info.
   * @param alignBuffers Whether to align buffers to an 8 byte boundary.
   * @param retainBuffers Whether to retain() each source buffer in the constructor. If false, the caller is
   *                      responsible for retaining the buffers beforehand.
   */
  public ArrowRecordBatch(
      int length, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers,
      ArrowBodyCompression bodyCompression, boolean alignBuffers, boolean retainBuffers) {
    super();
    this.length = length;
    this.nodes = nodes;
    this.buffers = buffers;
    Preconditions.checkArgument(bodyCompression != null, "body compression cannot be null");
    this.bodyCompression = bodyCompression;
    List<ArrowBuffer> arrowBuffers = new ArrayList<>(buffers.size());
    long offset = 0;
    for (ArrowBuf arrowBuf : buffers) {
      if (retainBuffers) {
        arrowBuf.getReferenceManager().retain();
      }
      long size = arrowBuf.readableBytes();
      arrowBuffers.add(new ArrowBuffer(offset, size));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Buffer in RecordBatch at {}, length: {}", offset, size);
      }
      offset += size;
      if (alignBuffers) { // align on 8 byte boundaries
        offset = DataSizeRoundingUtil.roundUpTo8Multiple(offset);
      }
    }
    this.buffersLayout = Collections.unmodifiableList(arrowBuffers);
  }

  // clone constructor
  // this constructor is different from the public ones in that the reference manager's
  // <code>retain</code> method is not called, so the first <code>dummy</code> parameter is used
  // to distinguish this from the public constructor.
  private ArrowRecordBatch(
      boolean dummy, int length, List<ArrowFieldNode> nodes,
      List<ArrowBuf> buffers, ArrowBodyCompression bodyCompression) {
    this.length = length;
    this.nodes = nodes;
    this.buffers = buffers;
    Preconditions.checkArgument(bodyCompression != null, "body compression cannot be null");
    this.bodyCompression = bodyCompression;
    this.closed = false;
    List<ArrowBuffer> arrowBuffers = new ArrayList<>();
    long offset = 0;
    for (ArrowBuf arrowBuf : buffers) {
      long size = arrowBuf.readableBytes();
      arrowBuffers.add(new ArrowBuffer(offset, size));
      offset += size;
    }
    this.buffersLayout = Collections.unmodifiableList(arrowBuffers);
  }

  public byte getMessageType() {
    return org.apache.arrow.flatbuf.MessageHeader.RecordBatch;
  }

  public int getLength() {
    return length;
  }

  public ArrowBodyCompression getBodyCompression() {
    return bodyCompression;
  }

  /**
   * Get the nodes in this record batch.
   *
   * @return the FieldNodes corresponding to the schema
   */
  public List<ArrowFieldNode> getNodes() {
    return nodes;
  }

  /**
   * Get the record batch buffers.
   *
   * @return the buffers containing the data
   */
  public List<ArrowBuf> getBuffers() {
    if (closed) {
      throw new IllegalStateException("already closed");
    }
    return buffers;
  }

  /**
   * Create a new ArrowRecordBatch which has the same information as this batch but whose buffers
   * are owned by that Allocator.
   *
   * <p>This will also close this record batch and make it no longer useful.
   *
   * @return A cloned ArrowRecordBatch
   */
  public ArrowRecordBatch cloneWithTransfer(final BufferAllocator allocator) {
    final List<ArrowBuf> newBufs = buffers.stream()
        .map(buf ->
          (buf.getReferenceManager().transferOwnership(buf, allocator)
            .getTransferredBuffer())
            .writerIndex(buf.writerIndex()))
        .collect(Collectors.toList());
    close();
    return new ArrowRecordBatch(false, length, nodes, newBufs, bodyCompression);
  }

  /**
   * Get the serialized layout.
   *
   * @return the serialized layout if we send the buffers on the wire
   */
  public List<ArrowBuffer> getBuffersLayout() {
    return buffersLayout;
  }

  @Override
  public int writeTo(FlatBufferBuilder builder) {
    RecordBatch.startNodesVector(builder, nodes.size());
    int nodesOffset = FBSerializables.writeAllStructsToVector(builder, nodes);
    RecordBatch.startBuffersVector(builder, buffers.size());
    int buffersOffset = FBSerializables.writeAllStructsToVector(builder, buffersLayout);
    int compressOffset = 0;
    if (bodyCompression.getCodec() != NoCompressionCodec.COMPRESSION_TYPE) {
      compressOffset = bodyCompression.writeTo(builder);
    }
    RecordBatch.startRecordBatch(builder);
    RecordBatch.addLength(builder, length);
    RecordBatch.addNodes(builder, nodesOffset);
    RecordBatch.addBuffers(builder, buffersOffset);
    if (bodyCompression.getCodec() != NoCompressionCodec.COMPRESSION_TYPE) {
      RecordBatch.addCompression(builder, compressOffset);
    }
    return RecordBatch.endRecordBatch(builder);
  }

  @Override
  public <T> T accepts(ArrowMessageVisitor<T> visitor) {
    return visitor.visit(this);
  }

  /**
   * Releases the buffers.
   */
  @Override
  public void close() {
    if (!closed) {
      closed = true;
      for (ArrowBuf arrowBuf : buffers) {
        arrowBuf.getReferenceManager().release();
      }
    }
  }

  @Override
  public String toString() {
    return "ArrowRecordBatch [length=" + length + ", nodes=" + nodes + ", #buffers=" + buffers.size() +
      ", buffersLayout=" + buffersLayout + ", closed=" + closed + "]";
  }

  /**
   * Computes the size of the serialized body for this recordBatch.
   */
  @Override
  public long computeBodyLength() {
    long size = 0;

    List<ArrowBuf> buffers = getBuffers();
    List<ArrowBuffer> buffersLayout = getBuffersLayout();
    if (buffers.size() != buffersLayout.size()) {
      throw new IllegalStateException("the layout does not match: " +
          buffers.size() + " != " + buffersLayout.size());
    }

    for (int i = 0; i < buffers.size(); i++) {
      ArrowBuf buffer = buffers.get(i);
      ArrowBuffer layout = buffersLayout.get(i);
      size = layout.getOffset() + buffer.readableBytes();

      // round up size to the next multiple of 8
      size = DataSizeRoundingUtil.roundUpTo8Multiple(size);
    }
    return size;
  }

}
