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

package org.apache.arrow.dataset.jni;

import java.util.ArrayList;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.file.JniWrapper;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.NativeUnderlyingMemory;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A set of serialization utility methods against {@link org.apache.arrow.vector.ipc.message.ArrowRecordBatch}.
 *
 * Mainly for JNI-related usages.
 */
public class UnsafeRecordBatchSerializer {

  /**
   * This is in response to native arrow::Buffer instance's destructor after
   * Java side {@link ArrowBuf} is transferred to native via JNI. At here we
   * think of a C++ transferred buffer holding one Java side buffer's reference
   * count so memory management system from Java side can be able to
   * know when native code finishes using the buffer. This way the corresponding
   * allocated memory space can be correctly collected.
   *
   * @see UnsafeRecordBatchSerializer#fromJavaManagedBatch(ArrowRecordBatch)
   */
  private static class TransferredReferenceCleaner implements Runnable {
    private static final long NATIVE_METHOD_REF =
        JniWrapper.get().newJniMethodReference("Ljava/lang/Runnable;", "run", "()V");
    private final ArrowBuf buf;

    private TransferredReferenceCleaner(ArrowBuf buf) {
      this.buf = buf;
    }

    @Override
    public void run() {
      buf.getReferenceManager().release();
    }
  }

  /**
   * Deserialize from native serialized bytes to {@link ArrowRecordBatch} using protobuf.
   * The input byte array should be written from native code and of type
   * {@link org.apache.arrow.dataset.jni.RecordBatchProtos.UnsafeNativeManagedRecordBatchProto}
   * in which a native buffer ID is required.
   *
   * @param allocator Allocator that the deserialized buffer should be associated with
   * @param bytes protobuf byte array
   * @return the deserialized record batch
   * @see NativeUnderlyingMemory
   */
  public static ArrowRecordBatch toNativeManagedBatch(
      BufferAllocator allocator,
      byte[] bytes) {
    final RecordBatchProtos.UnsafeNativeManagedRecordBatchProto batchProto;
    try {
      batchProto = RecordBatchProtos.UnsafeNativeManagedRecordBatchProto.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Deserialization failed. Please make sure " +
          "the byte array is ", e);
    }
    final ArrayList<ArrowBuf> buffers = new ArrayList<>();
    for (RecordBatchProtos.NativeManagedBufferProto buffer : batchProto.getBuffersList()) {
      final int size = LargeMemoryUtil.checkedCastToInt(buffer.getSize());
      final NativeUnderlyingMemory am = NativeUnderlyingMemory.create(allocator,
          size, buffer.getNativeBufferId(), buffer.getMemoryAddress());
      BufferLedger ledger = am.associate(allocator);
      ArrowBuf buf = new ArrowBuf(ledger, null, size, buffer.getMemoryAddress());
      buffers.add(buf);
    }

    try {
      final int numRows = LargeMemoryUtil.checkedCastToInt(batchProto.getNumRows());
      return new ArrowRecordBatch(numRows, batchProto.getFieldsList().stream()
          .map(field -> new ArrowFieldNode(field.getLength(), field.getNullCount()))
          .collect(Collectors.toList()), buffers);
    } finally {
      buffers.forEach(buffer -> buffer.getReferenceManager().release());
    }
  }

  /**
   * Serialize from {@link ArrowRecordBatch} to protobuf bytes for native use. A cleaner callback
   * {@link TransferredReferenceCleaner} will be created for each individual serialized
   * buffer. The callback should be invoked once the buffer is collected from native code.
   * We use the callback to decrease reference count of Java side {@link ArrowBuf} here.
   *
   * @param batch input record batch
   * @return serialized bytes
   * @see TransferredReferenceCleaner
   */
  public static byte[] fromJavaManagedBatch(ArrowRecordBatch batch) {
    RecordBatchProtos.UnsafeJavaManagedRecordBatchProto.Builder builder
        = RecordBatchProtos.UnsafeJavaManagedRecordBatchProto.newBuilder()
        .setNumRows(batch.getLength());

    ArrowBodyCompression bodyCompression = batch.getBodyCompression();

    if (bodyCompression.getCodec() != NoCompressionCodec.COMPRESSION_TYPE) {
      throw new UnsupportedOperationException("Could not serialize compressed buffers");
    }

    for (ArrowFieldNode node : batch.getNodes()) {
      builder.addFields(RecordBatchProtos.FieldProto.newBuilder()
          .setLength(node.getLength())
          .setNullCount(node.getNullCount())
          .build());
    }
    for (ArrowBuf buffer : batch.getBuffers()) {
      final TransferredReferenceCleaner cleaner = new TransferredReferenceCleaner(buffer);
      builder.addBuffers(RecordBatchProtos.JavaManagedBufferProto.newBuilder()
          .setMemoryAddress(buffer.memoryAddress())
          .setSize(buffer.getReferenceManager().getSize())
          .setCapacity(buffer.capacity())
          .setCleanerObjectRef(JniWrapper.get().newJniGlobalReference(cleaner))
          .setCleanerMethodRef(TransferredReferenceCleaner.NATIVE_METHOD_REF)
          .build());
    }
    final RecordBatchProtos.UnsafeJavaManagedRecordBatchProto batchProto = builder.build();
    return batchProto.toByteArray();
  }
}
