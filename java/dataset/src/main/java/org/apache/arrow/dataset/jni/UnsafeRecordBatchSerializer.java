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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.file.JniWrapper;
import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.NativeUnderlyingMemory;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowMessage;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.FBSerializable;
import org.apache.arrow.vector.ipc.message.FBSerializables;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

import com.google.flatbuffers.FlatBufferBuilder;

/**
 * A set of serialization utility methods against {@link org.apache.arrow.vector.ipc.message.ArrowRecordBatch}.
 *
 * <p>The utility should be used only in JNI case since the record batch
 * to serialize should keep alive during the life cycle of its deserialized
 * native record batch. We made this design for achieving zero-copy of
 * the buffer bodies.
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
   * @see UnsafeRecordBatchSerializer#serializeUnsafe(ArrowRecordBatch)
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
   * Deserialize from native serialized bytes to {@link ArrowRecordBatch} using flatbuffers.
   * The input byte array should be written from native code and of type
   * {@link Message}
   * in which a native buffer ID is required in custom metadata.
   *
   * @param allocator Allocator that the deserialized buffer should be associated with
   * @param bytes flatbuffers byte array
   * @return the deserialized record batch
   * @see NativeUnderlyingMemory
   */
  public static ArrowRecordBatch deserializeUnsafe(
      BufferAllocator allocator,
      byte[] bytes) {
    final ReadChannel metaIn = new ReadChannel(
        Channels.newChannel(new ByteArrayInputStream(bytes)));

    final Message metaMessage;
    try {
      final MessageMetadataResult result = MessageSerializer.readMessage(metaIn);
      Preconditions.checkNotNull(result);
      metaMessage = result.getMessage();
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize record batch metadata", e);
    }
    final RecordBatch batchMeta = (RecordBatch) metaMessage.header(new RecordBatch());
    Preconditions.checkNotNull(batchMeta);
    if (batchMeta.buffersLength() != metaMessage.customMetadataLength()) {
      throw new IllegalArgumentException("Buffer count mismatch between metadata and native managed refs");
    }

    final ArrayList<ArrowBuf> buffers = new ArrayList<>();
    for (int i = 0; i < batchMeta.buffersLength(); i++) {
      final Buffer bufferMeta = batchMeta.buffers(i);
      final KeyValue keyValue = metaMessage.customMetadata(i); // custom metadata containing native buffer refs
      final byte[] refDecoded = Base64.getDecoder().decode(keyValue.value());
      final long nativeBufferRef = ByteBuffer.wrap(refDecoded).order(ByteOrder.LITTLE_ENDIAN).getLong();
      final int size = LargeMemoryUtil.checkedCastToInt(bufferMeta.length());
      final NativeUnderlyingMemory am = NativeUnderlyingMemory.create(allocator,
          size, nativeBufferRef, bufferMeta.offset());
      BufferLedger ledger = am.associate(allocator);
      ArrowBuf buf = new ArrowBuf(ledger, null, size, bufferMeta.offset());
      buffers.add(buf);
    }

    try {
      final int numRows = LargeMemoryUtil.checkedCastToInt(batchMeta.length());
      final List<ArrowFieldNode> nodes = new ArrayList<>(batchMeta.nodesLength());
      for (int i = 0; i < batchMeta.nodesLength(); i++) {
        final FieldNode node = batchMeta.nodes(i);
        nodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
      }
      return new ArrowRecordBatch(numRows, nodes, buffers);
    } finally {
      buffers.forEach(buffer -> buffer.getReferenceManager().release());
    }
  }

  /**
   * Serialize from {@link ArrowRecordBatch} to flatbuffers bytes for native use. A cleaner callback
   * {@link TransferredReferenceCleaner} will be created for each individual serialized
   * buffer. The callback should be invoked once the buffer is collected from native code.
   * We use the callback to decrease reference count of Java side {@link ArrowBuf} here.
   *
   * @param batch input record batch
   * @return serialized bytes
   * @see TransferredReferenceCleaner
   */
  public static byte[] serializeUnsafe(ArrowRecordBatch batch) {
    final ArrowBodyCompression bodyCompression = batch.getBodyCompression();
    if (bodyCompression.getCodec() != NoCompressionCodec.COMPRESSION_TYPE) {
      throw new UnsupportedOperationException("Could not serialize compressed buffers");
    }

    final FlatBufferBuilder builder = new FlatBufferBuilder();
    List<ArrowBuf> buffers = batch.getBuffers();
    int[] metadataOffsets = new int[buffers.size() * 2];
    for (int i = 0, buffersSize = buffers.size(); i < buffersSize; i++) {
      ArrowBuf buffer = buffers.get(i);
      final TransferredReferenceCleaner cleaner = new TransferredReferenceCleaner(buffer);
      // cleaner object ref
      long objectRefValue = JniWrapper.get().newJniGlobalReference(cleaner);
      byte[] objectRefBytes = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN)
          .putLong(objectRefValue).array();
      metadataOffsets[i * 2] = KeyValue.createKeyValue(builder, builder.createString("JAVA_BUFFER_CO_REF_" + i),
          builder.createString(Base64.getEncoder().encodeToString(objectRefBytes)));
      // cleaner method ref
      long methodRefValue = TransferredReferenceCleaner.NATIVE_METHOD_REF;
      byte[] methodRefBytes = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN)
          .putLong(methodRefValue).array();
      metadataOffsets[i * 2 + 1] =
          KeyValue.createKeyValue(builder, builder.createString("JAVA_BUFFER_CM_REF_" + i),
              builder.createString(Base64.getEncoder().encodeToString(methodRefBytes)));
    }
    final ArrowMessage unsafeRecordMessage = new UnsafeRecordBatchMetadataMessage(batch);
    final int batchOffset = unsafeRecordMessage.writeTo(builder);
    final int customMetadataOffset = Message.createCustomMetadataVector(builder, metadataOffsets);
    Message.startMessage(builder);
    Message.addHeaderType(builder, unsafeRecordMessage.getMessageType());
    Message.addHeader(builder, batchOffset);
    Message.addVersion(builder, IpcOption.DEFAULT.metadataVersion.toFlatbufID());
    Message.addBodyLength(builder, unsafeRecordMessage.computeBodyLength());
    Message.addCustomMetadata(builder, customMetadataOffset);
    builder.finish(Message.endMessage(builder));
    final ByteBuffer metaBuffer = builder.dataBuffer();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      MessageSerializer.writeMessageBuffer(new WriteChannel(Channels.newChannel(out)), metaBuffer.remaining(),
          metaBuffer, IpcOption.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize Java record batch", e);
    }
    return out.toByteArray();
  }

  /**
   * IPC message for record batches that are based on unsafe shared virtual memory.
   */
  public static class UnsafeRecordBatchMetadataMessage implements ArrowMessage {
    private ArrowRecordBatch delegated;

    public UnsafeRecordBatchMetadataMessage(ArrowRecordBatch delegated) {
      this.delegated = delegated;
    }

    @Override
    public long computeBodyLength() {
      return 0L;
    }

    @Override
    public <T> T accepts(ArrowMessageVisitor<T> visitor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte getMessageType() {
      return MessageHeader.RecordBatch;
    }

    @Override
    public int writeTo(FlatBufferBuilder builder) {
      final List<ArrowFieldNode> nodes = delegated.getNodes();
      final List<ArrowBuf> buffers = delegated.getBuffers();
      final ArrowBodyCompression bodyCompression = delegated.getBodyCompression();
      final int length = delegated.getLength();
      RecordBatch.startNodesVector(builder, nodes.size());
      int nodesOffset = FBSerializables.writeAllStructsToVector(builder, nodes);
      RecordBatch.startBuffersVector(builder, buffers.size());
      int buffersOffset = FBSerializables.writeAllStructsToVector(builder, buffers.stream()
          .map(buf -> (FBSerializable) b -> Buffer.createBuffer(b, buf.memoryAddress(),
              buf.getReferenceManager().getSize()))
          .collect(Collectors.toList()));
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
    public void close() throws Exception {
      delegated.close();
    }
  }
}
