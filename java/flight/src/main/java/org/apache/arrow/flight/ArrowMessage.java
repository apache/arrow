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

package org.apache.arrow.flight;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flight.grpc.AddWritableBuffer;
import org.apache.arrow.flight.grpc.GetReadableBuffer;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

import io.grpc.Drainable;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.protobuf.ProtoUtils;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * The in-memory representation of FlightData used to manage a stream of Arrow messages.
 */
class ArrowMessage implements AutoCloseable {

  public static final boolean FAST_PATH = true;

  private static final int DESCRIPTOR_TAG =
      (FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int BODY_TAG =
      (FlightData.DATA_BODY_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int HEADER_TAG =
      (FlightData.DATA_HEADER_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int APP_METADATA_TAG =
      (FlightData.APP_METADATA_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;

  private static Marshaller<FlightData> NO_BODY_MARSHALLER = ProtoUtils.marshaller(FlightData.getDefaultInstance());

  /** Get the application-specific metadata in this message. The ArrowMessage retains ownership of the buffer. */
  public ArrowBuf getApplicationMetadata() {
    return appMetadata;
  }

  /** Types of messages that can be sent. */
  public enum HeaderType {
    NONE,
    SCHEMA,
    DICTIONARY_BATCH,
    RECORD_BATCH,
    TENSOR
    ;

    public static HeaderType getHeader(byte b) {
      switch (b) {
        case 0: return NONE;
        case 1: return SCHEMA;
        case 2: return DICTIONARY_BATCH;
        case 3: return RECORD_BATCH;
        case 4: return TENSOR;
        default:
          throw new UnsupportedOperationException("unknown type: " + b);
      }
    }

  }

  // Pre-allocated buffers for padding serialized ArrowMessages.
  private static List<ByteBuf> PADDING_BUFFERS = Arrays.asList(
      null,
      Unpooled.copiedBuffer(new byte[] { 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0, 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0, 0, 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 0 })
  );

  private final FlightDescriptor descriptor;
  private final Message message;
  private final ArrowBuf appMetadata;
  private final List<ArrowBuf> bufs;

  public ArrowMessage(FlightDescriptor descriptor, Schema schema) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int schemaOffset = schema.getSchema(builder);
    ByteBuffer serializedMessage = MessageSerializer.serializeMessage(builder, MessageHeader.Schema, schemaOffset, 0);
    serializedMessage = serializedMessage.slice();
    message = Message.getRootAsMessage(serializedMessage);
    bufs = ImmutableList.of();
    this.descriptor = descriptor;
    this.appMetadata = null;
  }

  /**
   * Create an ArrowMessage from a record batch and app metadata.
   * @param batch The record batch.
   * @param appMetadata The app metadata. May be null. Takes ownership of the buffer otherwise.
   */
  public ArrowMessage(ArrowRecordBatch batch, ArrowBuf appMetadata) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int batchOffset = batch.writeTo(builder);
    ByteBuffer serializedMessage = MessageSerializer.serializeMessage(builder, MessageHeader.RecordBatch, batchOffset,
        batch.computeBodyLength());
    serializedMessage = serializedMessage.slice();
    this.message = Message.getRootAsMessage(serializedMessage);
    this.bufs = ImmutableList.copyOf(batch.getBuffers());
    this.descriptor = null;
    this.appMetadata = appMetadata;
  }

  public ArrowMessage(ArrowDictionaryBatch batch) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int batchOffset = batch.writeTo(builder);
    ByteBuffer serializedMessage = MessageSerializer
        .serializeMessage(builder, MessageHeader.DictionaryBatch, batchOffset,
            batch.computeBodyLength());
    serializedMessage = serializedMessage.slice();
    this.message = Message.getRootAsMessage(serializedMessage);
    // asInputStream will free the buffers implicitly, so increment the reference count
    batch.getDictionary().getBuffers().forEach(buf -> buf.getReferenceManager().retain());
    this.bufs = ImmutableList.copyOf(batch.getDictionary().getBuffers());
    this.descriptor = null;
    this.appMetadata = null;
  }

  private ArrowMessage(FlightDescriptor descriptor, Message message, ArrowBuf appMetadata, ArrowBuf buf) {
    this.message = message;
    this.descriptor = descriptor;
    this.appMetadata = appMetadata;
    this.bufs = buf == null ? ImmutableList.of() : ImmutableList.of(buf);
  }

  public FlightDescriptor getDescriptor() {
    return descriptor;
  }

  public HeaderType getMessageType() {
    return HeaderType.getHeader(message.headerType());
  }

  public Message asSchemaMessage() {
    return message;
  }

  public Schema asSchema() {
    Preconditions.checkArgument(bufs.size() == 0);
    Preconditions.checkArgument(getMessageType() == HeaderType.SCHEMA);
    org.apache.arrow.flatbuf.Schema schema = new org.apache.arrow.flatbuf.Schema();
    message.header(schema);
    return Schema.convertSchema(schema);
  }

  public ArrowRecordBatch asRecordBatch() throws IOException {
    Preconditions.checkArgument(bufs.size() == 1, "A batch can only be consumed if it contains a single ArrowBuf.");
    Preconditions.checkArgument(getMessageType() == HeaderType.RECORD_BATCH);
    RecordBatch recordBatch = new RecordBatch();
    message.header(recordBatch);
    ArrowBuf underlying = bufs.get(0);
    underlying.getReferenceManager().retain();
    ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(recordBatch, underlying);
    return batch;
  }

  public ArrowDictionaryBatch asDictionaryBatch() throws IOException {
    Preconditions.checkArgument(bufs.size() == 1, "A batch can only be consumed if it contains a single ArrowBuf.");
    Preconditions.checkArgument(getMessageType() == HeaderType.DICTIONARY_BATCH);
    ArrowBuf underlying = bufs.get(0);
    return MessageSerializer.deserializeDictionaryBatch(message, underlying);
  }

  public Iterable<ArrowBuf> getBufs() {
    return Iterables.unmodifiableIterable(bufs);
  }

  private static ArrowMessage frame(BufferAllocator allocator, final InputStream stream) {

    try {
      FlightDescriptor descriptor = null;
      Message header = null;
      ArrowBuf body = null;
      ArrowBuf appMetadata = null;
      while (stream.available() > 0) {
        int tag = readRawVarint32(stream);
        switch (tag) {

          case DESCRIPTOR_TAG: {
            int size = readRawVarint32(stream);
            byte[] bytes = new byte[size];
            ByteStreams.readFully(stream, bytes);
            descriptor = FlightDescriptor.parseFrom(bytes);
            break;
          }
          case HEADER_TAG: {
            int size = readRawVarint32(stream);
            byte[] bytes = new byte[size];
            ByteStreams.readFully(stream, bytes);
            header = Message.getRootAsMessage(ByteBuffer.wrap(bytes));
            break;
          }
          case APP_METADATA_TAG: {
            int size = readRawVarint32(stream);
            appMetadata = allocator.buffer(size);
            GetReadableBuffer.readIntoBuffer(stream, appMetadata, size, FAST_PATH);
            break;
          }
          case BODY_TAG:
            if (body != null) {
              // only read last body.
              body.getReferenceManager().release();
              body = null;
            }
            int size = readRawVarint32(stream);
            body = allocator.buffer(size);
            GetReadableBuffer.readIntoBuffer(stream, body, size, FAST_PATH);
            break;

          default:
            // ignore unknown fields.
        }
      }

      return new ArrowMessage(descriptor, header, appMetadata, body);
    } catch (Exception ioe) {
      throw new RuntimeException(ioe);
    }

  }

  private static int readRawVarint32(InputStream is) throws IOException {
    int firstByte = is.read();
    return CodedInputStream.readRawVarint32(firstByte, is);
  }

  /**
   * Convert the ArrowMessage to an InputStream.
   * @return InputStream
   */
  private InputStream asInputStream(BufferAllocator allocator) {
    try {

      final ByteString bytes = ByteString.copyFrom(message.getByteBuffer(), message.getByteBuffer().remaining());

      if (getMessageType() == HeaderType.SCHEMA) {

        final FlightData.Builder builder = FlightData.newBuilder()
            .setDataHeader(bytes);

        if (descriptor != null) {
          builder.setFlightDescriptor(descriptor);
        }

        Preconditions.checkArgument(bufs.isEmpty());
        return NO_BODY_MARSHALLER.stream(builder.build());
      }

      Preconditions.checkArgument(getMessageType() == HeaderType.RECORD_BATCH ||
          getMessageType() == HeaderType.DICTIONARY_BATCH);
      Preconditions.checkArgument(!bufs.isEmpty());
      Preconditions.checkArgument(descriptor == null, "Descriptor should only be included in the schema message.");

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      CodedOutputStream cos = CodedOutputStream.newInstance(baos);
      cos.writeBytes(FlightData.DATA_HEADER_FIELD_NUMBER, bytes);

      if (appMetadata != null && appMetadata.capacity() > 0) {
        // Must call slice() as CodedOutputStream#writeByteBuffer writes -capacity- bytes, not -limit- bytes
        cos.writeByteBuffer(FlightData.APP_METADATA_FIELD_NUMBER, appMetadata.asNettyBuffer().nioBuffer().slice());
        // This is weird, but implicitly, writing an ArrowMessage frees any references it has
        appMetadata.getReferenceManager().release();
      }

      cos.writeTag(FlightData.DATA_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
      int size = 0;
      List<ByteBuf> allBufs = new ArrayList<>();
      for (ArrowBuf b : bufs) {
        allBufs.add(b.asNettyBuffer());
        size += b.readableBytes();
        // [ARROW-4213] These buffers must be aligned to an 8-byte boundary in order to be readable from C++.
        if (b.readableBytes() % 8 != 0) {
          int paddingBytes = 8 - (b.readableBytes() % 8);
          assert paddingBytes > 0 && paddingBytes < 8;
          size += paddingBytes;
          allBufs.add(PADDING_BUFFERS.get(paddingBytes).retain());
        }
      }
      // rawvarint is used for length definition.
      cos.writeUInt32NoTag(size);
      cos.flush();

      ArrowBuf initialBuf = allocator.buffer(baos.size());
      initialBuf.writeBytes(baos.toByteArray());
      final CompositeByteBuf bb = new CompositeByteBuf(allocator.getAsByteBufAllocator(), true, bufs.size() + 1,
          ImmutableList.<ByteBuf>builder().add(initialBuf.asNettyBuffer()).addAll(allBufs).build());
      // Implicitly, transfer ownership of our buffers to the input stream (which will decrement the refcount when done)
      final ByteBufInputStream is = new DrainableByteBufInputStream(bb);
      return is;
    } catch (Exception ex) {
      throw new RuntimeException("Unexpected IO Exception", ex);
    }

  }

  private class DrainableByteBufInputStream extends ByteBufInputStream implements Drainable {

    private final CompositeByteBuf buf;

    public DrainableByteBufInputStream(CompositeByteBuf buffer) {
      super(buffer, buffer.readableBytes(), true);
      this.buf = buffer;
    }

    @Override
    public int drainTo(OutputStream target) throws IOException {
      int size = buf.readableBytes();
      if (FAST_PATH && AddWritableBuffer.add(buf, target)) {
        return size;
      }

      buf.getBytes(0, target, buf.readableBytes());
      return size;
    }

    @Override
    public void close() {
      buf.release();
    }



  }

  public static Marshaller<ArrowMessage> createMarshaller(BufferAllocator allocator) {
    return new ArrowMessageHolderMarshaller(allocator);
  }

  private static class ArrowMessageHolderMarshaller implements MethodDescriptor.Marshaller<ArrowMessage> {

    private final BufferAllocator allocator;

    public ArrowMessageHolderMarshaller(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public InputStream stream(ArrowMessage value) {
      return value.asInputStream(allocator);
    }

    @Override
    public ArrowMessage parse(InputStream stream) {
      return ArrowMessage.frame(allocator, stream);
    }

  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(bufs);
    if (appMetadata != null) {
      appMetadata.close();
    }
  }
}
