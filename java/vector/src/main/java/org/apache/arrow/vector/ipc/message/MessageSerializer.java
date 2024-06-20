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

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;

import com.google.flatbuffers.FlatBufferBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.DictionaryBatch;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Utility class for serializing Messages. Messages are all serialized a similar way. 1. 4 byte
 * little endian message header prefix 2. FB serialized Message: This includes it the body length,
 * which is the serialized body and the type of the message. 3. Serialized message.
 *
 * <p>For schema messages, the serialization is simply the FB serialized Schema.
 *
 * <p>For RecordBatch messages the serialization is: 1. 4 byte little endian batch metadata header
 * 2. FB serialized RowBatch 3. Padding to align to 8 byte boundary. 4. serialized RowBatch buffers.
 */
public class MessageSerializer {

  // This 0xFFFFFFFF value is the first 4 bytes of a valid IPC message
  public static final int IPC_CONTINUATION_TOKEN = -1;

  /**
   * Convert an array of 4 bytes in little-endian to an native-endian i32 value.
   *
   * @param bytes byte array with minimum length of 4 in little-endian
   * @return converted an native-endian 32-bit integer
   */
  public static int bytesToInt(byte[] bytes) {
    return ((bytes[3] & 255) << 24)
        + ((bytes[2] & 255) << 16)
        + ((bytes[1] & 255) << 8)
        + ((bytes[0] & 255));
  }

  /**
   * Convert an integer to a little endian 4 byte array.
   *
   * @param value integer value input
   * @param bytes existing byte array with minimum length of 4 to contain the conversion output
   */
  public static void intToBytes(int value, byte[] bytes) {
    bytes[3] = (byte) (value >>> 24);
    bytes[2] = (byte) (value >>> 16);
    bytes[1] = (byte) (value >>> 8);
    bytes[0] = (byte) (value);
  }

  /**
   * Convert a long to a little-endian 8 byte array.
   *
   * @param value long value input
   * @param bytes existing byte array with minimum length of 8 to contain the conversion output
   */
  public static void longToBytes(long value, byte[] bytes) {
    bytes[7] = (byte) (value >>> 56);
    bytes[6] = (byte) (value >>> 48);
    bytes[5] = (byte) (value >>> 40);
    bytes[4] = (byte) (value >>> 32);
    bytes[3] = (byte) (value >>> 24);
    bytes[2] = (byte) (value >>> 16);
    bytes[1] = (byte) (value >>> 8);
    bytes[0] = (byte) (value);
  }

  public static int writeMessageBuffer(
      WriteChannel out, int messageLength, ByteBuffer messageBuffer) throws IOException {
    return writeMessageBuffer(out, messageLength, messageBuffer, IpcOption.DEFAULT);
  }

  /**
   * Write the serialized Message metadata, prefixed by the length, to the output Channel. This
   * ensures that it aligns to an 8 byte boundary and will adjust the message length to include any
   * padding used for alignment.
   *
   * @param out Output Channel
   * @param messageLength Number of bytes in the message buffer, written as little Endian prefix
   * @param messageBuffer Message metadata buffer to be written, this does not include any message
   *     body data which should be subsequently written to the Channel
   * @param option IPC write options
   * @return Number of bytes written
   * @throws IOException on error
   */
  public static int writeMessageBuffer(
      WriteChannel out, int messageLength, ByteBuffer messageBuffer, IpcOption option)
      throws IOException {

    // if write the pre-0.15.0 encapsulated IPC message format consisting of a 4-byte prefix instead
    // of 8 byte
    int prefixSize = option.write_legacy_ipc_format ? 4 : 8;

    // ensure that message aligns to 8 byte padding - prefix_size bytes, then message body
    if ((messageLength + prefixSize) % 8 != 0) {
      messageLength += 8 - (messageLength + prefixSize) % 8;
    }
    if (!option.write_legacy_ipc_format) {
      out.writeIntLittleEndian(IPC_CONTINUATION_TOKEN);
    }
    out.writeIntLittleEndian(messageLength);
    out.write(messageBuffer);
    out.align();

    // any bytes written are already captured by our size modification above
    return messageLength + prefixSize;
  }

  /** Serialize a schema object. */
  public static long serialize(WriteChannel out, Schema schema) throws IOException {
    return serialize(out, schema, IpcOption.DEFAULT);
  }

  /**
   * Serialize a schema object.
   *
   * @param out where to write the schema
   * @param schema the object to serialize to out
   * @return the number of bytes written
   * @throws IOException if something went wrong
   */
  public static long serialize(WriteChannel out, Schema schema, IpcOption option)
      throws IOException {
    long start = out.getCurrentPosition();
    Preconditions.checkArgument(start % 8 == 0, "out is not aligned");

    ByteBuffer serializedMessage = serializeMetadata(schema, option);

    int messageLength = serializedMessage.remaining();

    int bytesWritten = writeMessageBuffer(out, messageLength, serializedMessage, option);
    Preconditions.checkArgument(bytesWritten % 8 == 0, "out is not aligned");
    return bytesWritten;
  }

  /** Returns the serialized flatbuffer bytes of the schema wrapped in a message table. */
  @Deprecated
  public static ByteBuffer serializeMetadata(Schema schema) {
    return serializeMetadata(schema, IpcOption.DEFAULT);
  }

  /** Returns the serialized flatbuffer bytes of the schema wrapped in a message table. */
  public static ByteBuffer serializeMetadata(Schema schema, IpcOption writeOption) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int schemaOffset = schema.getSchema(builder);
    return MessageSerializer.serializeMessage(
        builder, org.apache.arrow.flatbuf.MessageHeader.Schema, schemaOffset, 0, writeOption);
  }

  /**
   * Deserializes an Arrow Schema object from a schema message. Format is from serialize().
   *
   * @param schemaMessage a Message of type MessageHeader.Schema
   * @return the deserialized Arrow Schema
   */
  public static Schema deserializeSchema(Message schemaMessage) {
    Preconditions.checkArgument(
        schemaMessage.headerType() == MessageHeader.Schema,
        "Expected schema but result was:  %s",
        schemaMessage.headerType());
    return Schema.convertSchema(
        (org.apache.arrow.flatbuf.Schema)
            schemaMessage.header(new org.apache.arrow.flatbuf.Schema()));
  }

  /**
   * Deserializes an Arrow Schema read from the input channel. Format is from serialize().
   *
   * @param in the channel to deserialize from
   * @return the deserialized Arrow Schema
   * @throws IOException if something went wrong
   */
  public static Schema deserializeSchema(ReadChannel in) throws IOException {
    MessageMetadataResult result = readMessage(in);
    if (result == null) {
      throw new IOException("Unexpected end of input when reading Schema");
    }
    if (result.getMessage().headerType() != MessageHeader.Schema) {
      throw new IOException("Expected schema but header was " + result.getMessage().headerType());
    }
    return deserializeSchema(result);
  }

  /**
   * Deserializes an Arrow Schema object from a {@link MessageMetadataResult}. Format is from
   * serialize().
   *
   * @param message a Message of type MessageHeader.Schema
   * @return the deserialized Arrow Schema
   */
  public static Schema deserializeSchema(MessageMetadataResult message) {
    return deserializeSchema(message.getMessage());
  }

  /** Serializes an ArrowRecordBatch. Returns the offset and length of the written batch. */
  public static ArrowBlock serialize(WriteChannel out, ArrowRecordBatch batch) throws IOException {
    return serialize(out, batch, IpcOption.DEFAULT);
  }

  /**
   * Serializes an ArrowRecordBatch. Returns the offset and length of the written batch.
   *
   * @param out where to write the batch
   * @param batch the object to serialize to out
   * @return the serialized block metadata
   * @throws IOException if something went wrong
   */
  public static ArrowBlock serialize(WriteChannel out, ArrowRecordBatch batch, IpcOption option)
      throws IOException {

    long start = out.getCurrentPosition();
    long bodyLength = batch.computeBodyLength();
    Preconditions.checkArgument(bodyLength % 8 == 0, "batch is not aligned");

    ByteBuffer serializedMessage = serializeMetadata(batch, option);

    int metadataLength = serializedMessage.remaining();

    int prefixSize = 4;
    if (!option.write_legacy_ipc_format) {
      out.writeIntLittleEndian(IPC_CONTINUATION_TOKEN);
      prefixSize = 8;
    }

    // calculate alignment bytes so that metadata length points to the correct location after
    // alignment
    int padding = (int) ((start + metadataLength + prefixSize) % 8);
    if (padding != 0) {
      metadataLength += (8 - padding);
    }

    out.writeIntLittleEndian(metadataLength);
    out.write(serializedMessage);

    // Align the output to 8 byte boundary.
    out.align();

    long bufferLength = writeBatchBuffers(out, batch);
    Preconditions.checkArgument(bufferLength % 8 == 0, "out is not aligned");

    // Metadata size in the Block account for the size prefix
    return new ArrowBlock(start, metadataLength + prefixSize, bufferLength);
  }

  /**
   * Write the Arrow buffers of the record batch to the output channel.
   *
   * @param out the output channel to write the buffers to
   * @param batch an ArrowRecordBatch containing buffers to be written
   * @return the number of bytes written
   * @throws IOException on error
   */
  public static long writeBatchBuffers(WriteChannel out, ArrowRecordBatch batch)
      throws IOException {
    long bufferStart = out.getCurrentPosition();
    List<ArrowBuf> buffers = batch.getBuffers();
    List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();

    for (int i = 0; i < buffers.size(); i++) {
      ArrowBuf buffer = buffers.get(i);
      ArrowBuffer layout = buffersLayout.get(i);
      long startPosition = bufferStart + layout.getOffset();
      if (startPosition != out.getCurrentPosition()) {
        out.writeZeros(startPosition - out.getCurrentPosition());
      }
      out.write(buffer);
      if (out.getCurrentPosition() != startPosition + layout.getSize()) {
        throw new IllegalStateException(
            "wrong buffer size: "
                + out.getCurrentPosition()
                + " != "
                + startPosition
                + layout.getSize());
      }
    }
    out.align();
    return out.getCurrentPosition() - bufferStart;
  }

  /**
   * Returns the serialized form of {@link RecordBatch} wrapped in a {@link
   * org.apache.arrow.flatbuf.Message}.
   */
  @Deprecated
  public static ByteBuffer serializeMetadata(ArrowMessage message) {
    return serializeMetadata(message, IpcOption.DEFAULT);
  }

  /**
   * Returns the serialized form of {@link RecordBatch} wrapped in a {@link
   * org.apache.arrow.flatbuf.Message}.
   */
  public static ByteBuffer serializeMetadata(ArrowMessage message, IpcOption writeOption) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int batchOffset = message.writeTo(builder);
    return serializeMessage(
        builder, message.getMessageType(), batchOffset, message.computeBodyLength(), writeOption);
  }

  /**
   * Deserializes an ArrowRecordBatch from a record batch message and data in an ArrowBuf.
   *
   * @param recordBatchMessage a Message of type MessageHeader.RecordBatch
   * @param bodyBuffer Arrow buffer containing the RecordBatch data
   * @return the deserialized ArrowRecordBatch
   * @throws IOException if something went wrong
   */
  public static ArrowRecordBatch deserializeRecordBatch(
      Message recordBatchMessage, ArrowBuf bodyBuffer) throws IOException {
    RecordBatch recordBatchFB = (RecordBatch) recordBatchMessage.header(new RecordBatch());
    return deserializeRecordBatch(recordBatchFB, bodyBuffer);
  }

  /**
   * Deserializes an ArrowRecordBatch read from the input channel. This uses the given allocator to
   * create an ArrowBuf for the batch body data.
   *
   * @param in Channel to read a RecordBatch message and data from
   * @param allocator BufferAllocator to allocate an Arrow buffer to read message body data
   * @return the deserialized ArrowRecordBatch
   * @throws IOException on error
   */
  public static ArrowRecordBatch deserializeRecordBatch(ReadChannel in, BufferAllocator allocator)
      throws IOException {
    MessageMetadataResult result = readMessage(in);
    if (result == null) {
      throw new IOException("Unexpected end of input when reading a RecordBatch");
    }
    if (result.getMessage().headerType() != MessageHeader.RecordBatch) {
      throw new IOException(
          "Expected RecordBatch but header was " + result.getMessage().headerType());
    }
    long bodyLength = result.getMessageBodyLength();
    ArrowBuf bodyBuffer = readMessageBody(in, bodyLength, allocator);
    return deserializeRecordBatch(result.getMessage(), bodyBuffer);
  }

  /**
   * Deserializes an ArrowRecordBatch knowing the size of the entire message up front. This
   * minimizes the number of reads to the underlying stream.
   *
   * @param in the channel to deserialize from
   * @param block the object to deserialize to
   * @param alloc to allocate buffers
   * @return the deserialized ArrowRecordBatch
   * @throws IOException if something went wrong
   */
  public static ArrowRecordBatch deserializeRecordBatch(
      ReadChannel in, ArrowBlock block, BufferAllocator alloc) throws IOException {
    // Metadata length contains prefix_size bytes plus byte padding
    long totalLen = block.getMetadataLength() + block.getBodyLength();

    ArrowBuf buffer = alloc.buffer(totalLen);
    if (in.readFully(buffer, totalLen) != totalLen) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }

    int prefixSize = buffer.getInt(0) == IPC_CONTINUATION_TOKEN ? 8 : 4;

    ArrowBuf metadataBuffer = buffer.slice(prefixSize, block.getMetadataLength() - prefixSize);

    Message messageFB = Message.getRootAsMessage(metadataBuffer.nioBuffer().asReadOnlyBuffer());

    RecordBatch recordBatchFB = (RecordBatch) messageFB.header(new RecordBatch());

    // Now read the body
    final ArrowBuf body =
        buffer.slice(block.getMetadataLength(), totalLen - block.getMetadataLength());
    return deserializeRecordBatch(recordBatchFB, body);
  }

  /**
   * Deserializes an ArrowRecordBatch given the Flatbuffer metadata and in-memory body.
   *
   * @param recordBatchFB Deserialized FlatBuffer record batch
   * @param body Read body of the record batch
   * @return ArrowRecordBatch from metadata and in-memory body
   * @throws IOException on error
   */
  public static ArrowRecordBatch deserializeRecordBatch(RecordBatch recordBatchFB, ArrowBuf body)
      throws IOException {
    // Now read the body
    int nodesLength = recordBatchFB.nodesLength();
    List<ArrowFieldNode> nodes = new ArrayList<>();
    for (int i = 0; i < nodesLength; ++i) {
      FieldNode node = recordBatchFB.nodes(i);
      if ((int) node.length() != node.length() || (int) node.nullCount() != node.nullCount()) {
        throw new IOException(
            "Cannot currently deserialize record batches with "
                + "node length larger than INT_MAX records.");
      }
      nodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
    }
    List<ArrowBuf> buffers = new ArrayList<>();
    for (int i = 0; i < recordBatchFB.buffersLength(); ++i) {
      Buffer bufferFB = recordBatchFB.buffers(i);
      ArrowBuf vectorBuffer = body.slice(bufferFB.offset(), bufferFB.length());
      buffers.add(vectorBuffer);
    }

    ArrowBodyCompression bodyCompression =
        recordBatchFB.compression() == null
            ? NoCompressionCodec.DEFAULT_BODY_COMPRESSION
            : new ArrowBodyCompression(
                recordBatchFB.compression().codec(), recordBatchFB.compression().method());

    if ((int) recordBatchFB.length() != recordBatchFB.length()) {
      throw new IOException(
          "Cannot currently deserialize record batches with more than INT_MAX records.");
    }
    ArrowRecordBatch arrowRecordBatch =
        new ArrowRecordBatch(
            checkedCastToInt(recordBatchFB.length()), nodes, buffers, bodyCompression);
    body.getReferenceManager().release();
    return arrowRecordBatch;
  }

  /**
   * Reads a record batch based on the metadata in serializedMessage and the underlying data buffer.
   */
  public static ArrowRecordBatch deserializeRecordBatch(
      MessageMetadataResult serializedMessage, ArrowBuf underlying) throws IOException {
    return deserializeRecordBatch(serializedMessage.getMessage(), underlying);
  }

  public static ArrowBlock serialize(WriteChannel out, ArrowDictionaryBatch batch)
      throws IOException {
    return serialize(out, batch, IpcOption.DEFAULT);
  }

  /**
   * Serializes a dictionary ArrowRecordBatch. Returns the offset and length of the written batch.
   *
   * @param out where to serialize
   * @param batch the batch to serialize
   * @param option options for IPC
   * @return the metadata of the serialized block
   * @throws IOException if something went wrong
   */
  public static ArrowBlock serialize(WriteChannel out, ArrowDictionaryBatch batch, IpcOption option)
      throws IOException {
    long start = out.getCurrentPosition();

    long bodyLength = batch.computeBodyLength();
    Preconditions.checkArgument(bodyLength % 8 == 0, "batch is not aligned");

    ByteBuffer serializedMessage = serializeMetadata(batch, option);

    int metadataLength = serializedMessage.remaining();

    int prefixSize = 4;
    if (!option.write_legacy_ipc_format) {
      out.writeIntLittleEndian(IPC_CONTINUATION_TOKEN);
      prefixSize = 8;
    }

    // calculate alignment bytes so that metadata length points to the correct location after
    // alignment
    int padding = (int) ((start + metadataLength + prefixSize) % 8);
    if (padding != 0) {
      metadataLength += (8 - padding);
    }

    out.writeIntLittleEndian(metadataLength);
    out.write(serializedMessage);

    // Align the output to 8 byte boundary.
    out.align();

    // write the embedded record batch
    long bufferLength = writeBatchBuffers(out, batch.getDictionary());
    Preconditions.checkArgument(bufferLength % 8 == 0, "out is not aligned");

    // Metadata size in the Block account for the size prefix
    return new ArrowBlock(start, metadataLength + prefixSize, bufferLength);
  }

  /**
   * Deserializes an ArrowDictionaryBatch from a dictionary batch Message and data in an ArrowBuf.
   *
   * @param message a message of type MessageHeader.DictionaryBatch
   * @param bodyBuffer Arrow buffer containing the DictionaryBatch data of type
   *     MessageHeader.DictionaryBatch
   * @return the deserialized ArrowDictionaryBatch
   * @throws IOException if something went wrong
   */
  public static ArrowDictionaryBatch deserializeDictionaryBatch(
      Message message, ArrowBuf bodyBuffer) throws IOException {
    DictionaryBatch dictionaryBatchFB = (DictionaryBatch) message.header(new DictionaryBatch());
    ArrowRecordBatch recordBatch = deserializeRecordBatch(dictionaryBatchFB.data(), bodyBuffer);
    return new ArrowDictionaryBatch(
        dictionaryBatchFB.id(), recordBatch, dictionaryBatchFB.isDelta());
  }

  /**
   * Deserializes an ArrowDictionaryBatch from a dictionary batch Message and data in an ArrowBuf.
   *
   * @param message a message of type MessageHeader.DictionaryBatch
   * @param bodyBuffer Arrow buffer containing the DictionaryBatch data of type
   *     MessageHeader.DictionaryBatch
   * @return the deserialized ArrowDictionaryBatch
   * @throws IOException if something went wrong
   */
  public static ArrowDictionaryBatch deserializeDictionaryBatch(
      MessageMetadataResult message, ArrowBuf bodyBuffer) throws IOException {
    return deserializeDictionaryBatch(message.getMessage(), bodyBuffer);
  }

  /**
   * Deserializes an ArrowDictionaryBatch read from the input channel. This uses the given allocator
   * to create an ArrowBuf for the batch body data.
   *
   * @param in Channel to read a DictionaryBatch message and data from
   * @param allocator BufferAllocator to allocate an Arrow buffer to read message body data
   * @return the deserialized ArrowDictionaryBatch
   * @throws IOException on error
   */
  public static ArrowDictionaryBatch deserializeDictionaryBatch(
      ReadChannel in, BufferAllocator allocator) throws IOException {
    MessageMetadataResult result = readMessage(in);
    if (result == null) {
      throw new IOException("Unexpected end of input when reading a DictionaryBatch");
    }
    if (result.getMessage().headerType() != MessageHeader.DictionaryBatch) {
      throw new IOException(
          "Expected DictionaryBatch but header was " + result.getMessage().headerType());
    }
    long bodyLength = result.getMessageBodyLength();
    ArrowBuf bodyBuffer = readMessageBody(in, bodyLength, allocator);
    return deserializeDictionaryBatch(result.getMessage(), bodyBuffer);
  }

  /**
   * Deserializes a DictionaryBatch knowing the size of the entire message up front. This minimizes
   * the number of reads to the underlying stream.
   *
   * @param in where to read from
   * @param block block metadata for deserializing
   * @param alloc to allocate new buffers
   * @return the deserialized ArrowDictionaryBatch
   * @throws IOException if something went wrong
   */
  public static ArrowDictionaryBatch deserializeDictionaryBatch(
      ReadChannel in, ArrowBlock block, BufferAllocator alloc) throws IOException {
    // Metadata length contains integer prefix plus byte padding
    long totalLen = block.getMetadataLength() + block.getBodyLength();

    ArrowBuf buffer = alloc.buffer(totalLen);
    if (in.readFully(buffer, totalLen) != totalLen) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }

    int prefixSize = buffer.getInt(0) == IPC_CONTINUATION_TOKEN ? 8 : 4;

    ArrowBuf metadataBuffer = buffer.slice(prefixSize, block.getMetadataLength() - prefixSize);

    Message messageFB = Message.getRootAsMessage(metadataBuffer.nioBuffer().asReadOnlyBuffer());

    DictionaryBatch dictionaryBatchFB = (DictionaryBatch) messageFB.header(new DictionaryBatch());

    // Now read the body
    final ArrowBuf body =
        buffer.slice(block.getMetadataLength(), totalLen - block.getMetadataLength());
    ArrowRecordBatch recordBatch = deserializeRecordBatch(dictionaryBatchFB.data(), body);
    return new ArrowDictionaryBatch(
        dictionaryBatchFB.id(), recordBatch, dictionaryBatchFB.isDelta());
  }

  /**
   * Deserialize a message that is either an ArrowDictionaryBatch or ArrowRecordBatch.
   *
   * @param reader MessageChannelReader to read a sequence of messages from a ReadChannel
   * @return The deserialized record batch
   * @throws IOException if the message is not an ArrowDictionaryBatch or ArrowRecordBatch
   */
  public static ArrowMessage deserializeMessageBatch(MessageChannelReader reader)
      throws IOException {
    MessageResult result = reader.readNext();
    if (result == null) {
      return null;
    } else if (result.getMessage().bodyLength() > Integer.MAX_VALUE) {
      throw new IOException("Cannot currently deserialize record batches over 2GB");
    }

    if (result.getMessage().version() != MetadataVersion.V4
        && result.getMessage().version() != MetadataVersion.V5) {
      throw new IOException(
          "Received metadata with an incompatible version number: "
              + result.getMessage().version());
    }

    switch (result.getMessage().headerType()) {
      case MessageHeader.RecordBatch:
        return deserializeRecordBatch(result.getMessage(), result.getBodyBuffer());
      case MessageHeader.DictionaryBatch:
        return deserializeDictionaryBatch(result.getMessage(), result.getBodyBuffer());
      default:
        throw new IOException("Unexpected message header type " + result.getMessage().headerType());
    }
  }

  /**
   * Deserialize a message that is either an ArrowDictionaryBatch or ArrowRecordBatch.
   *
   * @param in ReadChannel to read messages from
   * @param alloc Allocator for message data
   * @return The deserialized record batch
   * @throws IOException if the message is not an ArrowDictionaryBatch or ArrowRecordBatch
   */
  public static ArrowMessage deserializeMessageBatch(ReadChannel in, BufferAllocator alloc)
      throws IOException {
    return deserializeMessageBatch(new MessageChannelReader(in, alloc));
  }

  @Deprecated
  public static ByteBuffer serializeMessage(
      FlatBufferBuilder builder, byte headerType, int headerOffset, long bodyLength) {
    return serializeMessage(builder, headerType, headerOffset, bodyLength, IpcOption.DEFAULT);
  }

  /**
   * Serializes a message header.
   *
   * @param builder to write the flatbuf to
   * @param headerType headerType field
   * @param headerOffset header offset field
   * @param bodyLength body length field
   * @param writeOption IPC write options
   * @return the corresponding ByteBuffer
   */
  public static ByteBuffer serializeMessage(
      FlatBufferBuilder builder,
      byte headerType,
      int headerOffset,
      long bodyLength,
      IpcOption writeOption) {
    Message.startMessage(builder);
    Message.addHeaderType(builder, headerType);
    Message.addHeader(builder, headerOffset);
    Message.addVersion(builder, writeOption.metadataVersion.toFlatbufID());
    Message.addBodyLength(builder, bodyLength);
    builder.finish(Message.endMessage(builder));
    return builder.dataBuffer();
  }

  /**
   * Read a Message from the input channel and return a MessageMetadataResult that contains the
   * Message metadata, buffer containing the serialized Message metadata as read, and length of the
   * Message in bytes. Returns null if the end-of-stream has been reached.
   *
   * @param in ReadChannel to read messages from
   * @return MessageMetadataResult with deserialized Message metadata and message information if a
   *     valid Message was read, or null if end-of-stream
   * @throws IOException on error
   */
  public static MessageMetadataResult readMessage(ReadChannel in) throws IOException {

    // Read the message size. There is an i32 little endian prefix.
    ByteBuffer buffer = ByteBuffer.allocate(4);
    if (in.readFully(buffer) == 4) {

      int messageLength = MessageSerializer.bytesToInt(buffer.array());
      if (messageLength == IPC_CONTINUATION_TOKEN) {
        // Avoid breaking change in signature of ByteBuffer.clear() in JDK9+
        ((java.nio.Buffer) buffer).clear();
        // ARROW-6313, if the first 4 bytes are continuation message, read the next 4 for the length
        if (in.readFully(buffer) == 4) {
          messageLength = MessageSerializer.bytesToInt(buffer.array());
        }
      }

      // Length of 0 indicates end of stream
      if (messageLength != 0) {

        // Read the message into the buffer.
        ByteBuffer messageBuffer = ByteBuffer.allocate(messageLength);
        if (in.readFully(messageBuffer) != messageLength) {
          throw new IOException("Unexpected end of stream trying to read message.");
        }
        // see https://github.com/apache/arrow/issues/41717 for reason why we cast to
        // java.nio.Buffer
        ByteBuffer rewindBuffer = (ByteBuffer) ((java.nio.Buffer) messageBuffer).rewind();

        // Load the message.
        Message message = Message.getRootAsMessage(messageBuffer);

        return new MessageMetadataResult(messageLength, messageBuffer, message);
      }
    }
    return null;
  }

  /**
   * Read a Message body from the in channel into an ArrowBuf.
   *
   * @param in ReadChannel to read message body from
   * @param bodyLength Length in bytes of the message body to read
   * @param allocator Allocate the ArrowBuf to contain message body data
   * @return an ArrowBuf containing the message body data
   * @throws IOException on error
   */
  public static ArrowBuf readMessageBody(ReadChannel in, long bodyLength, BufferAllocator allocator)
      throws IOException {
    ArrowBuf bodyBuffer = allocator.buffer(bodyLength);
    try {
      if (in.readFully(bodyBuffer, bodyLength) != bodyLength) {
        throw new IOException("Unexpected end of input trying to read batch.");
      }
    } catch (RuntimeException | IOException e) {
      bodyBuffer.close();
      throw e;
    }
    return bodyBuffer;
  }
}
