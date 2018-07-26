/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.ipc.message;

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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;

/**
 * Utility class for serializing Messages. Messages are all serialized a similar way.
 * 1. 4 byte little endian message header prefix
 * 2. FB serialized Message: This includes it the body length, which is the serialized
 * body and the type of the message.
 * 3. Serialized message.
 *
 * For schema messages, the serialization is simply the FB serialized Schema.
 *
 * For RecordBatch messages the serialization is:
 * 1. 4 byte little endian batch metadata header
 * 2. FB serialized RowBatch
 * 3. Padding to align to 8 byte boundary.
 * 4. serialized RowBatch buffers.
 */
public class MessageSerializer {

  /**
   * Convert an array of 4 bytes to a little endian i32 value.
   *
   * @param bytes byte array with minimum length of 4
   * @return converted little endian 32-bit integer
   */
  public static int bytesToInt(byte[] bytes) {
    return ((bytes[3] & 255) << 24) +
        ((bytes[2] & 255) << 16) +
        ((bytes[1] & 255) << 8) +
        ((bytes[0] & 255) << 0);
  }

  /**
   * Convert an integer to a 4 byte array.
   *
   * @param value integer value input
   * @param bytes existing byte array with minimum length of 4 to contain the conversion output
   */
  public static void intToBytes(int value, byte[] bytes) {
    bytes[3] = (byte) (value >>> 24);
    bytes[2] = (byte) (value >>> 16);
    bytes[1] = (byte) (value >>> 8);
    bytes[0] = (byte) (value >>> 0);
  }

  /**
   * Aligns the message to 8 byte boundary and adjusts messageLength accordingly, then writes
   * the message length prefix and message buffer to the Channel.
   *
   * @param out Output Channel
   * @param messageLength Number of bytes in the message buffer, written as little Endian prefix
   * @param messageBuffer Message buffer to be written
   * @return Number of bytes written
   * @return
   * @throws IOException
   */
  public static int writeMessageBufferAligned(WriteChannel out, int messageLength, ByteBuffer messageBuffer) throws IOException {

    // ensure that message aligns to 8 byte padding - 4 bytes for size, then message body
    if ((messageLength + 4) % 8 != 0) {
      messageLength += 8 - (messageLength + 4) % 8;
    }
    out.writeIntLittleEndian(messageLength);
    out.write(messageBuffer);
    out.align();

    // any bytes written are already captured by our size modification above
    return messageLength + 4;
  }

  /**
   * Serialize a schema object.
   *
   * @param out    where to write the schema
   * @param schema the object to serialize to out
   * @return the number of bytes written
   * @throws IOException if something went wrong
   */
  public static long serialize(WriteChannel out, Schema schema) throws IOException {
    long start = out.getCurrentPosition();
    assert start % 8 == 0;

    FlatBufferBuilder builder = new FlatBufferBuilder();
    int schemaOffset = schema.getSchema(builder);
    ByteBuffer serializedMessage = serializeMessage(builder, MessageHeader.Schema, schemaOffset, 0);

    int messageLength = serializedMessage.remaining();

    int bytesWritten = writeMessageBufferAligned(out, messageLength, serializedMessage);
    assert bytesWritten % 8 == 0;
    return bytesWritten;
  }

  /**
   * Deserializes an Arrow Schema object from a schema message. Format is from serialize().
   *
   * @param schemaMessage a Message of type MessageHeader.Schema
   * @return the deserialized Arrow Schema
   */
  public static Schema deserializeSchema(Message schemaMessage) {
    return Schema.convertSchema((org.apache.arrow.flatbuf.Schema)
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
    MessageChannelResult result = readMessage(in);
    if (!result.hasMessage()) {
      throw new IOException("Unexpected end of input when reading Schema");
    }
    if (result.getMessage().headerType() != MessageHeader.Schema) {
      throw new IOException("Expected schema but header was " + result.getMessage().headerType());
    }

    return deserializeSchema(result.getMessage());
  }

  /**
   * Serializes an ArrowRecordBatch. Returns the offset and length of the written batch.
   *
   * @param out   where to write the batch
   * @param batch the object to serialize to out
   * @return the serialized block metadata
   * @throws IOException if something went wrong
   */
  public static ArrowBlock serialize(WriteChannel out, ArrowRecordBatch batch)
      throws IOException {

    long start = out.getCurrentPosition();
    int bodyLength = batch.computeBodyLength();
    assert bodyLength % 8 == 0;

    FlatBufferBuilder builder = new FlatBufferBuilder();
    int batchOffset = batch.writeTo(builder);

    ByteBuffer serializedMessage = serializeMessage(builder, MessageHeader.RecordBatch, batchOffset, bodyLength);

    int metadataLength = serializedMessage.remaining();

    // calculate alignment bytes so that metadata length points to the correct location after alignment
    int padding = (int) ((start + metadataLength + 4) % 8);
    if (padding != 0) {
      metadataLength += (8 - padding);
    }

    out.writeIntLittleEndian(metadataLength);
    out.write(serializedMessage);

    // Align the output to 8 byte boundary.
    out.align();

    long bufferLength = writeBatchBuffers(out, batch);
    assert bufferLength % 8 == 0;

    // Metadata size in the Block account for the size prefix
    return new ArrowBlock(start, metadataLength + 4, bufferLength);
  }

  /**
   * Write the Arrow buffers of the record batch to the output channel.
   *
   * @param out the output channel to write the buffers to
   * @param batch an ArrowRecordBatch containing buffers to be written
   * @return the number of bytes written
   * @throws IOException
   */
  public static long writeBatchBuffers(WriteChannel out, ArrowRecordBatch batch) throws IOException {
    long bufferStart = out.getCurrentPosition();
    List<ArrowBuf> buffers = batch.getBuffers();
    List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();

    for (int i = 0; i < buffers.size(); i++) {
      ArrowBuf buffer = buffers.get(i);
      ArrowBuffer layout = buffersLayout.get(i);
      long startPosition = bufferStart + layout.getOffset();
      if (startPosition != out.getCurrentPosition()) {
        out.writeZeros((int) (startPosition - out.getCurrentPosition()));
      }
      out.write(buffer);
      if (out.getCurrentPosition() != startPosition + layout.getSize()) {
        throw new IllegalStateException("wrong buffer size: " + out.getCurrentPosition() +
            " != " + startPosition + layout.getSize());
      }
    }
    out.align();
    return out.getCurrentPosition() - bufferStart;
  }

  /**
   * Deserializes an ArrowRecordBatch from a record batch message and data in an ArrowBuf.
   *
   * @param recordBatchMessage a Message of type MessageHeader.RecordBatch
   * @param bodyBuffer Arrow buffer containing the RecordBatch data
   * @return the deserialized ArrowRecordBatch
   * @throws IOException if something went wrong
   */
  public static ArrowRecordBatch deserializeRecordBatch(Message recordBatchMessage, ArrowBuf bodyBuffer)
      throws IOException {
    RecordBatch recordBatchFB = (RecordBatch) recordBatchMessage.header(new RecordBatch());
    return deserializeRecordBatch(recordBatchFB, bodyBuffer);
  }

  /**
   * Deserializes an ArrowRecordBatch read from the input channel. This uses the given allocator
   * to create an ArrowBuf for the batch body data.
   *
   * @param in Channel to read a RecordBatch message and data from
   * @param allocator BufferAllocator to allocate an Arrow buffer to read message body data
   * @return the deserialized ArrowRecordBatch
   * @throws IOException
   */
  public static ArrowRecordBatch deserializeRecordBatch(ReadChannel in, BufferAllocator allocator) throws IOException {
    MessageChannelResult result = readMessage(in);
    if (!result.hasMessage()) {
      throw new IOException("Unexpected end of input when reading a RecordBatch");
    }
    if (result.getMessage().headerType() != MessageHeader.RecordBatch) {
      throw new IOException("Expected RecordBatch but header was " + result.getMessage().headerType());
    }
    int bodyLength = (int) result.getMessageBodyLength();
    ArrowBuf bodyBuffer = readMessageBody(in, bodyLength, allocator);
    return deserializeRecordBatch(result.getMessage(), bodyBuffer);
  }

  /**
   * Deserializes an ArrowRecordBatch knowing the size of the entire message up front. This
   * minimizes the number of reads to the underlying stream.
   *
   * @param in    the channel to deserialize from
   * @param block the object to deserialize to
   * @param alloc to allocate buffers
   * @return the deserialized ArrowRecordBatch
   * @throws IOException if something went wrong
   */
  public static ArrowRecordBatch deserializeRecordBatch(ReadChannel in, ArrowBlock block,
                                                        BufferAllocator alloc) throws IOException {
    // Metadata length contains integer prefix plus byte padding
    long totalLen = block.getMetadataLength() + block.getBodyLength();

    if (totalLen > Integer.MAX_VALUE) {
      throw new IOException("Cannot currently deserialize record batches over 2GB");
    }

    ArrowBuf buffer = alloc.buffer((int) totalLen);
    if (in.readFully(buffer, (int) totalLen) != totalLen) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }

    ArrowBuf metadataBuffer = buffer.slice(4, block.getMetadataLength() - 4);

    Message messageFB =
        Message.getRootAsMessage(metadataBuffer.nioBuffer().asReadOnlyBuffer());

    RecordBatch recordBatchFB = (RecordBatch) messageFB.header(new RecordBatch());

    // Now read the body
    final ArrowBuf body = buffer.slice(block.getMetadataLength(),
        (int) totalLen - block.getMetadataLength());
    return deserializeRecordBatch(recordBatchFB, body);
  }

  /**
   * Deserializes an ArrowRecordBatch given the Flatbuffer metadata and in-memory body.
   *
   * @param recordBatchFB Deserialized FlatBuffer record batch
   * @param body Read body of the record batch
   * @return ArrowRecordBatch from metadata and in-memory body
   * @throws IOException
   */
  public static ArrowRecordBatch deserializeRecordBatch(RecordBatch recordBatchFB,
                                                        ArrowBuf body) throws IOException {
    // Now read the body
    int nodesLength = recordBatchFB.nodesLength();
    List<ArrowFieldNode> nodes = new ArrayList<>();
    for (int i = 0; i < nodesLength; ++i) {
      FieldNode node = recordBatchFB.nodes(i);
      if ((int) node.length() != node.length() ||
          (int) node.nullCount() != node.nullCount()) {
        throw new IOException("Cannot currently deserialize record batches with " +
            "node length larger than Int.MAX_VALUE");
      }
      nodes.add(new ArrowFieldNode((int) node.length(), (int) node.nullCount()));
    }
    List<ArrowBuf> buffers = new ArrayList<>();
    for (int i = 0; i < recordBatchFB.buffersLength(); ++i) {
      Buffer bufferFB = recordBatchFB.buffers(i);
      ArrowBuf vectorBuffer = body.slice((int) bufferFB.offset(), (int) bufferFB.length());
      buffers.add(vectorBuffer);
    }
    if ((int) recordBatchFB.length() != recordBatchFB.length()) {
      throw new IOException("Cannot currently deserialize record batches over 2GB");
    }
    ArrowRecordBatch arrowRecordBatch =
        new ArrowRecordBatch((int) recordBatchFB.length(), nodes, buffers);
    body.release();
    return arrowRecordBatch;
  }

  /**
   * Serializes a dictionary ArrowRecordBatch. Returns the offset and length of the written batch.
   *
   * @param out   where to serialize
   * @param batch the batch to serialize
   * @return the metadata of the serialized block
   * @throws IOException if something went wrong
   */
  public static ArrowBlock serialize(WriteChannel out, ArrowDictionaryBatch batch) throws IOException {
    long start = out.getCurrentPosition();
    int bodyLength = batch.computeBodyLength();
    assert bodyLength % 8 == 0;

    FlatBufferBuilder builder = new FlatBufferBuilder();
    int batchOffset = batch.writeTo(builder);

    ByteBuffer serializedMessage = serializeMessage(builder, MessageHeader.DictionaryBatch, batchOffset, bodyLength);

    int metadataLength = serializedMessage.remaining();

    // calculate alignment bytes so that metadata length points to the correct location after alignment
    int padding = (int) ((start + metadataLength + 4) % 8);
    if (padding != 0) {
      metadataLength += (8 - padding);
    }

    out.writeIntLittleEndian(metadataLength);
    out.write(serializedMessage);

    // Align the output to 8 byte boundary.
    out.align();

    // write the embedded record batch
    long bufferLength = writeBatchBuffers(out, batch.getDictionary());
    assert bufferLength % 8 == 0;

    // Metadata size in the Block account for the size prefix
    return new ArrowBlock(start, metadataLength + 4, bufferLength);
  }

  /**
   * Deserializes an ArrowDictionaryBatch from a dictionary batch Message and data in an ArrowBuf.
   *
   * @param message a message of type MessageHeader.DictionaryBatch
   * @param bodyBuffer Arrow buffer containing the DictionaryBatch data
   *                   of type MessageHeader.DictionaryBatch
   * @return the deserialized ArrowDictionaryBatch
   * @throws IOException if something went wrong
   */
  public static ArrowDictionaryBatch deserializeDictionaryBatch(Message message, ArrowBuf bodyBuffer) throws IOException {
    DictionaryBatch dictionaryBatchFB = (DictionaryBatch) message.header(new DictionaryBatch());
    ArrowRecordBatch recordBatch = deserializeRecordBatch(dictionaryBatchFB.data(), bodyBuffer);
    return new ArrowDictionaryBatch(dictionaryBatchFB.id(), recordBatch);
  }

  /**
   * Deserializes an ArrowDictionaryBatch read from the input channel. This uses the given allocator
   * to create an ArrowBuf for the batch body data.
   *
   * @param in Channel to read a DictionaryBatch message and data from
   * @param allocator BufferAllocator to allocate an Arrow buffer to read message body data
   * @return the deserialized ArrowDictionaryBatch
   * @throws IOException
   */
  public static ArrowDictionaryBatch deserializeDictionaryBatch(ReadChannel in, BufferAllocator allocator) throws IOException {
    MessageChannelResult result = readMessage(in);
    if (!result.hasMessage()) {
      throw new IOException("Unexpected end of input when reading a DictionaryBatch");
    }
    if (result.getMessage().headerType() != MessageHeader.DictionaryBatch) {
      throw new IOException("Expected DictionaryBatch but header was " + result.getMessage().headerType());
    }
    int bodyLength = (int) result.getMessageBodyLength();
    ArrowBuf bodyBuffer = readMessageBody(in, bodyLength, allocator);
    return deserializeDictionaryBatch(result.getMessage(), bodyBuffer);
  }

  /**
   * Deserializes a DictionaryBatch knowing the size of the entire message up front. This
   * minimizes the number of reads to the underlying stream.
   *
   * @param in    where to read from
   * @param block block metadata for deserializing
   * @param alloc to allocate new buffers
   * @return the deserialized ArrowDictionaryBatch
   * @throws IOException if something went wrong
   */
  public static ArrowDictionaryBatch deserializeDictionaryBatch(ReadChannel in,
                                                                ArrowBlock block,
                                                                BufferAllocator alloc) throws IOException {
    // Metadata length contains integer prefix plus byte padding
    long totalLen = block.getMetadataLength() + block.getBodyLength();

    if (totalLen > Integer.MAX_VALUE) {
      throw new IOException("Cannot currently deserialize record batches over 2GB");
    }

    ArrowBuf buffer = alloc.buffer((int) totalLen);
    if (in.readFully(buffer, (int) totalLen) != totalLen) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }

    ArrowBuf metadataBuffer = buffer.slice(4, block.getMetadataLength() - 4);

    Message messageFB =
        Message.getRootAsMessage(metadataBuffer.nioBuffer().asReadOnlyBuffer());

    DictionaryBatch dictionaryBatchFB = (DictionaryBatch) messageFB.header(new DictionaryBatch());

    // Now read the body
    final ArrowBuf body = buffer.slice(block.getMetadataLength(),
        (int) totalLen - block.getMetadataLength());
    ArrowRecordBatch recordBatch = deserializeRecordBatch(dictionaryBatchFB.data(), body);
    return new ArrowDictionaryBatch(dictionaryBatchFB.id(), recordBatch);
  }

  /**
   * Deserialize a message that is either an ArrowDictionaryBatch or ArrowRecordBatch.
   *
   * @param reader MessageChannelReader to read a sequence of messages from a ReadChannel
   * @return The deserialized record batch
   * @throws IOException if the message is not an ArrowDictionaryBatch or ArrowRecordBatch
   */
  public static ArrowMessage deserializeMessageBatch(MessageChannelReader reader) throws IOException {
    MessageHolder holder = new MessageHolder();
    if (!reader.readNext(holder)) {
      return null;
    } else if (holder.message.bodyLength() > Integer.MAX_VALUE) {
      throw new IOException("Cannot currently deserialize record batches over 2GB");
    }

    if (holder.message.version() != MetadataVersion.V4) {
      throw new IOException("Received metadata with an incompatible version number");
    }

    switch (holder.message.headerType()) {
      case MessageHeader.RecordBatch:
        return deserializeRecordBatch(holder.message, holder.bodyBuffer);
      case MessageHeader.DictionaryBatch:
        return deserializeDictionaryBatch(holder.message, holder.bodyBuffer);
      default:
        throw new IOException("Unexpected message header type " + holder.message.headerType());
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
  public static ArrowMessage deserializeMessageBatch(ReadChannel in, BufferAllocator alloc) throws IOException {
    return deserializeMessageBatch(new MessageChannelReader(in, alloc));
  }

  /**
   * Serializes a message header.
   *
   * @param builder      to write the flatbuf to
   * @param headerType   headerType field
   * @param headerOffset header offset field
   * @param bodyLength   body length field
   * @return the corresponding ByteBuffer
   */
  public static ByteBuffer serializeMessage(FlatBufferBuilder builder, byte headerType,
                                            int headerOffset, int bodyLength) {
    Message.startMessage(builder);
    Message.addHeaderType(builder, headerType);
    Message.addHeader(builder, headerOffset);
    Message.addVersion(builder, MetadataVersion.V4);
    Message.addBodyLength(builder, bodyLength);
    builder.finish(Message.endMessage(builder));
    return builder.dataBuffer();
  }

  /**
   * Read a Message from the in channel and return a MessageResult object that contains the
   * Message, raw buffer containing the read Message, and length of the Message in bytes. If
   * the end-of-stream has been reached, MessageResult.hasMessage() will return false.
   *
   * @param in ReadChannel to read messages from
   * @return MessageResult with Message and message information
   * @throws IOException
   */
  public static MessageChannelResult readMessage(ReadChannel in) throws IOException {
    int messageLength = 0;
    ByteBuffer messageBuffer = null;
    Message message = null;

    // Read the message size. There is an i32 little endian prefix.
    ByteBuffer buffer = ByteBuffer.allocate(4);
    if (in.readFully(buffer) == 4) {
      messageLength = MessageSerializer.bytesToInt(buffer.array());

      // Length of 0 indicates end of stream
      if (messageLength != 0) {

        // Read the message into the buffer.
        messageBuffer = ByteBuffer.allocate(messageLength);
        if (in.readFully(messageBuffer) != messageLength) {
          throw new IOException(
            "Unexpected end of stream trying to read message.");
        }
        messageBuffer.rewind();

        // Load the message.
        message = Message.getRootAsMessage(messageBuffer);
      }
    }

    return new MessageChannelResult(messageLength, messageBuffer, message);
  }

  /**
   * Read a Message body from the in channel into an ArrowBuf.
   *
   * @param in ReadChannel to read message body from
   * @param bodyLength Length in bytes of the message body to read
   * @param allocator Allocate the ArrowBuf to contain message body data
   * @return an ArrowBuf containing the message body data
   * @throws IOException
   */
  public static ArrowBuf readMessageBody(ReadChannel in, int bodyLength, BufferAllocator allocator) throws IOException {
    ArrowBuf bodyBuffer = allocator.buffer(bodyLength);
    if (in.readFully(bodyBuffer, bodyLength) != bodyLength) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }
    return bodyBuffer;
  }
}
