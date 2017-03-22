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
package org.apache.arrow.vector.stream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.DictionaryBatch;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.file.WriteChannel;
import org.apache.arrow.vector.schema.ArrowBuffer;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Utility class for serializing Messages. Messages are all serialized a similar way.
 * 1. 4 byte little endian message header prefix
 * 2. FB serialized Message: This includes it the body length, which is the serialized
 *    body and the type of the message.
 * 3. Serialized message.
 *
 * For schema messages, the serialization is simply the FB serialized Schema.
 *
 * For RecordBatch messages the serialization is:
 *   1. 4 byte little endian batch metadata header
 *   2. FB serialized RowBatch
 *   3. Padding to align to 8 byte boundary.
 *   4. serialized RowBatch buffers.
 */
public class MessageSerializer {

  public static int bytesToInt(byte[] bytes) {
    return ((bytes[3] & 255) << 24) +
           ((bytes[2] & 255) << 16) +
           ((bytes[1] & 255) <<  8) +
           ((bytes[0] & 255) <<  0);
  }

  /**
   * Serialize a schema object.
   */
  public static long serialize(WriteChannel out, Schema schema) throws IOException {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int schemaOffset = schema.getSchema(builder);
    ByteBuffer serializedMessage = serializeMessage(builder, MessageHeader.Schema, schemaOffset, 0);
    long size = out.writeIntLittleEndian(serializedMessage.remaining());
    size += out.write(serializedMessage);
    return size;
  }

  /**
   * Deserializes a schema object. Format is from serialize().
   */
  public static Schema deserializeSchema(ReadChannel in) throws IOException {
    Message message = deserializeMessage(in);
    if (message == null) {
      throw new IOException("Unexpected end of input. Missing schema.");
    }
    if (message.headerType() != MessageHeader.Schema) {
      throw new IOException("Expected schema but header was " + message.headerType());
    }

    return Schema.convertSchema((org.apache.arrow.flatbuf.Schema)
        message.header(new org.apache.arrow.flatbuf.Schema()));
  }


  /**
   * Serializes an ArrowRecordBatch. Returns the offset and length of the written batch.
   */
  public static ArrowBlock serialize(WriteChannel out, ArrowRecordBatch batch)
          throws IOException {

    long start = out.getCurrentPosition();
    int bodyLength = batch.computeBodyLength();

    FlatBufferBuilder builder = new FlatBufferBuilder();
    int batchOffset = batch.writeTo(builder);

    ByteBuffer serializedMessage = serializeMessage(builder, MessageHeader.RecordBatch, batchOffset, bodyLength);

    int metadataLength = serializedMessage.remaining();

    // calculate alignment bytes so that metadata length points to the correct location after alignment
    int padding = (int)((start + metadataLength + 4) % 8);
    if (padding != 0) {
        metadataLength += (8 - padding);
    }

    out.writeIntLittleEndian(metadataLength);
    out.write(serializedMessage);

    // Align the output to 8 byte boundary.
    out.align();

    long bufferLength = writeBatchBuffers(out, batch);

    // Metadata size in the Block account for the size prefix
    return new ArrowBlock(start, metadataLength + 4, bufferLength);
  }

  private static long writeBatchBuffers(WriteChannel out, ArrowRecordBatch batch) throws IOException {
    long bufferStart = out.getCurrentPosition();
    List<ArrowBuf> buffers = batch.getBuffers();
    List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();

    for (int i = 0; i < buffers.size(); i++) {
      ArrowBuf buffer = buffers.get(i);
      ArrowBuffer layout = buffersLayout.get(i);
      long startPosition = bufferStart + layout.getOffset();
      if (startPosition != out.getCurrentPosition()) {
        out.writeZeros((int)(startPosition - out.getCurrentPosition()));
      }
      out.write(buffer);
      if (out.getCurrentPosition() != startPosition + layout.getSize()) {
        throw new IllegalStateException("wrong buffer size: " + out.getCurrentPosition() +
            " != " + startPosition + layout.getSize());
      }
    }
    return out.getCurrentPosition() - bufferStart;
  }

  /**
   * Deserializes a RecordBatch
   */
  private static ArrowRecordBatch deserializeRecordBatch(ReadChannel in, Message message, BufferAllocator alloc)
      throws IOException {
    RecordBatch recordBatchFB = (RecordBatch) message.header(new RecordBatch());

    int bodyLength = (int) message.bodyLength();

    // Now read the record batch body
    ArrowBuf buffer = alloc.buffer(bodyLength);
    if (in.readFully(buffer, bodyLength) != bodyLength) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }
    return deserializeRecordBatch(recordBatchFB, buffer);
  }

  /**
   * Deserializes a RecordBatch knowing the size of the entire message up front. This
   * minimizes the number of reads to the underlying stream.
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

  // Deserializes a record batch given the Flatbuffer metadata and in-memory body
  private static ArrowRecordBatch deserializeRecordBatch(RecordBatch recordBatchFB,
      ArrowBuf body) {
    // Now read the body
    int nodesLength = recordBatchFB.nodesLength();
    List<ArrowFieldNode> nodes = new ArrayList<>();
    for (int i = 0; i < nodesLength; ++i) {
      FieldNode node = recordBatchFB.nodes(i);
      nodes.add(new ArrowFieldNode((int)node.length(), (int)node.nullCount()));
    }
    List<ArrowBuf> buffers = new ArrayList<>();
    for (int i = 0; i < recordBatchFB.buffersLength(); ++i) {
      Buffer bufferFB = recordBatchFB.buffers(i);
      ArrowBuf vectorBuffer = body.slice((int)bufferFB.offset(), (int)bufferFB.length());
      buffers.add(vectorBuffer);
    }
    ArrowRecordBatch arrowRecordBatch =
        new ArrowRecordBatch((int)recordBatchFB.length(), nodes, buffers);
    body.release();
    return arrowRecordBatch;
  }

  /**
   * Serializes a dictionary ArrowRecordBatch. Returns the offset and length of the written batch.
   */
  public static ArrowBlock serialize(WriteChannel out, ArrowDictionaryBatch batch) throws IOException {
    long start = out.getCurrentPosition();
    int bodyLength = batch.computeBodyLength();

    FlatBufferBuilder builder = new FlatBufferBuilder();
    int batchOffset = batch.writeTo(builder);

    ByteBuffer serializedMessage = serializeMessage(builder, MessageHeader.DictionaryBatch, batchOffset, bodyLength);

    int metadataLength = serializedMessage.remaining();

    // Add extra padding bytes so that length prefix + metadata is a multiple
    // of 8 after alignment
    if ((start + metadataLength + 4) % 8 != 0) {
      metadataLength += 8 - (start + metadataLength + 4) % 8;
    }

    out.writeIntLittleEndian(metadataLength);
    out.write(serializedMessage);

    // Align the output to 8 byte boundary.
    out.align();

    // write the embedded record batch
    long bufferLength = writeBatchBuffers(out, batch.getDictionary());

    // Metadata size in the Block account for the size prefix
    return new ArrowBlock(start, metadataLength + 4, bufferLength + 8);
  }

  /**
   * Deserializes a DictionaryBatch
   */
  private static ArrowDictionaryBatch deserializeDictionaryBatch(ReadChannel in,
                                                                 Message message,
                                                                 BufferAllocator alloc) throws IOException {
    DictionaryBatch dictionaryBatchFB = (DictionaryBatch) message.header(new DictionaryBatch());

    int bodyLength = (int) message.bodyLength();

    // Now read the record batch body
    ArrowBuf body = alloc.buffer(bodyLength);
    if (in.readFully(body, bodyLength) != bodyLength) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }
    ArrowRecordBatch recordBatch = deserializeRecordBatch(dictionaryBatchFB.data(), body);
    return new ArrowDictionaryBatch(dictionaryBatchFB.id(), recordBatch);
  }

  /**
   * Deserializes a DictionaryBatch knowing the size of the entire message up front. This
   * minimizes the number of reads to the underlying stream.
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

  public static ArrowMessage deserializeMessageBatch(ReadChannel in, BufferAllocator alloc) throws IOException {
    Message message = deserializeMessage(in);
    if (message == null) {
      return null;
    } else if (message.bodyLength() > Integer.MAX_VALUE) {
      throw new IOException("Cannot currently deserialize record batches over 2GB");
    }

    switch (message.headerType()) {
      case MessageHeader.RecordBatch: return deserializeRecordBatch(in, message, alloc);
      case MessageHeader.DictionaryBatch: return deserializeDictionaryBatch(in, message, alloc);
      default: throw new IOException("Unexpected message header type " + message.headerType());
    }
  }

  /**
   * Serializes a message header.
   */
  private static ByteBuffer serializeMessage(FlatBufferBuilder builder, byte headerType,
      int headerOffset, int bodyLength) {
    Message.startMessage(builder);
    Message.addHeaderType(builder, headerType);
    Message.addHeader(builder, headerOffset);
    Message.addVersion(builder, MetadataVersion.V2);
    Message.addBodyLength(builder, bodyLength);
    builder.finish(Message.endMessage(builder));
    return builder.dataBuffer();
  }

  private static Message deserializeMessage(ReadChannel in) throws IOException {
    // Read the message size. There is an i32 little endian prefix.
    ByteBuffer buffer = ByteBuffer.allocate(4);
    if (in.readFully(buffer) != 4) return null;
    int messageLength = bytesToInt(buffer.array());
    if (messageLength == 0) return null;

    buffer = ByteBuffer.allocate(messageLength);
    if (in.readFully(buffer) != messageLength) {
      throw new IOException(
          "Unexpected end of stream trying to read message.");
    }
    buffer.rewind();

    return Message.getRootAsMessage(buffer);
  }
}
