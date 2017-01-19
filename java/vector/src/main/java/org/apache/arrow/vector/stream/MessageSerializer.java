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

import org.apache.arrow.flatbuf.Buffer;
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
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;

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
    builder.finish(schema.getSchema(builder));
    ByteBuffer serializedBody = builder.dataBuffer();
    ByteBuffer serializedHeader =
        serializeHeader(MessageHeader.Schema, serializedBody.remaining());

    long size = out.writeIntLittleEndian(serializedHeader.remaining());
    size += out.write(serializedHeader);
    size += out.write(serializedBody);
    return size;
  }

  /**
   * Deserializes a schema object. Format is from serialize().
   */
  public static Schema deserializeSchema(ReadChannel in) throws IOException {
    Message header = deserializeHeader(in, MessageHeader.Schema);
    if (header == null) {
      throw new IOException("Unexpected end of input. Missing schema.");
    }

    // Now read the schema.
    ByteBuffer buffer = ByteBuffer.allocate((int)header.bodyLength());
    if (in.readFully(buffer) != header.bodyLength()) {
      throw new IOException("Unexpected end of input trying to read schema.");
    }
    buffer.rewind();
    return Schema.deserialize(buffer);
  }

  /**
   * Serializes an ArrowRecordBatch. Returns the offset and length of the written batch.
   */
  public static ArrowBlock serialize(WriteChannel out, ArrowRecordBatch batch)
      throws IOException {
    long start = out.getCurrentPosition();
    int bodyLength = batch.computeBodyLength();
    ByteBuffer metadata = WriteChannel.serialize(batch);

    int messageLength = 4 + metadata.remaining() + bodyLength;
    ByteBuffer serializedHeader =
        serializeHeader(MessageHeader.RecordBatch, messageLength);

    // Compute the required alignment. This is not a great way to do it. The issue is
    // that we need to know the message size to serialize the message header but the
    // size depends on the alignment, which depends on the message header.
    // This will serialize the header again with the updated size alignment adjusted.
    // TODO: We really just want sizeof(MessageHeader) from the serializeHeader() above.
    // Is there a way to do this?
    long bufferOffset = start + 4 + serializedHeader.remaining() + 4 + metadata.remaining();
    if (bufferOffset % 8 != 0) {
      messageLength += 8 - bufferOffset % 8;
      serializedHeader = serializeHeader(MessageHeader.RecordBatch, messageLength);
    }

    // Write message header.
    out.writeIntLittleEndian(serializedHeader.remaining());
    out.write(serializedHeader);

    // Write batch header. with the 4 byte little endian prefix
    out.writeIntLittleEndian(metadata.remaining());
    out.write(metadata);

    // Align the output to 8 byte boundary.
    out.align();

    long offset = out.getCurrentPosition();
    List<ArrowBuf> buffers = batch.getBuffers();
    List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();

    for (int i = 0; i < buffers.size(); i++) {
      ArrowBuf buffer = buffers.get(i);
      ArrowBuffer layout = buffersLayout.get(i);
      long startPosition = offset + layout.getOffset();
      if (startPosition != out.getCurrentPosition()) {
        out.writeZeros((int)(startPosition - out.getCurrentPosition()));
      }
      out.write(buffer);
      if (out.getCurrentPosition() != startPosition + layout.getSize()) {
        throw new IllegalStateException("wrong buffer size: " + out.getCurrentPosition() +
            " != " + startPosition + layout.getSize());
      }
    }
    return new ArrowBlock(start, (int)(out.getCurrentPosition() - start));
  }

  /**
   * Deserializes a RecordBatch
   */
  public static ArrowRecordBatch deserializeRecordBatch(ReadChannel in,
      BufferAllocator alloc) throws IOException {
    Message header = deserializeHeader(in, MessageHeader.RecordBatch);
    if (header == null) return null;

    int messageLen = (int)header.bodyLength();
    // Now read the buffer. This has the metadata followed by the data.
    ArrowBuf buffer = alloc.buffer(messageLen);
    long readPosition = in.getCurrentPositiion();
    if (in.readFully(buffer, messageLen) != messageLen) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }
    return deserializeRecordBatch(buffer, readPosition, messageLen);
  }

  /**
   * Deserializes a RecordBatch knowing the size of the entire message up front. This
   * minimizes the number of reads to the underlying stream.
   */
  public static ArrowRecordBatch deserializeRecordBatch(ReadChannel in, int messageLen,
      BufferAllocator alloc) throws IOException {
    ArrowBuf buffer = alloc.buffer(messageLen);
    long readPosition = in.getCurrentPositiion();
    if (in.readFully(buffer, messageLen) != messageLen) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }

    byte[] headerLenBytes = new byte[4];
    buffer.getBytes(0, headerLenBytes);
    int headerLen = bytesToInt(headerLenBytes);
    buffer = buffer.slice(4, messageLen - 4);
    messageLen -=4;
    readPosition += 4;

    Message header = Message.getRootAsMessage(buffer.nioBuffer());
    if (header.headerType() != MessageHeader.RecordBatch) {
      throw new IOException("Invalid message: expecting " + MessageHeader.RecordBatch +
          ". Message contained: " + header.headerType());
    }

    buffer = buffer.slice(headerLen, messageLen - headerLen);
    messageLen -= headerLen;
    readPosition += headerLen;
    return deserializeRecordBatch(buffer, readPosition, messageLen);
  }

  private static ArrowRecordBatch deserializeRecordBatch(
      ArrowBuf buffer, long readPosition, int bufferLen) {
    // Read the metadata. It starts with the 4 byte size of the metadata.
    int metadataSize = buffer.readInt();
    RecordBatch recordBatchFB =
        RecordBatch.getRootAsRecordBatch(buffer.nioBuffer().asReadOnlyBuffer());

    int bufferOffset = 4 + metadataSize;
    readPosition += bufferOffset;
    if (readPosition % 8 != 0) {
      bufferOffset += (int)(8 - readPosition % 8);
    }

    // Now read the body
    final ArrowBuf body = buffer.slice(bufferOffset, bufferLen - bufferOffset);
    int nodesLength = recordBatchFB.nodesLength();
    List<ArrowFieldNode> nodes = new ArrayList<>();
    for (int i = 0; i < nodesLength; ++i) {
      FieldNode node = recordBatchFB.nodes(i);
      nodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
    }
    List<ArrowBuf> buffers = new ArrayList<>();
    for (int i = 0; i < recordBatchFB.buffersLength(); ++i) {
      Buffer bufferFB = recordBatchFB.buffers(i);
      ArrowBuf vectorBuffer = body.slice((int)bufferFB.offset(), (int)bufferFB.length());
      buffers.add(vectorBuffer);
    }
    ArrowRecordBatch arrowRecordBatch =
        new ArrowRecordBatch(recordBatchFB.length(), nodes, buffers);
    buffer.release();
    return arrowRecordBatch;
  }

  /**
   * Serializes a message header.
   */
  private static ByteBuffer serializeHeader(byte headerType, int bodyLength) {
    FlatBufferBuilder headerBuilder = new FlatBufferBuilder();
    Message.startMessage(headerBuilder);
    Message.addHeaderType(headerBuilder, headerType);
    Message.addVersion(headerBuilder, MetadataVersion.V1);
    Message.addBodyLength(headerBuilder, bodyLength);
    headerBuilder.finish(Message.endMessage(headerBuilder));
    return headerBuilder.dataBuffer();
  }

  private static Message deserializeHeader(ReadChannel in, byte headerType) throws IOException {
    // Read the header size. There is an i32 little endian prefix.
    ByteBuffer buffer = ByteBuffer.allocate(4);
    if (in.readFully(buffer) != 4) {
      return null;
    }

    int headerLength = bytesToInt(buffer.array());
    buffer = ByteBuffer.allocate(headerLength);
    if (in.readFully(buffer) != headerLength) {
      throw new IOException(
          "Unexpected end of stream trying to read header.");
    }
    buffer.rewind();

    Message header = Message.getRootAsMessage(buffer);
    if (header.headerType() != headerType) {
      throw new IOException("Invalid message: expecting " + headerType +
          ". Message contained: " + header.headerType());
    }
    return header;
  }
}
