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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.apache.arrow.vector.file.WriteChannel;
import org.apache.arrow.vector.schema.ArrowBuffer;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import io.netty.buffer.ArrowBuf;

public class ArrowStreamWriter implements AutoCloseable {
  private final WriteChannel out;
  private final ArrowStreamHeader header;
  private boolean headerSent = false;

  /**
   * Creates the stream writer. non-blocking.
   * totalBatches can be set if the writer knows beforehand. Can be -1 if unknown.
   */
  public ArrowStreamWriter(WritableByteChannel out, Schema schema, int totalBatches) {
    this.out = new WriteChannel(out);
    this.header = new ArrowStreamHeader(schema, totalBatches);
  }

  public ArrowStreamWriter(OutputStream out, Schema schema, int totalBatches)
      throws IOException {
    this(Channels.newChannel(out), schema, totalBatches);
  }

  public long bytesWritten() { return out.getCurrentPosition(); }

  /**
   * Computes the size of the serialized body for this recordBatch.
   */
  public int computeBodyLength(ArrowRecordBatch recordBatch) {
    int size = 0;

    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
    if (buffers.size() != buffersLayout.size()) {
      throw new IllegalStateException("the layout does not match: " +
          buffers.size() + " != " + buffersLayout.size());
    }

    for (int i = 0; i < buffers.size(); i++) {
      ArrowBuf buffer = buffers.get(i);
      ArrowBuffer layout = buffersLayout.get(i);
      size += (layout.getOffset() - size);
      ByteBuffer nioBuffer =
          buffer.nioBuffer(buffer.readerIndex(), buffer.readableBytes());
      size += nioBuffer.remaining();
    }
    return size;
  }

  public void writeRecordBatch(ArrowRecordBatch recordBatch) throws IOException {
    // Send the header if we have not yet.
    checkAndSendHeader();

    // We want to write:
    //   MetadataLength (int32 little endian)
    //   BodyLength (int32 little endian)
    //   Metadata
    //   Body
    int bodyLength = computeBodyLength(recordBatch);
    ByteBuffer metadata = WriteChannel.serialize(recordBatch);
    // Metadata is the length of the metadata including i32 prefix.
    out.writeIntLittleEndian(metadata.remaining() + 4);
    out.writeIntLittleEndian(bodyLength);
    // write header
    out.write(recordBatch, true);

    // write body
    long bodyOffset = out.getCurrentPosition();
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();

    for (int i = 0; i < buffers.size(); i++) {
      ArrowBuf buffer = buffers.get(i);
      ArrowBuffer layout = buffersLayout.get(i);
      long startPosition = bodyOffset + layout.getOffset();
      if (startPosition != out.getCurrentPosition()) {
        out.writeZeros((int)(startPosition - out.getCurrentPosition()));
      }
      out.write(buffer);
      if (out.getCurrentPosition() != startPosition + layout.getSize()) {
        throw new IllegalStateException("wrong buffer size: " + out.getCurrentPosition() +
            " != " + startPosition + layout.getSize());
      }
    }
    if (bodyLength != out.getCurrentPosition() - bodyOffset) {
      throw new IllegalStateException("Wrong computed body size: " + bodyLength +
          " Actual size: " + (out.getCurrentPosition() - bodyOffset));
    }
  }

  @Override
  public void close() throws IOException {
    // The header might not have been sent if this is an empty stream. Send it even in
    // this case so readers see a valid empty stream.
    checkAndSendHeader();
    out.close();
  }

  private void checkAndSendHeader() throws IOException {
    if (!headerSent) {
      this.out.write(header, true);
      headerSent = true;
    }
  }
}

