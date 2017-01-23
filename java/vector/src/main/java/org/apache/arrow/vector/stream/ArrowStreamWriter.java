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
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.arrow.vector.file.WriteChannel;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowStreamWriter implements AutoCloseable {
  private final WriteChannel out;
  private final Schema schema;
  private boolean headerSent = false;

  /**
   * Creates the stream writer. non-blocking.
   * totalBatches can be set if the writer knows beforehand. Can be -1 if unknown.
   */
  public ArrowStreamWriter(WritableByteChannel out, Schema schema) {
    this.out = new WriteChannel(out);
    this.schema = schema;
  }

  public ArrowStreamWriter(OutputStream out, Schema schema)
      throws IOException {
    this(Channels.newChannel(out), schema);
  }

  public long bytesWritten() { return out.getCurrentPosition(); }

  public void writeRecordBatch(ArrowRecordBatch batch) throws IOException {
    // Send the header if we have not yet.
    checkAndSendHeader();
    MessageSerializer.serialize(out, batch);
  }

  /**
   * End the stream. This is not required and this object can simply be closed.
   */
  public void end() throws IOException {
    checkAndSendHeader();
    out.writeIntLittleEndian(0);
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
      MessageSerializer.serialize(out, schema);
      headerSent = true;
    }
  }
}

