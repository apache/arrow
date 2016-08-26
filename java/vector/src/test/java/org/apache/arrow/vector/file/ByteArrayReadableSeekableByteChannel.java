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
package org.apache.arrow.vector.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class ByteArrayReadableSeekableByteChannel implements SeekableByteChannel {
  private byte[] byteArray;
  private int position = 0;

  public ByteArrayReadableSeekableByteChannel(byte[] byteArray) {
    if (byteArray == null) {
      throw new NullPointerException();
    }
    this.byteArray = byteArray;
  }

  @Override
  public boolean isOpen() {
    return byteArray != null;
  }

  @Override
  public void close() throws IOException {
    byteArray = null;
  }

  @Override
  public int read(final ByteBuffer dst) throws IOException {
    int remainingInBuf = byteArray.length - this.position;
    int length = Math.min(dst.remaining(), remainingInBuf);
    dst.put(this.byteArray, this.position, length);
    this.position += length;
    return length;
  }

  @Override
  public long position() throws IOException {
    return this.position;
  }

  @Override
  public SeekableByteChannel position(final long newPosition) throws IOException {
    this.position = (int)newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    return this.byteArray.length;
  }

  @Override
  public int write(final ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Read only");
  }

  @Override
  public SeekableByteChannel truncate(final long size) throws IOException {
    throw new UnsupportedOperationException("Read only");
  }

}
