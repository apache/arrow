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

import java.nio.ByteBuffer;

/**
 * Wrapper around flatbuffer message table.  This keeps from exposing
 * flatbuffer details to upstream packages.
 */
public class ArrowMessageHeader {
  private org.apache.arrow.flatbuf.Message message;

  private ArrowMessageHeader(org.apache.arrow.flatbuf.Message message) {
    this.message = message;
  }

  public static ArrowMessageHeader create(ByteBuffer buffer) {
    return new ArrowMessageHeader(org.apache.arrow.flatbuf.Message.getRootAsMessage(buffer));
  }

  public  byte headerType() {
    return message.headerType();
  }

  public ByteBuffer byteBuffer() {
    return message.getByteBuffer();
  }

  public int bytesAfterMessage() {
    return message.getByteBuffer().remaining();
  }

  public org.apache.arrow.flatbuf.Message getMessage() {
    return message;
  }
}
