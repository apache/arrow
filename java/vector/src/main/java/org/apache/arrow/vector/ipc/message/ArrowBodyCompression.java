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

import org.apache.arrow.flatbuf.BodyCompression;

import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Compression information about data written to a channel.
 */
public class ArrowBodyCompression implements FBSerializable {

  private final byte codec;

  private final byte method;

  public ArrowBodyCompression(byte codec, byte method) {
    this.codec = codec;
    this.method = method;
  }

  @Override
  public int writeTo(FlatBufferBuilder builder) {
    return BodyCompression.createBodyCompression(builder, codec, method);
  }

  public byte getCodec() {
    return codec;
  }

  public byte getMethod() {
    return method;
  }

  @Override
  public String toString() {
    return "ArrowBodyCompression [codec=" + codec + ", method=" + method + "]";
  }
}
