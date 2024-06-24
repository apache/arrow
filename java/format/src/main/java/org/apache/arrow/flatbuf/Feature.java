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
package org.apache.arrow.flatbuf;

/**
 * Represents Arrow Features that might not have full support
 * within implementations. This is intended to be used in
 * two scenarios:
 *  1.  A mechanism for readers of Arrow Streams
 *      and files to understand that the stream or file makes
 *      use of a feature that isn't supported or unknown to
 *      the implementation (and therefore can meet the Arrow
 *      forward compatibility guarantees).
 *  2.  A means of negotiating between a client and server
 *      what features a stream is allowed to use. The enums
 *      values here are intented to represent higher level
 *      features, additional details maybe negotiated
 *      with key-value pairs specific to the protocol.
 *
 * Enums added to this list should be assigned power-of-two values
 * to facilitate exchanging and comparing bitmaps for supported
 * features.
 */
@SuppressWarnings("unused")
public final class Feature {
  private Feature() { }
  /**
   * Needed to make flatbuffers happy.
   */
  public static final long UNUSED = 0L;
  /**
   * The stream makes use of multiple full dictionaries with the
   * same ID and assumes clients implement dictionary replacement
   * correctly.
   */
  public static final long DICTIONARY_REPLACEMENT = 1L;
  /**
   * The stream makes use of compressed bodies as described
   * in Message.fbs.
   */
  public static final long COMPRESSED_BODY = 2L;
}

