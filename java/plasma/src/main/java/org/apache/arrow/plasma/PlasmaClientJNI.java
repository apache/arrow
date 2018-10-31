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

package org.apache.arrow.plasma;

import java.nio.ByteBuffer;

/**
 * JNI static methods for PlasmaClient
 */
public class PlasmaClientJNI {

  public static native long connect(String storeSocketName, String managerSocketName, int releaseDelay);

  public static native void disconnect(long conn);

  public static native ByteBuffer create(long conn, byte[] objectId, int size, byte[] metadata);

  public static native byte[] hash(long conn, byte[] objectId);

  public static native void seal(long conn, byte[] objectId);

  public static native void release(long conn, byte[] objectId);

  public static native ByteBuffer[][] get(long conn, byte[][] objectIds, int timeoutMs);

  public static native boolean contains(long conn, byte[] objectId);

  public static native void fetch(long conn, byte[][] objectIds);

  public static native byte[][] wait(long conn, byte[][] objectIds, int timeoutMs,
      int numReturns);

  public static native long evict(long conn, long numBytes);

}
