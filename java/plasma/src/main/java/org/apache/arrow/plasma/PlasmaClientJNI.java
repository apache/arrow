/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

  native public static long connect(String storeSocketName, String managerSocketName, int releaseDelay);

  native public static void disconnect(long conn);

  native public static ByteBuffer create(long conn, byte[] objectId, int size, byte[] metadata);

  native public static byte[] hash(long conn, byte[] objectId);

  native public static void seal(long conn, byte[] objectId);

  native public static void release(long conn, byte[] objectId);

  native public static ByteBuffer[][] get(long conn, byte[][] objectIds, int timeoutMs);

  native public static boolean contains(long conn, byte[] objectId);

  native public static void fetch(long conn, byte[][] objectIds);

  native public static byte[][] wait(long conn, byte[][] objectIds, int timeoutMs,
      int numReturns);

  native public static long evict(long conn, long numBytes);

}
