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

  native public static long connect(String configFile, String overwrites, String store_socket_name,
      String manager_socket_name, int release_delay);

  native public static void disconnect(long conn);

  native public static ByteBuffer create(long conn, byte[] object_id, int size, byte[] metadata);

  native public static byte[] hash(long conn, byte[] object_id);

  native public static void seal(long conn, byte[] object_id);

  native public static void release(long conn, byte[] object_id);

  native public static ByteBuffer[][] get(long conn, byte[][] object_ids, int timeout_ms);

  native public static boolean contains(long conn, byte[] object_id);

  native public static void fetch(long conn, byte[][] object_ids);

  native public static byte[][] wait(long conn, byte[][] object_ids, int timeout_ms,
      int num_returns);

  native public static long evict(long conn, long num_bytes);

}
