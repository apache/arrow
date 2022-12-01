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

import java.util.List;

import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.arrow.plasma.exceptions.PlasmaOutOfMemoryException;

/**
 * Object store interface, which provides the capabilities to put and get raw byte array, and serves.
 *
 * @deprecated Plasma is deprecated since 10.0.0. Plasma will not be released from Apache Arrow 12.0.0 or so.
 */
@Deprecated
public interface ObjectStoreLink {

  /**
   * Tuple for data and metadata stored in Plasma.
   */
  class ObjectStoreData {

    public ObjectStoreData(byte[] metadata, byte[] data) {
      this.data = data;
      this.metadata = metadata;
    }

    public final byte[] metadata;
    public final byte[] data;
  }

  /**
   * Put value in the local plasma store with object ID <tt>objectId</tt>.
   *
   * @param objectId The object ID of the value to be put.
   * @param value The value to put in the object store.
   * @param metadata encodes whatever metadata the user wishes to encode.
   */
  void put(byte[] objectId, byte[] value, byte[] metadata)
          throws DuplicateObjectException, PlasmaOutOfMemoryException;

  /**
   * Get a buffer from the PlasmaStore based on the <tt>objectId</tt>.
   *
   * @param objectId The object ID used to identify the object.
   * @param timeoutMs The number of milliseconds that the get call should block before timing out
   *                  and returning. Pass -1 if the call should block and 0 if the call should return immediately.
   * @param isMetadata false if get data, otherwise get metadata.
   * @return A PlasmaBuffer wrapping the object.
   */
  default byte[] get(byte[] objectId, int timeoutMs, boolean isMetadata) {
    byte[][] objectIds = {objectId};
    return get(objectIds, timeoutMs, isMetadata).get(0);
  }

  /**
   * Get buffers from the PlasmaStore based on <tt>objectIds</tt>.
   *
   * @param objectIds List of object IDs used to identify some objects.
   * @param timeoutMs The number of milliseconds that the get call should block before timing out
   *                  and returning. Pass -1 if the call should block and 0 if the call should return immediately.
   * @param isMetadata false if get data, otherwise get metadata.
   * @return List of PlasmaBuffers wrapping objects.
   */
  List<byte[]> get(byte[][] objectIds, int timeoutMs, boolean isMetadata);

  /**
   * Get buffer pairs (data & metadata) from the PlasmaStore based on <tt>objectIds</tt>.
   *
   * @param objectIds List of object IDs used to identify some objects.
   * @param timeoutMs The number of milliseconds that the get call should block before timing out
   *                  and returning. Pass -1 if the call should block and 0 if the call should return immediately.
   * @return List of Pairs of PlasmaBuffer wrapping objects and its metadata.
   */
  List<ObjectStoreData> get(byte[][] objectIds, int timeoutMs);

  /**
   * Compute the hash of an object in the object store.
   *
   * @param objectId The object ID used to identify the object.
   * @return A digest byte array contains object's SHA256 hash. <tt>null</tt> means that the object
   *         isn't in the object store.
   */
  byte[] hash(byte[] objectId);

  /**
   * Evict some objects to recover given count of bytes.
   *
   * @param numBytes The number of bytes to attempt to recover.
   * @return The number of bytes that have been evicted.
   */
  long evict(long numBytes);

  /**
   * Release the reference of the object.
   *
   * @param objectId The object ID used to release the reference of the object.
   */
  void release(byte[] objectId);

  /**
   * Removes object with given objectId from plasma store.
   *
   * @param objectId used to identify an object.
   */
  void delete(byte[] objectId);

  /**
   * Check if the object is present and has been sealed in the PlasmaStore.
   *
   * @param objectId used to identify an object.
   */
  boolean contains(byte[] objectId);

  /**
   * List all objects in the PlasmaStore.
   */
  List<byte[]> list();
}
