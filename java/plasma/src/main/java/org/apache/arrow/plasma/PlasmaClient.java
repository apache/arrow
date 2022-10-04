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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.arrow.plasma.exceptions.PlasmaOutOfMemoryException;

/**
 * The PlasmaClient is used to interface with a plasma store and manager.
 *
 * <p>The PlasmaClient can ask the PlasmaStore to allocate a new buffer, seal a buffer, and get a
 * buffer. Buffers are referred to by object IDs.
 *
 * @deprecated Plasma is deprecated since 10.0.0. Plasma will not be released from Apache Arrow 12.0.0 or so.
 */
@Deprecated
public class PlasmaClient implements ObjectStoreLink {

  private final long conn;

  protected void finalize() {
    PlasmaClientJNI.disconnect(this.conn);
  }

  // use plasma client to initialize the underlying jni system as well via config and config-overwrites
  public PlasmaClient(String storeSocketName, String managerSocketName, int releaseDelay) {
    this.conn = PlasmaClientJNI.connect(storeSocketName, managerSocketName, releaseDelay);
  }

  // interface methods --------------------

  @Override
  public void put(byte[] objectId, byte[] value, byte[] metadata)
          throws DuplicateObjectException, PlasmaOutOfMemoryException {
    ByteBuffer buf = PlasmaClientJNI.create(conn, objectId, value.length, metadata);
    buf.put(value);
    PlasmaClientJNI.seal(conn, objectId);
    PlasmaClientJNI.release(conn, objectId);
  }

  @Override
  public List<byte[]> get(byte[][] objectIds, int timeoutMs, boolean isMetadata) {
    ByteBuffer[][] bufs = PlasmaClientJNI.get(conn, objectIds, timeoutMs);
    assert bufs.length == objectIds.length;

    List<byte[]> ret = new ArrayList<>();
    for (int i = 0; i < bufs.length; i++) {
      ByteBuffer buf = bufs[i][isMetadata ? 1 : 0];
      if (buf == null) {
        ret.add(null);
      } else {
        byte[] bb = new byte[buf.remaining()];
        buf.get(bb);
        ret.add(bb);
      }
    }
    return ret;
  }

  @Override
  public byte[] hash(byte[] objectId) {
    return PlasmaClientJNI.hash(conn, objectId);
  }

  @Override
  public List<ObjectStoreData> get(byte[][] objectIds, int timeoutMs) {
    ByteBuffer[][] bufs = PlasmaClientJNI.get(conn, objectIds, timeoutMs);
    assert bufs.length == objectIds.length;

    List<ObjectStoreData> ret = new ArrayList<>();
    for (int i = 0; i < bufs.length; i++) {
      ByteBuffer databuf = bufs[i][0];
      ByteBuffer metabuf = bufs[i][1];
      if (databuf == null) {
        ret.add(new ObjectStoreData(null, null));
      } else {
        byte[] data = new byte[databuf.remaining()];
        databuf.get(data);
        byte[] meta;
        if (metabuf != null) {
          meta = new byte[metabuf.remaining()];
          metabuf.get(meta);
        } else {
          meta = null;
        }
        ret.add(new ObjectStoreData(meta, data));
      }
    }
    return ret;
  }

  /**
   * Get an object in Plasma Store with objectId. Will return an off-heap ByteBuffer.
   *
   * @param objectId used to identify an object.
   * @param timeoutMs time in milliseconfs to wait before this request time out.
   * @param isMetadata get this object's metadata or data.
   */
  public ByteBuffer getObjAsByteBuffer(byte[] objectId, int timeoutMs, boolean isMetadata) {
    byte[][] objectIds = new byte[][]{objectId};
    ByteBuffer[][] bufs = PlasmaClientJNI.get(conn, objectIds, timeoutMs);
    return bufs[0][isMetadata ? 1 : 0];
  }

  @Override
  public List<byte[]> list() {
    return Arrays.asList(PlasmaClientJNI.list(conn));
  }

  @Override
  public long evict(long numBytes) {
    return PlasmaClientJNI.evict(conn, numBytes);
  }

  // wrapper methods --------------------

  /**
   * Create an object in Plasma Store with particular size. Will return an off-heap ByteBuffer.
   *
   * @param objectId used to identify an object.
   * @param size size in bytes to be allocated for this object.
   * @param metadata this object's metadata. It should be null if there is no metadata.
   */
  public ByteBuffer create(byte[] objectId, int size, byte[] metadata)
        throws DuplicateObjectException, PlasmaOutOfMemoryException {
    return PlasmaClientJNI.create(conn, objectId, size, metadata);
  }

  /**
   * Seal the buffer in the PlasmaStore for a particular object ID.
   * Once a buffer has been sealed, the buffer is immutable and can only be accessed through get.
   *
   * @param objectId used to identify an object.
   */
  public void seal(byte[] objectId) {
    PlasmaClientJNI.seal(conn, objectId);
  }

  /**
   * Notify Plasma that the object is no longer needed.
   *
   * @param objectId used to identify an object.
   */
  public void release(byte[] objectId) {
    PlasmaClientJNI.release(conn, objectId);
  }

  /**
   * Removes object with given objectId from plasma store.
   *
   * @param objectId used to identify an object.
   */
  @Override
  public void delete(byte[] objectId) {
    PlasmaClientJNI.delete(conn, objectId);
  }

  /**
   * Check if the object is present and has been sealed in the PlasmaStore.
   *
   * @param objectId used to identify an object.
   */
  @Override
  public boolean contains(byte[] objectId) {
    return PlasmaClientJNI.contains(conn, objectId);
  }
}
