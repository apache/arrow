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


/**
 * The PlasmaClient is used to interface with a plasma store and manager.
 *
 * <p>The PlasmaClient can ask the PlasmaStore to allocate a new buffer, seal a buffer, and get a
 * buffer. Buffers are referred to by object IDs.
 */
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
  public void put(byte[] objectId, byte[] value, byte[] metadata) {
    ByteBuffer buf = null;
    try {
      buf = PlasmaClientJNI.create(conn, objectId, value.length, metadata);
    } catch (Exception e) {
      System.err.println("ObjectId " + objectId + " error at PlasmaClient put");
      e.printStackTrace();
    }
    if (buf == null) {
      return;
    }

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
        ret.add(new ObjectStoreData(data, meta));
      }
    }
    return ret;
  }

  @Override
  public long evict(long numBytes) {
    return PlasmaClientJNI.evict(conn, numBytes);
  }

  // wrapper methods --------------------

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
