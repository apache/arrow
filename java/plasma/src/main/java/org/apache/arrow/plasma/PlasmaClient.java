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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;



/**
 * The PlasmaClient is used to interface with a plasma store and manager.
 *
 * The PlasmaClient can ask the PlasmaStore to allocate a new buffer, seal a buffer, and get a
 * buffer. Buffers are referred to by object IDs.
 */
public class PlasmaClient implements ObjectStoreLink {

  private final long conn;

  protected void finalize() {
    PlasmaClientJNI.disconnect(this.conn);
  }

  // use plasma client to initialize the underlying jni system as well via config and config-overwrites
  public PlasmaClient(String configFile, String configOverwrites, String storeSocketName,
      String managerSocketName, int releaseDelay) {
    this.conn = PlasmaClientJNI.connect(
        configFile, configOverwrites, storeSocketName, managerSocketName, releaseDelay);
  }

  // interface methods --------------------

  @Override
  public void put(ObjectId objectId, byte[] value, byte[] metadata) {
    ByteBuffer buf = null;
    try {
      buf = PlasmaClientJNI.create(conn, objectId.getBytes(), value.length, metadata);
    } catch (Exception e) {
      System.err.println("ObjectId " + objectId + " error at PlasmaClient put");
      e.printStackTrace();
    }
    if (buf == null) {
      return;
    }

    buf.put(value);
    PlasmaClientJNI.seal(conn, objectId.getBytes());
    PlasmaClientJNI.release(conn, objectId.getBytes());
  }

  @Override
  public List<ObjectBuffer> get(List<? extends ObjectId> objectIds, int timeoutMs, boolean isMetadata) {
    byte[][] ids = getIdBytes(objectIds);
    ByteBuffer[][] bufs = PlasmaClientJNI.get(conn, ids, timeoutMs);
    assert bufs.length == objectIds.size();

    List<ObjectBuffer> ret = new ArrayList<>();
    for (int i = 0; i < bufs.length; i++) {
      ObjectId oid = objectIds.get(i);
      ByteBuffer buf = bufs[i][isMetadata ? 1 : 0];
      if (buf == null) {
        ret.add(new ObjectBuffer(null, null));
      } else {
        byte[] bb = new byte[buf.remaining()];
        buf.get(bb);
        ret.add(new ObjectBuffer(bb, (byte[] b) -> this.release(oid)));
      }
    }
    return ret;
  }

  private static byte[][] getIdBytes(List<? extends ObjectId> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
  }

  @Override
  public List<ObjectId> wait(List<? extends ObjectId> objectIds, int timeoutMs, int numReturns) {
    byte[][] ids = getIdBytes(objectIds);
    byte[][] readys = PlasmaClientJNI.wait(conn, ids, timeoutMs, numReturns);

    List<ObjectId> ret = new ArrayList<>();
    for (byte[] ready : readys) {
      for (ObjectId id : objectIds) {
        if (Arrays.equals(ready, id.getBytes())) {
          ret.add(id);
          break;
        }
      }
    }

    assert (ret.size() == readys.length);
    return ret;
  }

  @Override
  public byte[] hash(ObjectId objectId) {
    return PlasmaClientJNI.hash(conn, objectId.getBytes());
  }

  @Override
  public void fetch(List<? extends ObjectId> objectIds) {
    byte[][] ids = getIdBytes(objectIds);
    PlasmaClientJNI.fetch(conn, ids);
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
  public void seal(ObjectId objectId) {
    PlasmaClientJNI.seal(conn, objectId.getBytes());
  }

  /**
   * Notify Plasma that the object is no longer needed.
   *
   * @param objectId used to identify an object.
   */
  public void release(ObjectId objectId) {
    PlasmaClientJNI.release(conn, objectId.getBytes());
  }

  /**
   * Check if the object is present and has been sealed in the PlasmaStore.
   *
   * @param objectId used to identify an object.
   */
  public boolean contains(ObjectId objectId) {
    return PlasmaClientJNI.contains(conn, objectId.getBytes());
  }
}
