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

package org.apache.arrow.flight.grpc;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import io.netty.buffer.ByteBuf;

/**
 * Allow a user to add a ByteBuf based InputStream directly into GRPC WritableBuffer to avoid an
 * extra copy. This could be solved in GRPC by adding a ByteBufListable interface on InputStream and
 * letting BufferChainOutputStream take adavantage of it.
 */
public class AddWritableBuffer {

  private static final Constructor<?> bufConstruct;
  private static final Field bufferList;
  private static final Field current;
  private static final Method listAdd;
  private static final Class<?> bufChainOut;

  static {

    Constructor<?> tmpConstruct = null;
    Field tmpBufferList = null;
    Field tmpCurrent = null;
    Class<?> tmpBufChainOut = null;
    Method tmpListAdd = null;

    try {
      Class<?> nwb = Class.forName("io.grpc.netty.NettyWritableBuffer");

      Constructor<?> tmpConstruct2 = nwb.getDeclaredConstructor(ByteBuf.class);
      tmpConstruct2.setAccessible(true);

      Class<?> tmpBufChainOut2 = Class.forName("io.grpc.internal.MessageFramer$BufferChainOutputStream");

      Field tmpBufferList2 = tmpBufChainOut2.getDeclaredField("bufferList");
      tmpBufferList2.setAccessible(true);

      Field tmpCurrent2 = tmpBufChainOut2.getDeclaredField("current");
      tmpCurrent2.setAccessible(true);

      Method tmpListAdd2 = List.class.getDeclaredMethod("add", Object.class);

      // output fields last.
      tmpConstruct = tmpConstruct2;
      tmpBufferList = tmpBufferList2;
      tmpCurrent = tmpCurrent2;
      tmpListAdd = tmpListAdd2;
      tmpBufChainOut = tmpBufChainOut2;

    } catch (Exception ex) {
      ex.printStackTrace();
    }

    bufConstruct = tmpConstruct;
    bufferList = tmpBufferList;
    current = tmpCurrent;
    listAdd = tmpListAdd;
    bufChainOut = tmpBufChainOut;

  }

  /**
   * Add the provided ByteBuf to the output stream if it is possible.
   * @param buf The buffer to add.
   * @param stream The Candidate OutputStream to add to.
   * @return True if added. False if not possible.
   * @throws IOException
   */
  public static boolean add(ByteBuf buf, OutputStream stream) throws IOException {
    buf.readBytes(stream, buf.readableBytes());

    if (bufChainOut == null) {
      return false;
    }

    if (!stream.getClass().equals(bufChainOut)) {
      return false;
    }

    try {
      if (current.get(stream) != null) {
        return false;
      }

      buf.retain();
      Object obj = bufConstruct.newInstance(buf);
      Object list = bufferList.get(stream);
      listAdd.invoke(list, obj);
      current.set(stream, obj);
      return true;
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException e) {
      e.printStackTrace();
      return false;
    }
  }
}
