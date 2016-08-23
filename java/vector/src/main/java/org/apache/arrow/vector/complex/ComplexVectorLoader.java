/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.complex;

import java.util.Iterator;

import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.schema.ArrowFieldNode;

import io.netty.buffer.ArrowBuf;

public class ComplexVectorLoader {

  public static void load(ListVector listVector, ArrowFieldNode node, Iterator<ArrowBuf> buffers) {
    // listVector.valueCount = node.getLength(); ?
    VectorLoader.load(listVector.offsets, buffers.next());
    VectorLoader.load(listVector.bits, buffers.next());
  }

  public static void load(MapVector mapVector, ArrowFieldNode node, Iterator<ArrowBuf> buffers) {
    mapVector.valueCount = node.getLength();
    // no vector of it's own?
  }

}
