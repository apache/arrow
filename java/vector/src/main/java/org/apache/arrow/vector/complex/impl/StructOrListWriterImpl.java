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

package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructOrListWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;

public class StructOrListWriterImpl implements StructOrListWriter {

  public final BaseWriter.StructWriter struct;
  public final BaseWriter.ListWriter list;

  public StructOrListWriterImpl(final BaseWriter.StructWriter writer) {
    this.struct = writer;
    this.list = null;
  }

  public StructOrListWriterImpl(final BaseWriter.ListWriter writer) {
    this.struct = null;
    this.list = writer;
  }

  public void start() {
    if (struct != null) {
      struct.start();
    } else {
      list.startList();
    }
  }

  public void end() {
    if (struct != null) {
      struct.end();
    } else {
      list.endList();
    }
  }

  public StructOrListWriter struct(final String name) {
    assert struct != null;
    return new StructOrListWriterImpl(struct.struct(name));
  }

  public StructOrListWriter listoftstruct(final String name) {
    assert list != null;
    return new StructOrListWriterImpl(list.struct());
  }

  public StructOrListWriter list(final String name) {
    assert struct != null;
    return new StructOrListWriterImpl(struct.list(name));
  }

  public boolean isStructWriter() {
    return struct != null;
  }

  public boolean isListWriter() {
    return list != null;
  }

  public VarCharWriter varChar(final String name) {
    return (struct != null) ? struct.varChar(name) : list.varChar();
  }

  public IntWriter integer(final String name) {
    return (struct != null) ? struct.integer(name) : list.integer();
  }

  public BigIntWriter bigInt(final String name) {
    return (struct != null) ? struct.bigInt(name) : list.bigInt();
  }

  public Float4Writer float4(final String name) {
    return (struct != null) ? struct.float4(name) : list.float4();
  }

  public Float8Writer float8(final String name) {
    return (struct != null) ? struct.float8(name) : list.float8();
  }

  public BitWriter bit(final String name) {
    return (struct != null) ? struct.bit(name) : list.bit();
  }

  public VarBinaryWriter binary(final String name) {
    return (struct != null) ? struct.varBinary(name) : list.varBinary();
  }

}
