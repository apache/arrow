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
package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapOrListWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

public class MapOrListWriterImpl implements MapOrListWriter {

  public final BaseWriter.MapWriter map;
  public final BaseWriter.ListWriter list;

  public MapOrListWriterImpl(final BaseWriter.MapWriter writer) {
    this.map = writer;
    this.list = null;
  }

  public MapOrListWriterImpl(final BaseWriter.ListWriter writer) {
    this.map = null;
    this.list = writer;
  }

  public void start() {
    if (map != null) {
      map.start();
    } else {
      list.startList();
    }
  }

  public void end() {
    if (map != null) {
      map.end();
    } else {
      list.endList();
    }
  }

  public MapOrListWriter map(final String name) {
    assert map != null;
    return new MapOrListWriterImpl(map.map(name));
  }

  public MapOrListWriter listoftmap(final String name) {
    assert list != null;
    return new MapOrListWriterImpl(list.map());
  }

  public MapOrListWriter list(final String name) {
    assert map != null;
    return new MapOrListWriterImpl(map.list(name));
  }

  public boolean isMapWriter() {
    return map != null;
  }

  public boolean isListWriter() {
    return list != null;
  }

  @Override
  public VarCharWriter varChar(final String name) {
    return varChar(name, null);
  }

  @Override
  public VarCharWriter varChar(String name, DictionaryEncoding dictionary) {
    return (map != null) ? map.varChar(name, dictionary) : list.varChar(dictionary);
  }

  @Override
  public IntWriter integer(final String name) {
    return integer(name, null);
  }

  @Override
  public IntWriter integer(String name, DictionaryEncoding dictionary) {
    return (map != null) ? map.integer(name, dictionary) : list.integer(dictionary);
  }

  @Override
  public BigIntWriter bigInt(final String name) {
    return bigInt(name, null);
  }

  @Override
  public BigIntWriter bigInt(String name, DictionaryEncoding dictionary) {
    return (map != null) ? map.bigInt(name, dictionary) : list.bigInt(dictionary);
  }

  @Override
  public Float4Writer float4(final String name) {
    return float4(name, null);
  }

  @Override
  public Float4Writer float4(String name, DictionaryEncoding dictionary) {
    return (map != null) ? map.float4(name, dictionary) : list.float4(dictionary);
  }

  @Override
  public Float8Writer float8(final String name) {
    return float8(name, null);
  }

  @Override
  public Float8Writer float8(String name, DictionaryEncoding dictionary) {
    return (map != null) ? map.float8(name, dictionary) : list.float8(dictionary);
  }

  @Override
  public BitWriter bit(final String name) {
    return bit(name, null);
  }

  @Override
  public BitWriter bit(String name, DictionaryEncoding dictionary) {
    return (map != null) ? map.bit(name, dictionary) : list.bit(dictionary);
  }

  @Override
  public VarBinaryWriter binary(final String name) {
    return binary(name, null);
  }

  @Override
  public VarBinaryWriter binary(String name, DictionaryEncoding dictionary) {
    return (map != null) ? map.varBinary(name, dictionary) : list.varBinary(dictionary);
  }

}
