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
package org.apache.arrow.vector.ipc.message;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.DictionaryBatch;
import org.apache.arrow.flatbuf.MessageHeader;

/**
 * POJO wrapper around a Dictionary Batch IPC messages
 * (https://arrow.apache.org/docs/format/IPC.html#dictionary-batches)
 */
public class ArrowDictionaryBatch implements ArrowMessage {

  private final long dictionaryId;
  private final ArrowRecordBatch dictionary;
  private final boolean isDelta;

  @Deprecated
  public ArrowDictionaryBatch(long dictionaryId, ArrowRecordBatch dictionary) {
    this(dictionaryId, dictionary, false);
  }

  /** Constructs new instance. */
  public ArrowDictionaryBatch(long dictionaryId, ArrowRecordBatch dictionary, boolean isDelta) {
    this.dictionaryId = dictionaryId;
    this.dictionary = dictionary;
    this.isDelta = isDelta;
  }

  public boolean isDelta() {
    return isDelta;
  }

  public byte getMessageType() {
    return MessageHeader.DictionaryBatch;
  }

  public long getDictionaryId() {
    return dictionaryId;
  }

  public ArrowRecordBatch getDictionary() {
    return dictionary;
  }

  @Override
  public int writeTo(FlatBufferBuilder builder) {
    int dataOffset = dictionary.writeTo(builder);
    DictionaryBatch.startDictionaryBatch(builder);
    DictionaryBatch.addId(builder, dictionaryId);
    DictionaryBatch.addData(builder, dataOffset);
    DictionaryBatch.addIsDelta(builder, isDelta);
    return DictionaryBatch.endDictionaryBatch(builder);
  }

  @Override
  public long computeBodyLength() {
    return dictionary.computeBodyLength();
  }

  @Override
  public <T> T accepts(ArrowMessageVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "ArrowDictionaryBatch [dictionaryId="
        + dictionaryId
        + ", dictionary="
        + dictionary
        + "]";
  }

  @Override
  public void close() {
    dictionary.close();
  }
}
