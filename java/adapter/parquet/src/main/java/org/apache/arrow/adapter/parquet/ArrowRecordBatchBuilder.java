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

package org.apache.arrow.adapter.parquet;

/**
 * ArrowRecordBatchBuilder.
 */
public class ArrowRecordBatchBuilder {

  public int length;
  public ArrowFieldNodeBuilder[] nodeBuilders;
  public ArrowBufBuilder[] bufferBuilders;

  /**
   * Create an instance to wrap native parameters for ArrowRecordBatchBuilder.
   * @param length ArrowRecordBatch rowNums.
   * @param nodeBuilders an Array hold ArrowFieldNodeBuilder.
   * @param bufferBuilders an Array hold ArrowBufBuilder.
   */
  public ArrowRecordBatchBuilder(
      int length, ArrowFieldNodeBuilder[] nodeBuilders, ArrowBufBuilder[] bufferBuilders) {
    this.length = length;
    this.nodeBuilders = nodeBuilders;
    this.bufferBuilders = bufferBuilders;
  }

} 
