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

package org.apache.arrow.dataset.jni;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.dataset.ParquetWriteSupport;
import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestReservationListener extends TestDataset {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  public static final String AVRO_SCHEMA_USER = "user.avsc";

  /**
   * The default block size of C++ ReservationListenableMemoryPool.
   */
  public static final long DEFAULT_NATIVE_MEMORY_POOL_BLOCK_SIZE = 512 * 1024;

  @Test
  public void testDirectReservationListener() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");
    NativeMemoryPool pool = NativeMemoryPool.createListenable(DirectReservationListener.instance());
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        pool, FileFormat.PARQUET,
        writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(new String[0], 100);
    long initReservation = DirectReservationListener.instance().getCurrentDirectMemReservation();
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    long reservation = DirectReservationListener.instance().getCurrentDirectMemReservation();
    AutoCloseables.close(datum);
    AutoCloseables.close(pool);
    long finalReservation = DirectReservationListener.instance().getCurrentDirectMemReservation();
    final long expected_diff = DEFAULT_NATIVE_MEMORY_POOL_BLOCK_SIZE;
    Assert.assertEquals(expected_diff, reservation - initReservation);
    Assert.assertEquals(-expected_diff, finalReservation - reservation);
  }

  @Test
  public void testCustomReservationListener() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");
    final AtomicLong reserved = new AtomicLong(0L);
    ReservationListener listener = new ReservationListener() {
      @Override
      public void reserve(long size) {
        reserved.getAndAdd(size);
      }

      @Override
      public void unreserve(long size) {
        reserved.getAndAdd(-size);
      }
    };
    NativeMemoryPool pool = NativeMemoryPool.createListenable(listener);
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        pool, FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(new String[0], 100);
    long initReservation = reserved.get();
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    long reservation = reserved.get();
    AutoCloseables.close(datum);
    AutoCloseables.close(pool);
    long finalReservation = reserved.get();
    final long expected_diff = DEFAULT_NATIVE_MEMORY_POOL_BLOCK_SIZE;
    Assert.assertEquals(expected_diff, reservation - initReservation);
    Assert.assertEquals(-expected_diff, finalReservation - reservation);
  }
}
