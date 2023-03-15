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

package org.apache.arrow.dataset.substrait;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import com.google.common.collect.ImmutableList;

import io.substrait.proto.Plan;

public class TestTpchSubstraitConsumer extends TestDataset {
  private RootAllocator allocator = null;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  protected RootAllocator rootAllocator() {
    return allocator;
  }

  @Test
  public void testRunQueryNamedTableTpch01() throws Exception {
    // Query: Go to src/test/resources/substrait/tpch/sql/01.sql
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("lineitem.parquet"));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      Map<String, ArrowReader> mapReaderToTable = new HashMap<>();
      mapReaderToTable.put("LINEITEM", reader);
      Assertions.assertThrows(RuntimeException.class, () -> {
        try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
            getSubstraitTpchPlan("01.json"),
            mapReaderToTable
        )) {
          while (arrowReader.loadNextBatch()) {
          }
        }
      }, "conversion to arrow::compute::Declaration from Substrait relation sort");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRunBinaryQueryNamedTableTpch01() throws Exception {
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("lineitem.parquet"));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      Map<String, ArrowReader> mapReaderToTable = new HashMap<>();
      mapReaderToTable.put("LINEITEM", reader);
      // get binary oan
      String sql = "select\n" +
          "    l_returnflag,\n" +
          "    l_linestatus,\n" +
          "    sum(l_quantity) as sum_qty,\n" +
          "    sum(l_extendedprice) as sum_base_price,\n" +
          "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
          "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
          "    avg(l_quantity) as avg_qty,\n" +
          "    avg(l_extendedprice) as avg_price,\n" +
          "    avg(l_discount) as avg_disc,\n" +
          "    count(*) as count_order\n" +
          "from\n" +
          "    lineitem\n" +
          "where\n" +
          "    l_shipdate <= date '1998-12-01' - interval '120' day (3)\n" +
          "group by\n" +
          "    l_returnflag,\n" +
          "    l_linestatus\n" +
          "order by\n" +
          "    l_returnflag,\n" +
          "    l_linestatus";
      String lineitem = "CREATE TABLE LINEITEM (L_ORDERKEY BIGINT NOT NULL, L_PARTKEY BIGINT NOT NULL, " +
          "L_SUPPKEY BIGINT NOT NULL, L_LINENUMBER INTEGER, L_QUANTITY DECIMAL, L_EXTENDEDPRICE DECIMAL, " +
          "L_DISCOUNT DECIMAL, L_TAX DECIMAL, L_RETURNFLAG CHAR(1), L_LINESTATUS CHAR(1), L_SHIPDATE DATE, " +
          "L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT CHAR(25), L_SHIPMODE CHAR(10), " +
          "L_COMMENT VARCHAR(44) )";
      Plan plan = getPlan(sql, ImmutableList.of(lineitem));
      ByteBuffer directByteBuffer = ByteBuffer.allocateDirect(plan.toByteArray().length);
      directByteBuffer.put(plan.toByteArray());
      // run query
      Assertions.assertThrows(RuntimeException.class, () -> {
        try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
            directByteBuffer,
            mapReaderToTable
        )) {
          while (arrowReader.loadNextBatch()) {
          }
        }
      }, "conversion to arrow::compute::Declaration from Substrait relation sort");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRunQueryNamedTableTpch02() throws Exception {
    // Query: Go to src/test/resources/substrait/tpch/sql/02.sql
    ScanOptions optionsPart = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsSupplier = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsPartsupp = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsNation = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsRegion = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("part.parquet"));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(optionsPart);
        ArrowReader readerPart = scanner.scanBatches();

        DatasetFactory datasetFactorySupplier = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(), FileFormat.PARQUET, getNamedTableUri("supplier.parquet"));
        Dataset datasetSupplier = datasetFactorySupplier.finish();
        Scanner scannerSupplier = datasetSupplier.newScan(optionsSupplier);
        ArrowReader readerSupplier = scannerSupplier.scanBatches();

        DatasetFactory datasetFactoryPartsupp = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("partsupp.parquet"));
        Dataset datasetPartsupp = datasetFactoryPartsupp.finish();
        Scanner scannerPartsupp = datasetPartsupp.newScan(optionsPartsupp);
        ArrowReader readerPartsupp = scannerPartsupp.scanBatches();

        DatasetFactory datasetFactoryNation = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(), FileFormat.PARQUET, getNamedTableUri("nation.parquet"));
        Dataset datasetNation = datasetFactoryNation.finish();
        Scanner scannerNation = datasetNation.newScan(optionsNation);
        ArrowReader readerNation = scannerNation.scanBatches();

        DatasetFactory datasetFactoryRegion = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(), FileFormat.PARQUET, getNamedTableUri("region.parquet"));
        Dataset datasetRegion = datasetFactoryRegion.finish();
        Scanner scannerRegion = datasetRegion.newScan(optionsRegion);
        ArrowReader readerRegion = scannerRegion.scanBatches()

    ) {
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("PART", readerPart);
      mapTableToArrowReader.put("SUPPLIER", readerSupplier);
      mapTableToArrowReader.put("PARTSUPP", readerPartsupp);
      mapTableToArrowReader.put("NATION", readerNation);
      mapTableToArrowReader.put("REGION", readerRegion);
      Assertions.assertThrows(RuntimeException.class, () -> {
        try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
            getSubstraitTpchPlan("02.json"),
            mapTableToArrowReader
        )) {
          while (arrowReader.loadNextBatch()) {
          }
        }
      }, "conversion to arrow::compute::Declaration from Substrait relation sort");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRunQueryNamedTableTpch03() throws Exception {
    // Query: Go to src/test/resources/substrait/tpch/sql/03.sql
    ScanOptions optionsCustomer = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsOrders = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsLineitem = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("customer.parquet"));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(optionsCustomer);
        ArrowReader readerPart = scanner.scanBatches();

        DatasetFactory datasetFactoryOrders = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(), FileFormat.PARQUET, getNamedTableUri("orders.parquet"));
        Dataset datasetOrders = datasetFactoryOrders.finish();
        Scanner scannerOrders = datasetOrders.newScan(optionsOrders);
        ArrowReader readerOrders = scannerOrders.scanBatches();

        DatasetFactory datasetFactoryLineitem = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("lineitem.parquet"));
        Dataset datasetLineitem = datasetFactoryLineitem.finish();
        Scanner scannerLineitem = datasetLineitem.newScan(optionsLineitem);
        ArrowReader readerLineitem = scannerLineitem.scanBatches();
    ) {
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("CUSTOMER", readerPart);
      mapTableToArrowReader.put("ORDERS", readerOrders);
      mapTableToArrowReader.put("LINEITEM", readerLineitem);
      Assertions.assertThrows(RuntimeException.class, () -> {
        try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
            getSubstraitTpchPlan("03.json"),
            mapTableToArrowReader
        )) {
          while (arrowReader.loadNextBatch()) {
          }
        }
      }, "conversion to arrow::compute::Declaration from Substrait relation sort");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
