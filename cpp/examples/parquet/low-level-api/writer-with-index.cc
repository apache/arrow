// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>
#include <cstdio>
#include <reader_writer_forindex.h>
#include <iomanip>
#include <sys/time.h>

#include "parquet/column_reader.h"
#include "parquet/column_scanner.h"
#include "parquet/deprecated_io.h"
#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

/*
 * This example illustrates PARQUET-1404 for page level skipping in  
 * writing and reading Parquet Files in C++ and serves as a
 * reference to the API for reader and writer enhanced with Column Index and Offset Index
 * The file contains all the physical data types supported by Parquet.
 * This example uses the RowGroupWriter API that supports writing RowGroups based on a
 *certain size
 **/

/* Parquet is a structured columnar file format
 * Parquet File = "Parquet data" + "Parquet Metadata"
 * "Parquet data" is simply a vector of RowGroups. Each RowGroup is a batch of rows in a
 * columnar layout
 * "Parquet Metadata" contains the "file schema" and attributes of the RowGroups and their
 * Columns
 * "file schema" is a tree where each node is either a primitive type (leaf nodes) or a
 * complex (nested) type (internal nodes)
 * For specific details, please refer the format here:
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 **/


/*********************************************************************************
                   PARQUET WRITER WITH PAGE SKIPPING EXAMPLE
**********************************************************************************/

void writecolswithindexbf(int NUM_ROWS_PER_ROW_GROUP,parquet::RowGroupWriter*& rg_writer,float fpp,int32_t int32factor,int64_t int64factor, float float_factor,double double_factor,int FIXED_LENGTH){
    uint32_t num_bytes = 0;
    rg_writer->InitBloomFilter(NUM_ROWS_PER_ROW_GROUP,num_bytes,fpp);
    
    // Write the Int32 column
    parquet::Int32Writer* int32_writer =
        static_cast<parquet::Int32Writer*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int32_t value = i*int32factor;
      int32_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int32 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      
        // Write the Int64 column. Each row has not[repeats twice].
    parquet::Int64Writer* int64_writer =
        static_cast<parquet::Int64Writer*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int64_t value = i*int64factor;
      int64_writer->WriteBatch(1, nullptr,nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int64 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      

    // Write the Float column
    parquet::FloatWriter* float_writer =
        static_cast<parquet::FloatWriter*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
   
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      float value = static_cast<float>(i) * float_factor;//1.1f;
      float_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes float " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
       
         // Write the Double column
    parquet::DoubleWriter* double_writer =
        static_cast<parquet::DoubleWriter*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      double value = i * double_factor;//1.1111111;
      double_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes double " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
       
        // Write the ByteArray column. Make every alternate values NULL
    parquet::ByteArrayWriter* ba_writer =
        static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::ByteArray value;
      char hello[FIXED_LENGTH];// = "parquet";
      int64_t startnumber = i;
      for ( int ci = 0; ci < FIXED_LENGTH; ci++ ) {
          hello[FIXED_LENGTH-ci-1] = (startnumber%10) + 48;
          startnumber /= 10;
      }
      hello[FIXED_LENGTH] = '\0';
      std::string test(hello);
      // if (i % 2 == 0) {
      int16_t definition_level = 1;
      value.ptr = reinterpret_cast<const uint8_t*>(test.c_str());
      value.len = test.size();
      ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(&value);
      // } else {
      //   int16_t definition_level = 1;
      //   value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
      //   value.len = FIXED_LENGTH;
      //   ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      //   rg_writer->AppendRowGroupBloomFilter(&value);
      // }
    }
    std::cout << "number of bytes bytearray " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      
}

void writecolswithindexbfunsorted(int NUM_ROWS_PER_ROW_GROUP,parquet::RowGroupWriter*& rg_writer,float fpp, int32_t int32factor,int64_t int64factor, float float_factor,double double_factor,int FIXED_LENGTH){
    uint32_t num_bytes = 0;
    rg_writer->InitBloomFilter(NUM_ROWS_PER_ROW_GROUP,num_bytes,fpp);
    srand(time(NULL));
         // Write the Int32 column
    parquet::Int32Writer* int32_writer =
        static_cast<parquet::Int32Writer*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int32_t value = rand()%NUM_ROWS_PER_ROW_GROUP;
      int32_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int32 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
     
    srand(time(NULL));
    // Write the Int64 column. Each row has not[repeats twice].
    parquet::Int64Writer* int64_writer =
        static_cast<parquet::Int64Writer*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int64_t value = rand()%NUM_ROWS_PER_ROW_GROUP;
      int64_writer->WriteBatch(1, nullptr,nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int64 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
    srand(time(NULL));
         // Write the Float column
    parquet::FloatWriter* float_writer =
        static_cast<parquet::FloatWriter*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      float value = static_cast<float>(rand()%NUM_ROWS_PER_ROW_GROUP) * float_factor;//1.1f;
      float_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes float " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
     srand(time(NULL));
         // Write the Double column
    parquet::DoubleWriter* double_writer =
        static_cast<parquet::DoubleWriter*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      double value = rand()%NUM_ROWS_PER_ROW_GROUP * double_factor;//1.1111111;
      double_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes double " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
     srand(time(NULL));
         // Write the ByteArray column. Make every alternate values NULL
    parquet::ByteArrayWriter* ba_writer =
        static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumnWithIndex(num_bytes,true,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::ByteArray value;
      char hello[FIXED_LENGTH];// = "parquet";
      int64_t startnumber = i;
      for ( int ci = 0; ci < FIXED_LENGTH; ci++ ) {
          hello[FIXED_LENGTH-ci-1] = (startnumber%10) + 48;
          startnumber /= 10;
      }
      hello[FIXED_LENGTH] = '\0';
      std::string test(hello);
      // if (i % 2 == 0) {
      int16_t definition_level = 1;
      value.ptr = reinterpret_cast<const uint8_t*>(test.c_str());
      value.len = test.size();
      ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(&value);
      // } else {
      //   int16_t definition_level = 1;
      //   value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
      //   value.len = FIXED_LENGTH;
      //   ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      //   rg_writer->AppendRowGroupBloomFilter(&value);
      // }
    }
    std::cout << "number of bytes ByteArray " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
     
}

void writeparquetwithindexbf(int NUM_ROWS, int num_rg, float fpp) {
  const char* filename_1 = "parquet_cpp_example_";
  std::string s1(std::to_string(NUM_ROWS)+"_");
  const char* filename_2 = s1.c_str();
  std::string s2(std::to_string(num_rg));
  const char* filename_4 = s2.c_str();
  const char* filename_3 = "_sorted.parquet";

  char PARQUET_FILENAME[strlen(filename_1) + strlen(filename_2) + strlen(filename_4) + strlen(filename_3)];
  strcpy(PARQUET_FILENAME,filename_1);
  strcat(PARQUET_FILENAME,filename_2);
  strcat(PARQUET_FILENAME,filename_4);
  strcat(PARQUET_FILENAME,filename_3);
  /**********************************************************************************
                             PARQUET WRITER EXAMPLE
  **********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values
  try {
    // Create a local file output stream instance.
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> out_file;
    PARQUET_THROW_NOT_OK(FileClass::Open(PARQUET_FILENAME, &out_file));

    // Setup the parquet schema
    std::shared_ptr<GroupNode> schema = SetupSchema();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::UNCOMPRESSED);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, props);

    // Append a RowGroup with a specific number of rows.
    parquet::RowGroupWriter* rg_writer;
    for ( int i=0; i < num_rg; i++) {
      rg_writer = file_writer->AppendRowGroup(NUM_ROWS/num_rg);
      writecolswithindexbf(NUM_ROWS/num_rg,rg_writer,fpp,1,1,1.1f,1.1111111,124);
    }
    // Close the ParquetFileWriter
    file_writer->CloseWithIndex(true,true);

    // Write the bytes to file
    DCHECK(out_file->Close().ok());
  } catch (const std::exception& e) {
    std::cerr << "Parquet write error: " << e.what() << std::endl;
    //return -1;
  }
}

void writeparquetwithindexbfunsorted(int NUM_ROWS, int num_rg,float fpp) {
  const char* filename_1 = "parquet_cpp_example_";
  std::string s1(std::to_string(NUM_ROWS)+"_");
  const char* filename_2 = s1.c_str();
  std::string s2(std::to_string(num_rg));
  const char* filename_4 = s2.c_str();
  const char* filename_3 = "_unsorted.parquet";
  
  char PARQUET_FILENAME[strlen(filename_1) + strlen(filename_2) + strlen(filename_4) + strlen(filename_3)];
  strcpy(PARQUET_FILENAME,filename_1);
  strcat(PARQUET_FILENAME,filename_2);
  strcat(PARQUET_FILENAME,filename_4);
  strcat(PARQUET_FILENAME,filename_3);
  /**********************************************************************************
                             PARQUET WRITER EXAMPLE
  **********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values
  try {
    // Create a local file output stream instance.
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> out_file;
    PARQUET_THROW_NOT_OK(FileClass::Open(PARQUET_FILENAME, &out_file));

    // Setup the parquet schema
    std::shared_ptr<GroupNode> schema = SetupSchema();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::UNCOMPRESSED);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, props);

    // Append a RowGroup with a specific number of rows.
    parquet::RowGroupWriter* rg_writer;
    for ( int i=0; i < num_rg; i++) {
      rg_writer = file_writer->AppendRowGroup(NUM_ROWS/num_rg);
      writecolswithindexbf(NUM_ROWS/num_rg,rg_writer,fpp,1,1,1.1f,1.1111111,124);
    }

    // Close the ParquetFileWriter
    file_writer->CloseWithIndex(true,true);

    // Write the bytes to file
    DCHECK(out_file->Close().ok());
  } catch (const std::exception& e) {
    std::cerr << "Parquet write error: " << e.what() << std::endl;
    //return -1;
  }
}

void writecolswithoutindexbf(int NUM_ROWS_PER_ROW_GROUP,parquet::RowGroupWriter*& rg_writer,int32_t int32factor,int64_t int64factor, float float_factor,double double_factor,int FIXED_LENGTH){
    uint32_t num_bytes = 0;
    //rg_writer->InitBloomFilter(NUM_ROWS_PER_ROW_GROUP,num_bytes);
    
    // Write the Int32 column
    parquet::Int32Writer* int32_writer =
        static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int32_t value = i*int32factor;
      int32_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int32 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      
        // Write the Int64 column. Each row has not[repeats twice].
    parquet::Int64Writer* int64_writer =
        static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int64_t value = i*int64factor;
      int64_writer->WriteBatch(1, nullptr,nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int64 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      

    // Write the Float column
    parquet::FloatWriter* float_writer =
        static_cast<parquet::FloatWriter*>(rg_writer->NextColumn());
   
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      float value = static_cast<float>(i) * float_factor;//1.1f;
      float_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes float " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
       
         // Write the Double column
    parquet::DoubleWriter* double_writer =
        static_cast<parquet::DoubleWriter*>(rg_writer->NextColumn());
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      double value = i * double_factor;//1.1111111;
      double_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes double " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
       
        // Write the ByteArray column. Make every alternate values NULL
    parquet::ByteArrayWriter* ba_writer =
        static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::ByteArray value;
      char hello[FIXED_LENGTH];// = "parquet";
      int64_t startnumber = i;
      for ( int ci = 0; ci < FIXED_LENGTH; ci++ ) {
          hello[FIXED_LENGTH-ci-1] = (startnumber%10) + 48;
          startnumber /= 10;
      }
      hello[FIXED_LENGTH] = '\0';
      std::string test(hello);
      // if (i % 2 == 0) {
      int16_t definition_level = 1;
      value.ptr = reinterpret_cast<const uint8_t*>(test.c_str());
      value.len = test.size();
      ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(&value);
      // } else {
      //   int16_t definition_level = 1;
      //   value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
      //   value.len = FIXED_LENGTH;
      //   ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      //   rg_writer->AppendRowGroupBloomFilter(&value);
      // }
    }
    std::cout << "number of bytes bytearray " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      
}

void writeparquetwithoutindexbf(int NUM_ROWS, int num_rg, float fpp) {
  const char* filename_1 = "parquet_cpp_example_";
  std::string s1(std::to_string(NUM_ROWS)+"_");
  const char* filename_2 = s1.c_str();
  std::string s2(std::to_string(num_rg));
  const char* filename_4 = s2.c_str();
  const char* filename_3 = "_WOIBF-sorted.parquet";

  char PARQUET_FILENAME[strlen(filename_1) + strlen(filename_2) + strlen(filename_4) + strlen(filename_3)];
  strcpy(PARQUET_FILENAME,filename_1);
  strcat(PARQUET_FILENAME,filename_2);
  strcat(PARQUET_FILENAME,filename_4);
  strcat(PARQUET_FILENAME,filename_3);
  /**********************************************************************************
                             PARQUET WRITER EXAMPLE
  **********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values
  try {
    // Create a local file output stream instance.
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> out_file;
    PARQUET_THROW_NOT_OK(FileClass::Open(PARQUET_FILENAME, &out_file));

    // Setup the parquet schema
    std::shared_ptr<GroupNode> schema = SetupSchema();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::UNCOMPRESSED);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, props);

    // Append a RowGroup with a specific number of rows.
    parquet::RowGroupWriter* rg_writer;
    for ( int i=0; i < num_rg; i++) {
      rg_writer = file_writer->AppendRowGroup(NUM_ROWS/num_rg);
      writecolswithoutindexbf(NUM_ROWS/num_rg,rg_writer,1,1,1.1f,1.1111111,124);
    }
    // Close the ParquetFileWriter
    file_writer->CloseWithIndex(false,false);

    // Write the bytes to file
    DCHECK(out_file->Close().ok());
  } catch (const std::exception& e) {
    std::cerr << "Parquet write error: " << e.what() << std::endl;
    //return -1;
  }
}

void writecolsonlyindex(int NUM_ROWS_PER_ROW_GROUP,parquet::RowGroupWriter*& rg_writer,float fpp, int32_t int32factor,int64_t int64factor, float float_factor,double double_factor,int FIXED_LENGTH){
    uint32_t num_bytes = 0;
    //rg_writer->InitBloomFilter(NUM_ROWS_PER_ROW_GROUP,num_bytes);
    
    // Write the Int32 column
    parquet::Int32Writer* int32_writer =
        static_cast<parquet::Int32Writer*>(rg_writer->NextColumnWithIndex(num_bytes,true,false,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int32_t value = i*int32factor;
      int32_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int32 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      
        // Write the Int64 column. Each row has not[repeats twice].
    parquet::Int64Writer* int64_writer =
        static_cast<parquet::Int64Writer*>(rg_writer->NextColumnWithIndex(num_bytes,true,false,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int64_t value = i*int64factor;
      int64_writer->WriteBatch(1, nullptr,nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int64 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      

    // Write the Float column
    parquet::FloatWriter* float_writer =
        static_cast<parquet::FloatWriter*>(rg_writer->NextColumnWithIndex(num_bytes,true,false,fpp));
   
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      float value = static_cast<float>(i) * float_factor;//1.1f;
      float_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes float " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
       
         // Write the Double column
    parquet::DoubleWriter* double_writer =
        static_cast<parquet::DoubleWriter*>(rg_writer->NextColumnWithIndex(num_bytes,true,false,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      double value = i * double_factor;//1.1111111;
      double_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes double " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
       
        // Write the ByteArray column. Make every alternate values NULL
    parquet::ByteArrayWriter* ba_writer =
        static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumnWithIndex(num_bytes,true,false,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::ByteArray value;
      char hello[FIXED_LENGTH];// = "parquet";
      int64_t startnumber = i;
      for ( int ci = 0; ci < FIXED_LENGTH; ci++ ) {
          hello[FIXED_LENGTH-ci-1] = (startnumber%10) + 48;
          startnumber /= 10;
      }
      hello[FIXED_LENGTH] = '\0';
      std::string test(hello);
      // if (i % 2 == 0) {
      int16_t definition_level = 1;
      value.ptr = reinterpret_cast<const uint8_t*>(test.c_str());
      value.len = test.size();
      ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      //rg_writer->AppendRowGroupBloomFilter(&value);
      // } else {
      //   int16_t definition_level = 1;
      //   value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
      //   value.len = FIXED_LENGTH;
      //   ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      //   rg_writer->AppendRowGroupBloomFilter(&value);
      // }
    }
    std::cout << "number of bytes bytearray " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      
}

void writeparquetonlyindex(int NUM_ROWS, int num_rg, float fpp) {
  const char* filename_1 = "parquet_cpp_example_";
  std::string s1(std::to_string(NUM_ROWS)+"_");
  const char* filename_2 = s1.c_str();
  std::string s2(std::to_string(num_rg));
  const char* filename_4 = s2.c_str();
  const char* filename_3 = "_only-index-sorted.parquet";

  char PARQUET_FILENAME[strlen(filename_1) + strlen(filename_2) + strlen(filename_4) + strlen(filename_3)];
  strcpy(PARQUET_FILENAME,filename_1);
  strcat(PARQUET_FILENAME,filename_2);
  strcat(PARQUET_FILENAME,filename_4);
  strcat(PARQUET_FILENAME,filename_3);
  /**********************************************************************************
                             PARQUET WRITER EXAMPLE
  **********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values
  try {
    // Create a local file output stream instance.
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> out_file;
    PARQUET_THROW_NOT_OK(FileClass::Open(PARQUET_FILENAME, &out_file));

    // Setup the parquet schema
    std::shared_ptr<GroupNode> schema = SetupSchema();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::UNCOMPRESSED);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, props);

    // Append a RowGroup with a specific number of rows.
    parquet::RowGroupWriter* rg_writer;
    for ( int i=0; i < num_rg; i++) {
      rg_writer = file_writer->AppendRowGroup(NUM_ROWS/num_rg);
      writecolsonlyindex(NUM_ROWS/num_rg,rg_writer,fpp,1,1,1.1f,1.1111111,124);
    }
    // Close the ParquetFileWriter
    file_writer->CloseWithIndex(true,false);

    // Write the bytes to file
    DCHECK(out_file->Close().ok());
  } catch (const std::exception& e) {
    std::cerr << "Parquet write error: " << e.what() << std::endl;
    //return -1;
  }
}

void writecolsonlybf(int NUM_ROWS_PER_ROW_GROUP,parquet::RowGroupWriter*& rg_writer,float fpp,int32_t int32factor,int64_t int64factor, float float_factor,double double_factor,int FIXED_LENGTH){
    uint32_t num_bytes = 0;
    rg_writer->InitBloomFilter(NUM_ROWS_PER_ROW_GROUP,num_bytes,fpp);
    
    // Write the Int32 column
    parquet::Int32Writer* int32_writer =
        static_cast<parquet::Int32Writer*>(rg_writer->NextColumnWithIndex(num_bytes,false,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int32_t value = i*int32factor;
      int32_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int32 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      
        // Write the Int64 column. Each row has not[repeats twice].
    parquet::Int64Writer* int64_writer =
        static_cast<parquet::Int64Writer*>(rg_writer->NextColumnWithIndex(num_bytes,false,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int64_t value = i*int64factor;
      int64_writer->WriteBatch(1, nullptr,nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes int64 " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      

    // Write the Float column
    parquet::FloatWriter* float_writer =
        static_cast<parquet::FloatWriter*>(rg_writer->NextColumnWithIndex(num_bytes,false,true,fpp));
   
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      float value = static_cast<float>(i) * float_factor;//1.1f;
      float_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes float " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
       
         // Write the Double column
    parquet::DoubleWriter* double_writer =
        static_cast<parquet::DoubleWriter*>(rg_writer->NextColumnWithIndex(num_bytes,false,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      double value = i * double_factor;//1.1111111;
      double_writer->WriteBatch(1, nullptr, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(value);
    }
    std::cout << "number of bytes double " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
       
        // Write the ByteArray column. Make every alternate values NULL
    parquet::ByteArrayWriter* ba_writer =
        static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumnWithIndex(num_bytes,false,true,fpp));
    
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::ByteArray value;
      char hello[FIXED_LENGTH];// = "parquet";
      int64_t startnumber = i;
      for ( int ci = 0; ci < FIXED_LENGTH; ci++ ) {
          hello[FIXED_LENGTH-ci-1] = (startnumber%10) + 48;
          startnumber /= 10;
      }
      hello[FIXED_LENGTH] = '\0';
      std::string test(hello);
      // if (i % 2 == 0) {
      int16_t definition_level = 1;
      value.ptr = reinterpret_cast<const uint8_t*>(test.c_str());
      value.len = test.size();
      ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      rg_writer->AppendRowGroupBloomFilter(&value);
      // } else {
      //   int16_t definition_level = 1;
      //   value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
      //   value.len = FIXED_LENGTH;
      //   ba_writer->WriteBatch(1, &definition_level, nullptr, &value, true);
      //   rg_writer->AppendRowGroupBloomFilter(&value);
      // }
    }
    std::cout << "number of bytes bytearray " << num_bytes/NUM_ROWS_PER_ROW_GROUP << std::endl;
      
}

void writeparquetonlybf(int NUM_ROWS, int num_rg, float fpp) {
  const char* filename_1 = "parquet_cpp_example_";
  std::string s1(std::to_string(NUM_ROWS)+"_");
  const char* filename_2 = s1.c_str();
  std::string s2(std::to_string(num_rg));
  const char* filename_4 = s2.c_str();
  const char* filename_3 = "_only-bf-sorted.parquet";

  char PARQUET_FILENAME[strlen(filename_1) + strlen(filename_2) + strlen(filename_4) + strlen(filename_3)];
  strcpy(PARQUET_FILENAME,filename_1);
  strcat(PARQUET_FILENAME,filename_2);
  strcat(PARQUET_FILENAME,filename_4);
  strcat(PARQUET_FILENAME,filename_3);
  /**********************************************************************************
                             PARQUET WRITER EXAMPLE
  **********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values
  try {
    // Create a local file output stream instance.
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> out_file;
    PARQUET_THROW_NOT_OK(FileClass::Open(PARQUET_FILENAME, &out_file));

    // Setup the parquet schema
    std::shared_ptr<GroupNode> schema = SetupSchema();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::UNCOMPRESSED);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, props);

    // Append a RowGroup with a specific number of rows.
    parquet::RowGroupWriter* rg_writer;
    for ( int i=0; i < num_rg; i++) {
      rg_writer = file_writer->AppendRowGroup(NUM_ROWS/num_rg);
      writecolsonlybf(NUM_ROWS/num_rg,rg_writer,fpp,1,1,1.1f,1.1111111,124);
    }
    // Close the ParquetFileWriter
    file_writer->CloseWithIndex(false,true);

    // Write the bytes to file
    DCHECK(out_file->Close().ok());
  } catch (const std::exception& e) {
    std::cerr << "Parquet write error: " << e.what() << std::endl;
    //return -1;
  }
}

int main(int argc, char** argv) {
  if (argc == 4){
    int NUM_ROWS = atoi(argv[1]);
    int num_rg = atoi(argv[2]);
    float fpp = atof(argv[3]);
    //writeparquetwithoutindexbf(NUM_ROWS,num_rg,fpp);
    //writeparquetonlyindex(NUM_ROWS,num_rg,fpp);
    //writeparquetonlybf(NUM_ROWS,num_rg,fpp);
    writeparquetwithindexbfunsorted(NUM_ROWS,num_rg,fpp);
  }
  
  std::cout << "Parquet Writing and Reading Complete" << std::endl;

  return 0;
}
