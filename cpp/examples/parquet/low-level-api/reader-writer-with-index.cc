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

constexpr int NUM_ROWS = 20;//2500000;
constexpr int64_t ROW_GROUP_SIZE = (sizeof(uint32_t)+sizeof(int32_t)+sizeof(int64_t)+sizeof(float)+sizeof(double)

                                    +sizeof(parquet::ByteArray)+sizeof(parquet::FixedLenByteArray))*NUM_ROWS;//16 * 1024 * 1024;  // 16 MB
//char PARQUET_FILENAME[] = "";
//const char PARQUET_FILENAME[] = "/home/abalajiee/parquet_data/testing_write.parquet";

struct return_multiple{
   std::shared_ptr<parquet::ColumnReader> column_reader;
   bool b;
   int32_t p;
   int64_t r;
   uint32_t e;
   double d;
   float i;
   char *c,*a,*t;
};

typedef return_multiple return_multiple;

int parquet_writer(int argc, char** argv);

void returnReaderwithType(std::shared_ptr<parquet::ColumnReader> cr, parquet::ColumnReader*& cr1);

return_multiple getPredicate(std::shared_ptr<parquet::ColumnReader> cr,std::shared_ptr<parquet::RowGroupReader> rg,char* predicate,
                             int& col_id,int64_t& page_index,int& PREDICATE_COL,int64_t& row_index,bool with_index);

bool printVal(std::shared_ptr<parquet::ColumnReader>column_reader, parquet::ColumnReader* int64_reader,int ind,return_multiple vals,int64_t& row_counter,
               bool checkpredicate);
bool printRange(std::shared_ptr<parquet::ColumnReader>column_reader, parquet::ColumnReader* int64_reader,int ind,return_multiple vals_min,return_multiple vals_max,int64_t& row_counter);

void run_for_one_predicate(int num_columns,int num_row_groups, std::unique_ptr<parquet::ParquetFileReader>& parquet_reader, char** argv,int predicate_index);

void first_pass_for_predicate_only(std::shared_ptr<parquet::RowGroupReader> rg,int predicate_column_number,int num_columns, char* predicate,bool with_index);

int parquet_reader(int argc, char** argv);
/**************Declaration END*********************************/


int main(int argc, char** argv) {
  if (false)
     parquet_writer(argc, argv);
  
  parquet_reader(argc,argv);

  std::cout << "Parquet Writing and Reading Complete" << std::endl;

  return 0;
}

/*********************************************************************************
                   PARQUET READER WITH PAGE SKIPPING EXAMPLE
**********************************************************************************/
int parquet_reader(int argc,char** argv) {

   std::string PARQUET_FILENAME = argv[1];
   try {
     // Create a ParquetReader instance
     std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
       parquet::ParquetFileReader::OpenFile(PARQUET_FILENAME, false);

     // Get the File MetaData
     std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

     int num_row_groups = file_metadata->num_row_groups();

     // Get the number of Columns
     int num_columns = file_metadata->num_columns();
     //      assert(num_columns == NUM_COLS);

     run_for_one_predicate(num_columns,num_row_groups,parquet_reader,argv,3);

     if ( argc == 5 ){
       run_for_one_predicate(num_columns,num_row_groups,parquet_reader,argv,4);
     }
     
     return 0;
   } catch (const std::exception& e) {
      std::cerr << "Parquet read error: " << e.what() << std::endl;
      return -1;
  }

}

void run_for_one_predicate(int num_columns,int num_row_groups, std::unique_ptr<parquet::ParquetFileReader>& parquet_reader, char** argv,int predicate_index) {
  // Iterate over all the RowGroups in the file
    for (int r = 0; r < num_row_groups; ++r) {
    
      char *col_num = argv[2];
      char *predicate_val  = argv[predicate_index];

      std::stringstream ss(col_num);
      int col_id = 0;
        ss >> col_id;
        // Get the RowGroup Reader
       std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);

        struct timeval start_time,end_time;
        float total_time= 0.0;
        int num_runs = 5;
         
        
        /********FIRST PASS WITHOUT INDEX***************/
        total_time = 0.0;
        for(int t  =0 ; t< num_runs; t++){
            gettimeofday(&start_time,NULL);
          first_pass_for_predicate_only(row_group_reader,col_id,num_columns,predicate_val,false);
          gettimeofday(&end_time,NULL);
          
            float time_elapsed = ((float)(end_time.tv_sec-start_time.tv_sec) + abs((float)(end_time.tv_usec - start_time.tv_usec))/1000000.0);

            std::cout << std::setprecision(3) << "\n time for predicate one pass without index: " << time_elapsed << std::endl;

            total_time += time_elapsed;
        }
        std::cout << std::setprecision(3)  << "total time " << total_time << std::endl;
        float avg_time = (float)total_time/(float)num_runs;
        std::cout << std::setprecision(3) <<  "\n avg time for predicate one pass without index: " <<  avg_time << " sec for " << num_runs << " runs" << std::endl;

       /**************FIRST PASS WITH INDEX*****************/
       total_time = 0.0;
        for(int t  =0 ; t< num_runs; t++){
            gettimeofday(&start_time,NULL);
          first_pass_for_predicate_only(row_group_reader,col_id,num_columns,predicate_val,true);
          gettimeofday(&end_time,NULL);
          
            float time_elapsed = ((float)(end_time.tv_sec-start_time.tv_sec) + abs((float)(end_time.tv_usec - start_time.tv_usec))/1000000.0);

            std::cout << std::setprecision(3) << "\n time for predicate one pass: " << time_elapsed << std::endl;

            total_time += time_elapsed;
        }
        std::cout << std::setprecision(3)  << "total time " << total_time << std::endl;
        avg_time = (float)total_time/(float)num_runs;
        std::cout << std::setprecision(3) <<  "\n avg time for predicate one pass: " <<  avg_time << " sec for " << num_runs << " runs" << std::endl;

      /***********FIRST PASS END **********/

      /***********Second PASS *************/
                //  TODO //

      /***********************************/
      
     }
}


void first_pass_for_predicate_only(std::shared_ptr<parquet::RowGroupReader> row_group_reader,int col_id, int num_columns, char* predicate_val,bool with_index) {

    int64_t row_index = 0;

    std::vector<int> col_row_counts(num_columns, 0);

    //      assert(row_group_reader->metadata()->total_byte_size() < ROW_GROUP_SIZE);

    // int16_t definition_level;
    // int16_t repetition_level;
    std::shared_ptr<parquet::ColumnReader> column_reader;
    

    // std::cout<< "test arg v" <<argv[1] << std::endl;
  
    int64_t page_index = -1;

    char c;
    // int64_t predicate;
    // sscanf(argv[2], "%" SCNd64 "%c", &predicate, &c);

    // int PREDICATE_COL;
    // sscanf(argv[2], "%d" "%c", &PREDICATE_COL, &c);
    // Get the Column Reader for the Int64 column
      std::shared_ptr<parquet::ColumnReader> predicate_column_reader = row_group_reader->Column(col_id);
      
      
      std::cout << "Column Type: " << predicate_column_reader->type() << std::endl;
      
      // std::cout << "given predicate: " << predicate << " type of predicate: " << typeid(predicate).name() << std::endl;
      
      std::shared_ptr<parquet::ColumnReader> column_reader_with_index;
      
      parquet::ColumnReader* generic_reader;
  
      int PREDICATE_COL  = col_id;
      return_multiple vals = getPredicate(predicate_column_reader,row_group_reader,predicate_val,col_id,page_index,PREDICATE_COL,row_index,with_index);
      column_reader_with_index = vals.column_reader;
      
      //SAMPLE row group reader call in the comment below
      // row_group_reader->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,predicate_column_reader->type());

      returnReaderwithType(column_reader_with_index,generic_reader);
      // Read all the rows in the column
      std::cout << "column id:" << col_id << " page index:" << page_index << std::endl;

      int counter = 0;
      int ind = 0;
      int64_t row_counter = -1;

      if(with_index){
        ind = row_index;
        row_counter = -1;
        generic_reader->Skip(row_index);
        do{ ind++;
         if((printVal(column_reader_with_index,generic_reader,ind,vals,row_counter,true)))
             break;
        }while((generic_reader->HasNext()));
      }
      else{
        while (generic_reader->HasNext()) { 
            ind++;

          if(printVal(column_reader_with_index,generic_reader,ind,vals,row_counter,true))
             break;
          //        int64_t expected_value = col_row_counts[col_id];  
          //        assert(value == expected_value);
         col_row_counts[col_id]++;
        } 
      }
        
      
}

return_multiple getPredicate(std::shared_ptr<parquet::ColumnReader> cr,std::shared_ptr<parquet::RowGroupReader> rg,char* predicate_val,
                             int& col_id,int64_t& page_index,int& PREDICATE_COL,int64_t& row_index, bool with_index){
    const int CHAR_LEN = 10000000;
    
    return_multiple vals;
    std::stringstream ss(predicate_val);
    switch(cr->type()){
          case Type::BOOLEAN:{
            bool b;
            
            ss >> std::boolalpha >> b;
            void * predicate = static_cast<void*>(&b);

            vals.column_reader = (with_index)?
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type()):
                      rg->Column(col_id);
            vals.b = b;
            return vals;
          }
          case Type::INT32:{
            int32_t val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = (with_index)?
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type()):
                      rg->Column(col_id);
            vals.p = val;
            return vals;
          }
          case Type::INT64:{
            int64_t val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = (with_index)?
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type()):
                      rg->Column(col_id);
            vals.r = val;
            return vals;
          }
          case Type::INT96:{
            uint32_t val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = (with_index)?
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type()):
                      rg->Column(col_id);
            vals.e = val;
            return vals;
          }
          case Type::FLOAT:{
            float val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = (with_index)?
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type()):
                      rg->Column(col_id);
            vals.d = val;
            return vals;
          }
          case Type::DOUBLE:{
            double val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = (with_index)?
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type()):
                      rg->Column(col_id);
            vals.i = val;
            return vals;
          }
          case Type::BYTE_ARRAY:{
            char* val = predicate_val;
            
            void * predicate = static_cast<void*>(val);
            vals.column_reader = (with_index)?
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type()):
                      rg->Column(col_id);
            vals.c = val;
            return vals;
          }
          case Type::FIXED_LEN_BYTE_ARRAY:{
            char* val = predicate_val;
            
            void * predicate = static_cast<void*>(val);
            vals.column_reader = (with_index)?
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type()):
                      rg->Column(col_id);
            vals.a = val;
            return vals;
          }
          default:{
            std::cout<< "type not supported" << std::endl;
            vals.a = NULL;
            vals.b = NULL;
            vals.c = NULL;
            vals.t = NULL;
            return vals;
          }
    }
}

int parquet_writer(int argc, char** argv) {

  /**********************************************************************************
                             PARQUET WRITER EXAMPLE
  **********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values

  //argv[2] and argv[3] already taken for reader with index for predicate column number and predicate search value
  char* PARQUET_FILENAME = argv[1];
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

    // Append a BufferedRowGroup to keep the RowGroup open until a certain size
    parquet::RowGroupWriter* rg_writer = file_writer->AppendBufferedRowGroup();

    int num_columns = file_writer->num_columns();
    std::vector<int64_t> buffered_values_estimate(num_columns, 0);
    for (int i = 0; i < NUM_ROWS; i++) {
      int64_t estimated_bytes = 0;
      // Get the estimated size of the values that are not written to a page yet
      for (int n = 0; n < num_columns; n++) {
        estimated_bytes += buffered_values_estimate[n];
      }

      // We need to consider the compressed pages
      // as well as the values that are not compressed yet
      if ((rg_writer->total_bytes_written() + rg_writer->total_compressed_bytes() +
           estimated_bytes) > ROW_GROUP_SIZE) {
        rg_writer->Close();
        std::fill(buffered_values_estimate.begin(), buffered_values_estimate.end(), 0);
        rg_writer = file_writer->AppendBufferedRowGroup();
      }

      int col_id = 0;
      // Write the Bool column
      parquet::BoolWriter* bool_writer =
          static_cast<parquet::BoolWriter*>(rg_writer->column(col_id));
      bool bool_value = ((i % 2) == 0) ? true : false;
      bool_writer->WriteBatch(1, nullptr, nullptr, &bool_value);
      buffered_values_estimate[col_id] = bool_writer->EstimatedBufferedValueBytes();

      // Write the Int32 column
      col_id++;
      parquet::Int32Writer* int32_writer =
          static_cast<parquet::Int32Writer*>(rg_writer->column(col_id));
      int32_t int32_value = i;
      int32_writer->WriteBatchWithIndex(1, nullptr, nullptr, &int32_value);
      buffered_values_estimate[col_id] = int32_writer->EstimatedBufferedValueBytes();

      // Write the Int64 column. Each row has repeats twice.
      col_id++;
      parquet::Int64Writer* int64_writer =
          static_cast<parquet::Int64Writer*>(rg_writer->column(col_id));
      int64_t int64_value1 = 2 * i;
      int16_t definition_level = 1;
      int16_t repetition_level = 0;
      int64_writer->WriteBatchWithIndex(1, &definition_level, &repetition_level, &int64_value1);
      int64_t int64_value2 = (2 * i + 1);
      repetition_level = 1;  // start of a new record
      int64_writer->WriteBatchWithIndex(1, &definition_level, &repetition_level, &int64_value2);
      buffered_values_estimate[col_id] = int64_writer->EstimatedBufferedValueBytes();

      // Write the INT96 column.
      col_id++;
      parquet::Int96Writer* int96_writer =
          static_cast<parquet::Int96Writer*>(rg_writer->column(col_id));
      parquet::Int96 int96_value;
      int96_value.value[0] = i;
      int96_value.value[1] = i + 1;
      int96_value.value[2] = i + 2;
      int96_writer->WriteBatch(1, nullptr, nullptr, &int96_value);
      buffered_values_estimate[col_id] = int96_writer->EstimatedBufferedValueBytes();

      // Write the Float column
      col_id++;
      parquet::FloatWriter* float_writer =
          static_cast<parquet::FloatWriter*>(rg_writer->column(col_id));
      float float_value = static_cast<float>(i) * 1.1f;
      float_writer->WriteBatchWithIndex(1, nullptr, nullptr, &float_value);
      buffered_values_estimate[col_id] = float_writer->EstimatedBufferedValueBytes();

      // Write the Double column
      col_id++;
      parquet::DoubleWriter* double_writer =
          static_cast<parquet::DoubleWriter*>(rg_writer->column(col_id));
      double double_value = i * 1.1111111;
      double_writer->WriteBatchWithIndex(1, nullptr, nullptr, &double_value);
      buffered_values_estimate[col_id] = double_writer->EstimatedBufferedValueBytes();

      // Write the ByteArray column. Make every alternate values NULL
      col_id++;
      parquet::ByteArrayWriter* ba_writer =
          static_cast<parquet::ByteArrayWriter*>(rg_writer->column(col_id));
      parquet::ByteArray ba_value;
      char hello[FIXED_LENGTH] = "parquet";
      hello[7] = static_cast<char>(static_cast<int>('0') + i / 100);
      hello[8] = static_cast<char>(static_cast<int>('0') + (i / 10) % 10);
      hello[9] = static_cast<char>(static_cast<int>('0') + i % 10);
      if (i % 2 == 0) {
        int16_t definition_level = 1;
        ba_value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
        ba_value.len = FIXED_LENGTH;
        ba_writer->WriteBatchWithIndex(1, &definition_level, nullptr, &ba_value);
      } else {
        int16_t definition_level = 0;
        ba_writer->WriteBatchWithIndex(1, &definition_level, nullptr, nullptr);
      }
      buffered_values_estimate[col_id] = ba_writer->EstimatedBufferedValueBytes();

      // Write the FixedLengthByteArray column
      col_id++;
      parquet::FixedLenByteArrayWriter* flba_writer =
          static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->column(col_id));
      parquet::FixedLenByteArray flba_value;
      char v = static_cast<char>(i);
      char flba[FIXED_LENGTH] = {v, v, v, v, v, v, v, v, v, v};
      flba_value.ptr = reinterpret_cast<const uint8_t*>(&flba[0]);

      flba_writer->WriteBatchWithIndex(1, nullptr, nullptr, &flba_value);
      buffered_values_estimate[col_id] = flba_writer->EstimatedBufferedValueBytes();
    }

    // Close the RowGroupWriter
    rg_writer->Close();
    // Close the ParquetFileWriter
    file_writer->Close();

    // Write the bytes to file
    DCHECK(out_file->Close().ok());
  } catch (const std::exception& e) {
    std::cerr << "Parquet write error: " << e.what() << std::endl;
    return -1;
  }

return 0;

}

void returnReaderwithType(std::shared_ptr<parquet::ColumnReader>column_reader, parquet::ColumnReader*& int64_reader){
      switch (column_reader->type()) {
       case Type::BOOLEAN:
           int64_reader = static_cast<parquet::BoolReader*>(column_reader.get());
           break;
        case Type::INT32:
          int64_reader = static_cast<parquet::Int32Reader*>(column_reader.get());
          break;
        case Type::INT64:
          int64_reader = static_cast<parquet::Int64Reader*>(column_reader.get());
          break;
        case Type::INT96:
           int64_reader = static_cast<parquet::Int96Reader*>(column_reader.get());
           break;
        case Type::FLOAT:
           int64_reader = static_cast<parquet::FloatReader*>(column_reader.get());
           break;
        case Type::DOUBLE:
           int64_reader = static_cast<parquet::DoubleReader*>(column_reader.get());
           break;
        case Type::BYTE_ARRAY:
            int64_reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());
            break;
        case Type::FIXED_LEN_BYTE_ARRAY:
            int64_reader = static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());
            break;
        default:
           parquet::ParquetException::NYI("type reader not implemented");
      }
}

bool printVal(std::shared_ptr<parquet::ColumnReader>column_reader, parquet::ColumnReader* int64_reader,int ind,return_multiple vals,int64_t& row_counter,
              bool checkpredicate = false) {

      int64_t values_read = 0;
      //int64_t 0;
       switch (column_reader->type()) {
       case Type::BOOLEAN:
          {
           bool test;
           bool predicate = vals.b;
           int64_reader->callReadBatch(1,&test,&values_read);
           row_counter = ind;
           
           if ( checkpredicate && test == predicate) {
           row_counter = ind;
           std::cout << "with predicate row number: " << row_counter << " " << test << "\n" ;
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }else{
            row_counter = ind;
           //std::cout << "row number: " << row_counter << " " << test << "\n";
           return false;
          }
           break;
          }
        case Type::INT32:
          {
            int32_t val;
            int32_t predicate = vals.p;
            int64_reader->callReadBatch(1,&val,&values_read);
           row_counter = ind;
           
           if ( checkpredicate && val == predicate) {
           row_counter = ind;
           std::cout << "with predicate row number: " << row_counter << " " << val << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }else{
            row_counter = ind;
           //std::cout << "row number: " << row_counter << " " << val << "\n";
           return false;
          }
          break;
          }
        case Type::INT64:
         {
          int64_t value;
          int64_t predicate = vals.r;
         // Read one value at a time. The number of rows read is returned. values_read
         // contains the number of non-null rows
          int64_reader->callReadBatch(1,&value,&values_read);

        // Ensure only one value is read
          //assert(rows_read == 1);
        // There are no NULL values in the rows written
       //        assert(values_read == 1);
        // Verify the value written
          if ( checkpredicate && value == predicate) {
           row_counter = ind;
           std::cout << "with predicate row number: " << row_counter << " " << value << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }else{
            row_counter = ind;
           //std::cout << "row number: " << row_counter << " " << value << "\n";
           return false;
          }
          break;
         }
        case Type::INT96:
           {
              uint32_t val;
              uint32_t predicate = vals.e;
           int64_reader->callReadBatch(1,&val,&values_read);
           row_counter = ind;
           
           if ( checkpredicate && val == predicate) {
           row_counter = ind;
           std::cout << "with predicate row number: " << row_counter << " " << val << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }else{
            row_counter = ind;
           //std::cout << "row number: " << row_counter << " " << val << "\n";
           return false;
          }
           break;
           }
        case Type::FLOAT:
           {
              float val;
              float predicate = vals.d;
              float error_factor = 100000000000.0;
           int64_reader->callReadBatch(1,&val,&values_read);
           if ( checkpredicate && fabs(val-predicate)<=std::numeric_limits<double>::epsilon()*error_factor) {
           row_counter = ind;
           std::cout << "with predicate row number: " << row_counter << " " << val << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }else{
            row_counter = ind;
           //std::cout << "row number: " << row_counter << " " << val << "\n";
           return false;
          }
           break;
           }
        case Type::DOUBLE:
           {
              double val;
              double predicate = vals.i;
           int64_reader->callReadBatch(1,&val,&values_read);
           double error_factor = 1000000000000.0;

           if ( checkpredicate && fabs(val-predicate)<=std::numeric_limits<double>::epsilon()*error_factor) {
           row_counter = ind;
           std::cout << "with predicate row number: " << row_counter << " " << val << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }else{
            row_counter = ind;
           //std::cout << "row number: " << row_counter << " " << val << "\n";
           return false;
          }
           break;
           }
        case Type::BYTE_ARRAY:
          {
            parquet::ByteArray str;
            char* predicate = vals.c;
            int64_reader->callReadBatch(1,&str,&values_read);
            std::string result = parquet::ByteArrayToString(str);

            row_counter = ind;
            // std::cout << "row number: " << row_counter << " " << result << "\n";
            if ( checkpredicate && strcmp(result.c_str(),predicate) == 0) {
           row_counter = ind;
           std::cout << "with predicate row number: " << row_counter << " " << result << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }else{
            row_counter = ind;
           //std::cout << "row number: " << row_counter << " " << result << "\n";
           return false;
          }
            break;
          }
        case Type::FIXED_LEN_BYTE_ARRAY:
          {
            parquet::FLBA str;
            char* predicate = vals.a;
            int64_reader->callReadBatch(1,&str,&values_read);
            std::string result = parquet::FixedLenByteArrayToString(str,sizeof(str));

            row_counter = ind;
            // std::cout << "row number: " << row_counter << " " << result << "\n";
            if ( checkpredicate && strcmp(result.c_str(),predicate)) {
           row_counter = ind;
           std::cout << "with predicate row number: " << row_counter << " " << result << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }else{
            row_counter = ind;
           //std::cout << "row number: " << row_counter << " " << result << "\n";
           return false;
          }
          break;
          }
        default:{
           parquet::ParquetException::NYI("type reader not implemented");
           return false;
        }
      }
      return false;
        
}

