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
#include <iomanip>
#include <sys/time.h>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <time.h>
#include "parquet/api/reader.h"
#include "parquet/column_reader.h"
#include "parquet/column_scanner.h"
#include "parquet/deprecated_io.h"
#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/parquet_types.h"
#include "parquet/file_reader.h"
#include <chrono>

using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

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
                   PARQUET READER WITH PAGE SKIPPING EXAMPLE
**********************************************************************************/

constexpr int NUM_ROWS = 20;//2500000;
constexpr int64_t ROW_GROUP_SIZE = (sizeof(uint32_t)+sizeof(int32_t)+sizeof(int64_t)+sizeof(float)+sizeof(double)

                                    +sizeof(parquet::ByteArray)+sizeof(parquet::FixedLenByteArray))*NUM_ROWS;//16 * 1024 * 1024;  // 16 MB
//char PARQUET_FILENAME[] = "";
//const char PARQUET_FILENAME[] = "/home/abalajiee/parquet_data/testing_write.parquet";

int parseLine(char* line){
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char* p = line;
    while (*p <'0' || *p > '9') p++;
    line[i-3] = '\0';
    i = atoi(p);
    return i;
}

int getMemValue(){ //Note: this value is in KB!
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1; 
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmRSS:", 6) == 0){ 
            result = parseLine(line);
            break;
        }
    }   
    fclose(file);
    return result;
}

int getReadBytesValue(){ //Note: this value is in KB!
    FILE* file = fopen("/proc/self/io", "r");
    int result = 0; 
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "read_bytes:", 11) == 0){ 
            result = parseLine(line);
            break;
        }
    }   
    fclose(file);
    return result;
}

int getReadBytesCacheValue(){ //Note: this value is in KB!
    FILE* file = fopen("/proc/self/io", "r");
    int result = 0; 
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "rchar:", 6) == 0){ 
            result = parseLine(line);
            break;
        }
    }   
    fclose(file);
    return result;
}

int getWriteBytesValue(){ //Note: this value is in KB!
    FILE* file = fopen("/proc/self/io", "r");
    int result = 0; 
    char line[128];
    int trunc = 0;
    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "write_bytes:", 12) == 0){ 
            result = parseLine(line);
            break;
        }
        if (strncmp(line, "cancelled_write_bytes:", 22) == 0){ 
            trunc = parseLine(line);
            break;
        }
    }   
    fclose(file);
    return result-trunc;
}

int getWriteBytesCacheValue(){ //Note: this value is in KB!
    FILE* file = fopen("/proc/self/io", "r");
    int result = 0; 
    char line[128];
    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "wchar:", 6) == 0){ 
            result = parseLine(line);
            break;
        }
    }   
    fclose(file);
    return result;
}

/*
rchar: 91439151
wchar: 986032
syscr: 54376
syscw: 45314
read_bytes: 17989632
write_bytes: 626688
cancelled_write_bytes: 233472
*/

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

typedef struct time_to_run{
       float wo_index = 0.0;                      //without index
       float wo_total_pages_scanned = 0.0;      
       float wo_totaltime = 0.0;
       float wo_mem_used = 0.0;
       float wo_read_bytes = 0.0;
       float wo_write_bytes = 0.0;
       float w_totaltime = 0.0;                   //with index without binary without blf
       float w_index = 0.0;
       float w_total_pages_scanned = 0.0;
       float w_mem_used = 0.0;
       float w_read_bytes = 0.0;
       float w_write_bytes = 0.0;
       float b_totaltime = 0.0;                  //with binary search  without blf
       float b_index = 0.0;
       float b_total_pages_scanned = 0.0;
       float b_mem_used = 0.0;
       float b_read_bytes = 0.0;
       float b_write_bytes = 0.0;
       float w_blf_totaltime = 0.0;              // with blf without pageblf
       float w_blf_index = 0.0;
       float w_blf_total_pages_scanned = 0.0;
       float w_blf_mem_used = 0.0;
       float w_blf_read_bytes = 0.0;
       float w_blf_write_bytes = 0.0;
       float w_pageblf_totaltime = 0.0;              // with blf with pageblf
       float w_pageblf_index = 0.0;
       float w_pageblf_total_pages_scanned = 0.0;
       float w_pageblf_mem_used = 0.0;
       float w_pageblf_read_bytes = 0.0;
       float w_pageblf_write_bytes = 0.0;
       float blf_load_time = 0.0;
       float index_load_time = 0.0;
  } trun;

int parquet_writer(int argc, char** argv);

void returnReaderwithType(std::shared_ptr<parquet::ColumnReader> cr, parquet::ColumnReader*& cr1);

return_multiple getPredicate(std::shared_ptr<parquet::ColumnReader> cr,std::shared_ptr<parquet::RowGroupReader> rg,char* predicate,
                             int& col_id,int64_t& page_index,int& PREDICATE_COL,int64_t& row_index,bool with_index, 
                             bool binary_search, int64_t& count_pages_scanned, int64_t& total_num_pages, 
                             int64_t& last_first_row, bool with_bloom_filter, bool with_page_bf,
                             std::vector<int64_t>& unsorted_min_index, std::vector<int64_t>& unsorted_row_index);

bool printVal(std::ofstream& runfile, std::shared_ptr<parquet::ColumnReader>column_reader, parquet::ColumnReader* int64_reader,int ind,return_multiple vals,int64_t& row_counter,
               bool checkpredicate,int equal_to);
bool printRange(std::shared_ptr<parquet::ColumnReader>column_reader, parquet::ColumnReader* int64_reader,int ind,return_multiple vals_min,return_multiple vals_max,int64_t& row_counter);

trun run_for_one_predicate(std::ofstream& runfile, int num_columns,std::shared_ptr<parquet::RowGroupReader>& row_group_reader, std::unique_ptr<parquet::ParquetFileReader>& parquet_reader, int col_id,char** argv,
                           int predicate_index, int equal_to, bool binary_search, bool with_bloom_filter, bool with_page_bf);

int64_t first_pass_for_predicate_only(std::ofstream& runfile,std::shared_ptr<parquet::RowGroupReader> rg,int predicate_column_number,int num_columns, char* predicate,
                                      bool with_index, int equal_to, bool binary_search, bool with_bloom_filter, bool with_page_bf);

int parquet_reader(int argc, char** argv);
/**************Declaration END*********************************/


int main(int argc, char** argv) {

  parquet_reader(argc,argv);

  std::cout << "Parquet Writing and Reading Complete" << std::endl;

  return 0;
}

void getnumrows(char* num,int64_t& num_rows){
  int charlen = strlen(num);
  int charin = 0;
  while ( num[charin] != '\0' ) {
    num_rows += (num[charin] - 48)*((int64_t)pow(10,charlen-charin));
    charin++;
  }
}

int intlog(int num_rows){
  return (int)log10(num_rows);
}

char* convertToCharptr(int64_t number,char*& predicate,int charlen){
  int i = 0;
  for ( ; i < charlen ; i++ ) {
     predicate[charlen-i-1] = number%10+48;
     number = number/10;
  }
  predicate[i] = '\0';
  return predicate;
}


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
     std::ofstream runfile;
     runfile.open(PARQUET_FILENAME+"-run-results.txt");//+"-"+std::to_string(col_id);
     if ( argc == 3 ){
        // Point Queries & range queries
        
        int64_t num_rows = 0;
        int num_queries = 1000;
        int num_runs = 1;

      //   char *col_num = argv[3];
      //  std::stringstream ss(col_num);
      //  int col_id;
      //  ss >> col_id;

        getnumrows(argv[2],num_rows);
        
        trun times_by_type[num_columns];
        
        runfile << time(NULL) << std::endl;
        runfile << "############################## --  RUNNING POINT QUERIES -- ########################################" << std::endl;
        for ( int col_id = 0; col_id < num_columns; col_id++){
                  
          times_by_type[col_id].w_index = 0.0;
          times_by_type[col_id].wo_index = 0.0;
          times_by_type[col_id].wo_totaltime = 0.0;
          times_by_type[col_id].w_totaltime = 0.0;
          times_by_type[col_id].b_totaltime = 0.0;
          times_by_type[col_id].w_blf_totaltime = 0.0;
          times_by_type[col_id].w_pageblf_totaltime = 0.0;
          times_by_type[col_id].b_index = 0.0;
          times_by_type[col_id].wo_total_pages_scanned = 0.0;
          times_by_type[col_id].w_total_pages_scanned = 0.0;
          times_by_type[col_id].b_total_pages_scanned = 0.0;
          times_by_type[col_id].blf_load_time = 0.0;
          times_by_type[col_id].index_load_time = 0.0;
        }
        
        // over each rowgroup
        for ( int r = 0; r < num_row_groups; r++) {
        // for each column so many queries run so many times.
        for ( int col_id =0; col_id < num_columns; col_id++){
            //re-initialize column index, offset index and bloomfilters for each column
            std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);
         // for that column so many runs
          for(int i=0; i < num_runs; i++){    
            int predicateindex = 0;
            char** predicates = (char**)malloc(sizeof(char*)*num_queries);
            while ( predicateindex < num_queries ){
              // one query of the queries of the run
              // sleep(1);
              srand(time(NULL));
              char* predicate_val = (char*)malloc(intlog(num_rows)+1);
              convertToCharptr(rand()%num_rows,predicate_val,intlog(num_rows));
              predicates[predicateindex] = predicate_val;
            
              runfile  << " run number " << i << "-- Query number " << predicateindex << "-- col_num " << col_id  << " predicate: " << predicates[predicateindex] << std::endl;
              trun avgtime = run_for_one_predicate(runfile, num_columns,row_group_reader,parquet_reader,col_id,predicates,predicateindex,0,true,true,true);
              
              times_by_type[col_id].wo_totaltime += avgtime.wo_totaltime;
              times_by_type[col_id].w_totaltime += avgtime.w_totaltime;
              times_by_type[col_id].b_totaltime += avgtime.b_totaltime;
              times_by_type[col_id].w_blf_totaltime += avgtime.w_blf_totaltime;
              // times_by_type[col_id].w_pageblf_totaltime = avgtime.w_pageblf_totaltime;

              times_by_type[col_id].wo_total_pages_scanned += avgtime.wo_total_pages_scanned;
              times_by_type[col_id].w_total_pages_scanned += avgtime.w_total_pages_scanned;
              times_by_type[col_id].b_total_pages_scanned += avgtime.b_total_pages_scanned;
              times_by_type[col_id].w_blf_total_pages_scanned += avgtime.w_blf_total_pages_scanned;
              // times_by_type[col_id].w_pageblf_total_pages_scanned += avgtime.w_pageblf_total_pages_scanned;

              times_by_type[col_id].wo_mem_used += avgtime.wo_mem_used;
              times_by_type[col_id].w_mem_used += avgtime.w_mem_used;
              times_by_type[col_id].b_mem_used += avgtime.b_mem_used;
              times_by_type[col_id].w_blf_mem_used += avgtime.w_blf_mem_used;
              // times_by_type[col_id].w_pageblf_mem_used += avgtime.w_pageblf_mem_used;

              times_by_type[col_id].wo_read_bytes += avgtime.wo_read_bytes;
              times_by_type[col_id].w_read_bytes += avgtime.w_read_bytes;
              times_by_type[col_id].b_read_bytes += avgtime.b_read_bytes;
              times_by_type[col_id].w_blf_read_bytes += avgtime.w_blf_read_bytes;
              // times_by_type[col_id].w_pageblf_read_bytes += avgtime.w_pageblf_read_bytes;

              times_by_type[col_id].wo_write_bytes += avgtime.wo_write_bytes;
              times_by_type[col_id].w_write_bytes += avgtime.w_write_bytes;
              times_by_type[col_id].b_write_bytes += avgtime.b_write_bytes;
              times_by_type[col_id].w_blf_write_bytes += avgtime.w_blf_write_bytes;
              // times_by_type[col_id].w_pageblf_write_bytes += avgtime.w_pageblf_write_bytes;
              times_by_type[col_id].blf_load_time = row_group_reader->GetBLFLoadTime();
              times_by_type[col_id].index_load_time = row_group_reader->GetIndexLoadTime();

              predicateindex++;
            }
          }

          runfile << "############################### -- POINT QUERY RUN TIME RESULTS FINAL --" << col_id << "-- ################################" << std::endl;

          runfile<< "|----------------------------col_num " << col_id << "----------------------------|" << std::endl;
          
          runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w/o index " 
          << (times_by_type[col_id].wo_totaltime/(num_runs*num_queries)) << std::endl
          << " avg num of datapage indices scanned " << (times_by_type[col_id].wo_total_pages_scanned/(num_runs*num_queries)) << std::endl
          << " avg memory used in kB " << times_by_type[col_id].wo_mem_used << std::endl
          << " avg bytes read " << times_by_type[col_id].wo_read_bytes << std::endl
          << " avg bytes written " << times_by_type[col_id].wo_write_bytes
          << std::endl;
          
          runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w index " 
          << (times_by_type[col_id].w_totaltime/(num_runs*num_queries)) << std::endl
          << " avg num of datapage indices scanned " << (times_by_type[col_id].w_total_pages_scanned/(num_runs*num_queries)) << std::endl
          << " avg memory used in kB " << times_by_type[col_id].w_mem_used << std::endl
          << " avg bytes read " << times_by_type[col_id].w_read_bytes << std::endl
          << " avg bytes written " << times_by_type[col_id].w_write_bytes
          << std::endl;
          
          runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w index with bloomfilter " 
          << (times_by_type[col_id].b_totaltime/(num_runs*num_queries)) << std::endl
          << " avg num of datapage indices scanned " << (times_by_type[col_id].b_total_pages_scanned/(num_runs*num_queries)) << std::endl
          << " avg memory used in kB " << times_by_type[col_id].b_mem_used << std::endl
          << " avg bytes read " << times_by_type[col_id].b_read_bytes << std::endl
          << " avg bytes written " << times_by_type[col_id].b_write_bytes
          << std::endl;
        
          runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w/o index with bloomfilter " 
          << (times_by_type[col_id].w_blf_totaltime/(num_runs*num_queries)) << std::endl
          << " avg num of datapage indices scanned " << (times_by_type[col_id].w_blf_total_pages_scanned/(num_runs*num_queries)) << std::endl
          << " avg memory used in kB " << times_by_type[col_id].w_blf_mem_used << std::endl
          << " avg bytes read " << times_by_type[col_id].w_blf_read_bytes << std::endl
          << " avg bytes written " << times_by_type[col_id].w_blf_write_bytes
          << std::endl;

          // runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w index with binary with bloomfilter " 
          // << (times_by_type[col_id].w_pageblf_totaltime/(num_runs*num_queries)) << std::endl
          // << " avg num of datapage indices scanned " << (times_by_type[col_id].w_pageblf_total_pages_scanned/(num_runs*num_queries)) << std::endl
          // << " avg memory used in kB " << times_by_type[col_id].w_pageblf_mem_used << std::endl
          // << " avg bytes read " << times_by_type[col_id].w_pageblf_read_bytes << std::endl
          // << " avg bytes written " << times_by_type[col_id].w_pageblf_write_bytes
          // << std::endl;
            
          runfile<< "|----------------------------------------------------------------------------------|" << std::endl;

        }
        }
         
        runfile << "############################### -- POINT QUERY RUN TIME RESULTS FINAL  ################################" << std::endl;
        for ( int col_id =0; col_id < num_columns; col_id++){
          runfile<< "|----------------------------col_num " << col_id << "----------------------------|" << std::endl;
          
          runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w/o index " 
          << (times_by_type[col_id].wo_totaltime/(num_runs*num_queries)) << std::endl
          << " avg num of datapage indices scanned " << (times_by_type[col_id].wo_total_pages_scanned/(num_runs*num_queries)) << std::endl
          << " avg memory used in kB " << times_by_type[col_id].wo_mem_used << std::endl
          << " avg bytes read " << times_by_type[col_id].wo_read_bytes << std::endl
          << " avg bytes written " << times_by_type[col_id].wo_write_bytes
          << std::endl;
          
          runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w index " 
          << (times_by_type[col_id].w_totaltime/(num_runs*num_queries)) << std::endl
          << " avg num of datapage indices scanned " << (times_by_type[col_id].w_total_pages_scanned/(num_runs*num_queries)) << std::endl
          << " avg memory used in kB " << times_by_type[col_id].w_mem_used << std::endl
          << " avg bytes read " << times_by_type[col_id].w_read_bytes << std::endl
          << " avg bytes written " << times_by_type[col_id].w_write_bytes
          << " index load time " << times_by_type[col_id].index_load_time
          << std::endl;
          
          runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w index with bloomfilter " 
          << (times_by_type[col_id].b_totaltime/(num_runs*num_queries)) << std::endl
          << " avg num of datapage indices scanned " << (times_by_type[col_id].b_total_pages_scanned/(num_runs*num_queries)) << std::endl
          << " avg memory used in kB " << times_by_type[col_id].b_mem_used << std::endl
          << " avg bytes read " << times_by_type[col_id].b_read_bytes << std::endl
          << " avg bytes written " << times_by_type[col_id].b_write_bytes
          << " index load time " << times_by_type[col_id].index_load_time
          << " blf load time " << times_by_type[col_id].blf_load_time
          << std::endl;
        
          runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w/o index with bloomfilter " 
          << (times_by_type[col_id].w_blf_totaltime/(num_runs*num_queries)) << std::endl
          << " avg num of datapage indices scanned " << (times_by_type[col_id].w_blf_total_pages_scanned/(num_runs*num_queries)) << std::endl
          << " avg memory used in kB " << times_by_type[col_id].w_blf_mem_used << std::endl
          << " avg bytes read " << times_by_type[col_id].w_blf_read_bytes << std::endl
          << " avg bytes written " << times_by_type[col_id].w_blf_write_bytes
          << " blf load time " << times_by_type[col_id].blf_load_time
          << std::endl;

          // runfile << std::setprecision(3)  <<"POINT QUERY: minimum average time w index with binary with bloomfilter " 
          // << (times_by_type[col_id].w_pageblf_totaltime/(num_runs*num_queries)) << std::endl
          // << " avg num of datapage indices scanned " << (times_by_type[col_id].w_pageblf_total_pages_scanned/(num_runs*num_queries)) << std::endl
          // << " avg memory used in kB " << times_by_type[col_id].w_pageblf_mem_used << std::endl
          // << " avg bytes read " << times_by_type[col_id].w_pageblf_read_bytes << std::endl
          // << " avg bytes written " << times_by_type[col_id].w_pageblf_write_bytes
          // << std::endl;
            
          runfile<< "|----------------------------------------------------------------------------------|" << std::endl;

        }
        runfile << "#######################################################################################################" << std::endl;
      }

     if ( argc == 4 ) {
       char *col_num = argv[2];
       std::stringstream ss(col_num);
       int colid;
       ss >> colid;
       for ( int r = 0; r < num_row_groups; r++) {
        std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);
          run_for_one_predicate(runfile,num_columns,row_group_reader,parquet_reader,colid,argv,3,0,true,true,true);
       }
     }
     

     if ( argc == 5 ){
       char *col_num = argv[2];
       std::stringstream ss(col_num);
       int colid;
       ss >> colid;
       for ( int r = 0; r < num_row_groups; r++) {
        std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);
         run_for_one_predicate(runfile,num_columns,row_group_reader,parquet_reader,colid,argv,3,1,true,true,true);
         run_for_one_predicate(runfile,num_columns,row_group_reader,parquet_reader,colid,argv,4,-1,true,true,true);
       }
     }
     runfile.close();
     return 0;
   } catch (const std::exception& e) {
      std::cerr << "Parquet read error: " << e.what() << std::endl;
      return -1;
  }

}

trun run_for_one_predicate(std::ofstream& runfile,int num_columns,std::shared_ptr<parquet::RowGroupReader>& row_group_reader, std::unique_ptr<parquet::ParquetFileReader>& parquet_reader, int colid,char** argv,int predicate_index, 
                           int equal_to, bool binary_search, bool with_bloom_filter, bool with_page_bf) {

    
    trun avgtime;
    int64_t prev_num_bytes_r = 0;
    int64_t prev_num_bytes_rc = 0;
    int64_t prev_num_bytes_w = 0;
    int64_t prev_num_bytes_wc = 0;
    int64_t curr_num_bytes_r = 0;
    int64_t curr_num_bytes_rc = 0;
    int64_t curr_num_bytes_w = 0;
    int64_t curr_num_bytes_wc = 0;
    int64_t prev_mem_used = 0;
    int64_t curr_mem_used = 0;
  // Iterate over all the RowGroups in the file
    //for (int r = 0; r < num_row_groups; ++r) 
    {
    
      
      char *predicate_val  = argv[predicate_index];

      int col_id = colid;
        // Get the RowGroup Reader

        clock_t start_time,end_time;
        float total_time= 0.0;
        int num_runs = 1;
         
        float total_pages_scanned = 0.0;

        runfile << " Column ID: " << col_id << "| Column Type: " << row_group_reader->Column(col_id)->type() << std::endl;

        /********FIRST PASS WITHOUT INDEX***************/
        /*total_time = 0.0;
        prev_mem_used = getMemValue();
        prev_num_bytes_r = getReadBytesValue();
        prev_num_bytes_w = getWriteBytesValue();
        runfile << " ########################################################################## " << std::endl;
        runfile << "\n time for predicate one pass without index: " << std::endl;
        for(int t  =0 ; t< num_runs; t++){
            auto start_time = std::chrono::high_resolution_clock::now();
          total_pages_scanned += first_pass_for_predicate_only(runfile, row_group_reader,col_id,num_columns,predicate_val,false,equal_to,!binary_search,!with_bloom_filter, !with_page_bf);
          auto end_time = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::microseconds>(start_time-end_time);
            float time_elapsed = (float) duration.count();

            runfile << std::setprecision(3) << time_elapsed << std::endl;
            curr_mem_used = getMemValue();
            curr_num_bytes_r = getReadBytesValue();
            curr_num_bytes_w = getWriteBytesValue();
            runfile << "\n memory used currently by the process in virtual memory (in kB): " << curr_mem_used << std::endl;
            runfile << "\n change in memory used (in kB): " << curr_mem_used-prev_mem_used << std::endl;
            runfile << "\n number of bytes read from storage layer (in B): " << curr_num_bytes_r - prev_num_bytes_r << std::endl;
            runfile << "\n number of bytes written to storage (in B): " << curr_num_bytes_w - prev_num_bytes_w << std::endl; 
            runfile << "\n number of bytes read from cache (in B): " << curr_num_bytes_rc - prev_num_bytes_r << std::endl;
            runfile << "\n number of bytes written cancelled by cache (in B): " << curr_num_bytes_wc - prev_num_bytes_wc << std::endl; 

            total_time = (t!=0 && time_elapsed > total_time)? total_time:time_elapsed;
        }
        avgtime.wo_total_pages_scanned = total_pages_scanned/num_runs;
        avgtime.wo_totaltime = total_time;
        avgtime.wo_mem_used = curr_mem_used-prev_mem_used;
        avgtime.wo_read_bytes = curr_num_bytes_r - prev_num_bytes_r;
        avgtime.wo_write_bytes = curr_num_bytes_w - prev_num_bytes_w;
        runfile << " ------------------------------------------------------------------------ " << std::endl;*/
       
        /**************FIRST PASS WITH INDEX WITHOUT BINARY WITHOUT BF PAGE BF*****************/

        /*total_time = 0.0;
        total_pages_scanned = 0.0;
        prev_mem_used = getMemValue();
        prev_num_bytes_r = getReadBytesValue();
        prev_num_bytes_w = getWriteBytesValue();
        runfile << " ------------------------------------------------------------------------ " << std::endl;
        runfile << "\n time for predicate one pass without bloom filter: " << std::endl;
        for(int t  =0 ; t< num_runs; t++){
            auto start_time = std::chrono::high_resolution_clock::now();
          first_pass_for_predicate_only(runfile, row_group_reader,col_id,num_columns,predicate_val,true,equal_to, !binary_search, !with_bloom_filter,!with_page_bf);
          auto end_time = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::microseconds>(start_time-end_time);
            float time_elapsed = (float) duration.count();

            runfile << std::setprecision(3) << time_elapsed << std::endl;
            curr_mem_used = getMemValue();
            curr_num_bytes_r = getReadBytesValue();
            curr_num_bytes_w = getWriteBytesValue();
            runfile << "\n memory used currently by the process in virtual memory (in kB): " << curr_mem_used << std::endl;
            runfile << "\n change in memory used (in kB): " << curr_mem_used-prev_mem_used << std::endl;
            runfile << "\n number of bytes read from storage layer (in B): " << curr_num_bytes_r - prev_num_bytes_r << std::endl;
            runfile << "\n number of bytes written to storage (in B): " << curr_num_bytes_w - prev_num_bytes_w << std::endl; 
            runfile << "\n number of bytes read from cache (in B): " << curr_num_bytes_rc - prev_num_bytes_r << std::endl;
            runfile << "\n number of bytes written cancelled by cache (in B): " << curr_num_bytes_wc - prev_num_bytes_wc << std::endl; 
            runfile << "\n index load time: " << row_group_reader->GetIndexLoadTime() << std::endl; 
            total_time = (t!=0 && time_elapsed > total_time)? total_time:time_elapsed;
        }
        
        avgtime.w_total_pages_scanned = total_pages_scanned/num_runs;
        avgtime.w_totaltime = total_time;
        avgtime.w_mem_used = curr_mem_used-prev_mem_used;
        avgtime.w_read_bytes = curr_num_bytes_r - prev_num_bytes_r;
        avgtime.w_write_bytes = curr_num_bytes_w - prev_num_bytes_w;
        runfile << " ------------------------------------------------------------------------ " << std::endl;*/
        /**************FIRST PASS WITH INDEX WITH BINARY WITHOUT BF PAGE BF*****************/

        /*total_time = 0.0;
        total_pages_scanned = 0.0;
        prev_mem_used = getMemValue();
        prev_num_bytes_r = getReadBytesValue();
        prev_num_bytes_w = getWriteBytesValue();
        runfile << " ------------------------------------------------------------------------ " << std::endl;
        runfile << "\n time for predicate one pass with bloom filter: "  << std::endl;
        for(int t  =0 ; t< num_runs; t++){
            auto start_time = std::chrono::high_resolution_clock::now();
          first_pass_for_predicate_only(runfile, row_group_reader,col_id,num_columns,predicate_val,true,equal_to, !binary_search, with_bloom_filter,!with_page_bf);
          auto end_time = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::microseconds>(start_time-end_time);
          
            float time_elapsed = (float) duration.count();

            runfile << std::setprecision(3) << time_elapsed << std::endl;
            curr_mem_used = getMemValue();
            curr_num_bytes_r = getReadBytesValue();
            curr_num_bytes_w = getWriteBytesValue();
            runfile << "\n memory used currently by the process in virtual memory (in kB): " << curr_mem_used << std::endl;
            runfile << "\n change in memory used (in kB): " << curr_mem_used-prev_mem_used << std::endl;
            runfile << "\n number of bytes read from storage layer (in B): " << curr_num_bytes_r - prev_num_bytes_r << std::endl;
            runfile << "\n number of bytes written to storage (in B): " << curr_num_bytes_w - prev_num_bytes_w << std::endl; 
            runfile << "\n number of bytes read from cache (in B): " << curr_num_bytes_rc - prev_num_bytes_r << std::endl;
            runfile << "\n number of bytes written cancelled by cache (in B): " << curr_num_bytes_wc - prev_num_bytes_wc << std::endl; 
            runfile << "\n index load time: " << row_group_reader->GetIndexLoadTime() << std::endl; 
            runfile << "\n blf load time: " << row_group_reader->GetBLFLoadTime() << std::endl; 
            total_time = (t!=0 && time_elapsed > total_time)? total_time:time_elapsed;
        }
        
        avgtime.b_total_pages_scanned = total_pages_scanned/num_runs;
        avgtime.b_totaltime = total_time;
        avgtime.b_mem_used = curr_mem_used-prev_mem_used;
        avgtime.b_read_bytes = curr_num_bytes_r - prev_num_bytes_r;
        avgtime.b_write_bytes = curr_num_bytes_w - prev_num_bytes_w;
        runfile << " ------------------------------------------------------------------------ " << std::endl;*/
        /**************FIRST PASS WITH INDEX WITH BINARY WITH BF WITHOUT PAGE BF*****************/

        total_time = 0.0;
        total_pages_scanned = 0.0;
        prev_mem_used = getMemValue();
        prev_num_bytes_r = getReadBytesValue();
        prev_num_bytes_w = getWriteBytesValue();
        runfile << " ------------------------------------------------------------------------ " << std::endl;
        runfile << "\n time for predicate without index with bloom filter: " << std::endl;
        for(int t  =0 ; t< num_runs; t++){
            auto start_time = std::chrono::high_resolution_clock::now();
          first_pass_for_predicate_only(runfile, row_group_reader,col_id,num_columns,predicate_val,false,equal_to, !binary_search, with_bloom_filter,!with_page_bf);
          auto end_time = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::microseconds>(start_time-end_time);

            float time_elapsed = (float) duration.count();

            runfile << std::setprecision(3) << time_elapsed << std::endl;
            curr_mem_used = getMemValue();
            curr_num_bytes_r = getReadBytesValue();
            curr_num_bytes_w = getWriteBytesValue();
            runfile << "\n memory used currently by the process in virtual memory (in kB): " << curr_mem_used << std::endl;
            runfile << "\n change in memory used (in kB): " << curr_mem_used-prev_mem_used << std::endl;
            runfile << "\n number of bytes read from storage layer (in B): " << curr_num_bytes_r - prev_num_bytes_r << std::endl;
            runfile << "\n number of bytes written to storage (in B): " << curr_num_bytes_w - prev_num_bytes_w << std::endl; 
            runfile << "\n number of bytes read from cache (in B): " << curr_num_bytes_rc - prev_num_bytes_r << std::endl;
            runfile << "\n number of bytes written cancelled by cache (in B): " << curr_num_bytes_wc - prev_num_bytes_wc << std::endl; 
            runfile << "\n blf load time: " << row_group_reader->GetBLFLoadTime() << std::endl; 
            total_time = (t!=0 && time_elapsed > total_time)? total_time:time_elapsed;
        }
        
        avgtime.w_blf_total_pages_scanned = total_pages_scanned/num_runs;
        avgtime.w_blf_totaltime = total_time;
        avgtime.w_blf_mem_used = curr_mem_used-prev_mem_used;
        avgtime.w_blf_read_bytes = curr_num_bytes_r - prev_num_bytes_r;
        avgtime.w_blf_write_bytes = curr_num_bytes_w - prev_num_bytes_w;
        runfile << " ########################################################################## " << std::endl;

      /***********FIRST PASS END **********/

      /***********Second PASS *************/
                //  TODO //

      /***********************************/
      
     }
     return avgtime;
}


int64_t first_pass_for_predicate_only(std::ofstream& runfile, std::shared_ptr<parquet::RowGroupReader> row_group_reader,int col_id, int num_columns, char* predicate_val,bool with_index,
                                   int equal_to, bool binary_search, bool with_bloom_filter, bool with_page_bf) {

    int64_t row_index = 0;
    int64_t count_pages_scanned = 0, total_num_pages = 0, last_first_row = 0;

    std::vector<int> col_row_counts(num_columns, 0);

    //      assert(row_group_reader->metadata()->total_byte_size() < ROW_GROUP_SIZE);

    // int16_t definition_level;
    // int16_t repetition_level;
    std::shared_ptr<parquet::ColumnReader> column_reader;
    

    // std::cout<< "test arg v" <<argv[1] << std::endl;
  
    int64_t page_index = -1;

    std::vector<int64_t> unsorted_page_index; 
    std::vector<int64_t> unsorted_row_index;

    char c;
    // int64_t predicate;
    // sscanf(argv[2], "%" SCNd64 "%c", &predicate, &c);

    // int PREDICATE_COL;
    // sscanf(argv[2], "%d" "%c", &PREDICATE_COL, &c);
    // Get the Column Reader for the Int64 column
      std::shared_ptr<parquet::ColumnReader> predicate_column_reader = row_group_reader->Column(col_id);
      
      
      
      // std::cout << "given predicate: " << predicate << " type of predicate: " << typeid(predicate).name() << std::endl;
      
      std::shared_ptr<parquet::ColumnReader> column_reader_with_index;
      
      parquet::ColumnReader* generic_reader;
  
      int PREDICATE_COL  = col_id;
      return_multiple vals = getPredicate(predicate_column_reader,row_group_reader,predicate_val,col_id,page_index,PREDICATE_COL,row_index,with_index,binary_search, count_pages_scanned,
                                            total_num_pages, last_first_row, with_bloom_filter, with_page_bf,
                                            unsorted_page_index, unsorted_row_index);
      column_reader_with_index = vals.column_reader;
      
      //SAMPLE row group reader call in the comment below
      // row_group_reader->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,predicate_column_reader->type());

      returnReaderwithType(column_reader_with_index,generic_reader);

      int counter = 0;
      int ind = 0;
      int64_t row_counter = 0;

      if (unsorted_row_index.size()==0){
        if ( row_index != -1 ) {
        if(with_index){
          ind = row_index;
          row_counter = 0;
          generic_reader->Skip(row_index);
          do{ ind++;
            if((printVal(runfile, column_reader_with_index,generic_reader,ind,vals,row_counter,true,equal_to)))
               break;
          }while((generic_reader->HasNext()));
        }
        else{
            while (generic_reader->HasNext()) { 
              ind++;
              count_pages_scanned++;
              if(printVal(runfile, column_reader_with_index,generic_reader,ind,vals,row_counter,true,equal_to))
               break;
          //        int64_t expected_value = col_row_counts[col_id];  
          //        assert(value == expected_value);
              col_row_counts[col_id]++;
            } 
          }
          // Read all the rows in the column
          runfile << "| page index: " << page_index << "| number of rows loaded: " << ind <<
          "| total number of pages: " << total_num_pages << "| last page first row index: " << last_first_row << std::endl;
        
        }
        else{
          runfile << "non-member query" << std::endl;
        }
      }
       else{
         ind = 0;
         int index_list_count = 0;
         bool found = false;
         for(int64_t row_index: unsorted_row_index) {
              row_counter = 0;
              generic_reader->Skip(row_index);
              do{ ind++;
                  if((printVal(runfile, column_reader_with_index,generic_reader,ind,vals,row_counter,true,equal_to))){
                    found = true;
                    break;
                  }
                  
              }while((generic_reader->HasNext()));
            // Read all the rows in the column
            runfile << "| page index: " << unsorted_page_index[index_list_count] << "| number of rows loaded: " << ind <<
           "| total number of pages: " << total_num_pages << "| last page first row index: " << last_first_row << std::endl;
            index_list_count++;
            if (found) break;
          }
          if ( ind == (int)unsorted_row_index.size())
             runfile << "non-member query" << std::endl;
       }

      return count_pages_scanned;
}

return_multiple getPredicate(std::shared_ptr<parquet::ColumnReader> cr,std::shared_ptr<parquet::RowGroupReader> rg,char* predicate_val,
                             int& col_id,int64_t& page_index,int& PREDICATE_COL,int64_t& row_index, bool with_index, 
                             bool binary_search, int64_t& count_pages_scanned,
                             int64_t& total_num_pages, int64_t& last_first_row, bool with_bloom_filter, bool with_page_bf,
                             std::vector<int64_t>& unsorted_min_index, std::vector<int64_t>& unsorted_row_index){
    const int CHAR_LEN = 10000000;
    
    return_multiple vals;
    std::stringstream ss(predicate_val);
    switch(cr->type()){
          case Type::BOOLEAN:{
            bool b;
            
            ss >> std::boolalpha >> b;
            void * predicate = static_cast<void*>(&b);

            vals.column_reader = 
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type(),with_index, binary_search, count_pages_scanned,
                                            total_num_pages, last_first_row, with_bloom_filter,with_page_bf,
                                            unsorted_min_index, unsorted_row_index);
            vals.b = b;
            return vals;
          }
          case Type::INT32:{
            int32_t val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = 
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type(),with_index, binary_search, count_pages_scanned,
                                            total_num_pages, last_first_row, with_bloom_filter,with_page_bf,
                                            unsorted_min_index, unsorted_row_index);
            vals.p = val;
            return vals;
          }
          case Type::INT64:{
            int64_t val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = 
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type(),with_index, binary_search, count_pages_scanned,
                                            total_num_pages, last_first_row, with_bloom_filter,with_page_bf,
                                            unsorted_min_index, unsorted_row_index);
            vals.r = val;
            return vals;
          }
          case Type::INT96:{
            uint32_t val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = 
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type(),with_index, binary_search, count_pages_scanned,
                                            total_num_pages, last_first_row, with_bloom_filter,with_page_bf,
                                            unsorted_min_index, unsorted_row_index);
            vals.e = val;
            return vals;
          }
          case Type::FLOAT:{
            float val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = 
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type(),with_index, binary_search, count_pages_scanned,
                                            total_num_pages, last_first_row, with_bloom_filter,with_page_bf,
                                            unsorted_min_index, unsorted_row_index);
            vals.d = val;
            return vals;
          }
          case Type::DOUBLE:{
            double val;
            
            ss >> val;
            void * predicate = static_cast<void*>(&val);
            vals.column_reader = 
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type(),with_index, binary_search, count_pages_scanned,
                                            total_num_pages, last_first_row, with_bloom_filter,with_page_bf,
                                            unsorted_min_index, unsorted_row_index);
            vals.i = val;
            return vals;
          }
          case Type::BYTE_ARRAY:{
            char* val = predicate_val;
            
            void * predicate = static_cast<void*>(val);
            vals.column_reader = 
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type(),with_index, binary_search, count_pages_scanned,
                                            total_num_pages, last_first_row, with_bloom_filter,with_page_bf,
                                            unsorted_min_index, unsorted_row_index);
            vals.c = val;
            return vals;
          }
          case Type::FIXED_LEN_BYTE_ARRAY:{
            char* val = predicate_val;
            
            void * predicate = static_cast<void*>(val);
            vals.column_reader = 
                      rg->ColumnWithIndex(col_id,predicate,page_index,PREDICATE_COL,row_index,cr->type(),with_index, binary_search, count_pages_scanned,
                                            total_num_pages, last_first_row, with_bloom_filter,with_page_bf,
                                            unsorted_min_index, unsorted_row_index);
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

bool printVal(std::ofstream& runfile, std::shared_ptr<parquet::ColumnReader>column_reader, parquet::ColumnReader* int64_reader,int ind,return_multiple vals,int64_t& row_counter,
              bool checkpredicate = false,int equal_to = 0) {

      int64_t values_read = 0;
      //int64_t 0;
       switch (column_reader->type()) {
       case Type::BOOLEAN:
          {
           bool test;
           bool predicate = vals.b;
           int64_reader->callReadBatch(1,&test,&values_read);
           row_counter = ind;
           
           if ( equal_to == 0 && checkpredicate && test == predicate) {
           row_counter = ind;
           runfile << "with predicate row number: " << row_counter << " " << test << "\n" ;
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }
          else if ( equal_to == -1 && checkpredicate && test < predicate ){

          }
          else if ( equal_to == 1 && checkpredicate && test > predicate ) {

          }
          else{
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
           
           if ( equal_to == 0 && checkpredicate && val == predicate) {
           row_counter = ind;
           runfile << "with predicate row number: " << row_counter << " " << val << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }
          else if ( equal_to == -1 && checkpredicate && val < predicate ){

          }
          else if ( equal_to == 1 && checkpredicate && val > predicate ) {

          }
          else{
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
          if ( equal_to == 0 && checkpredicate && value == predicate) {
           row_counter = ind;
           runfile << "with predicate row number: " << row_counter << " " << value << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }
          else if ( equal_to == -1 && checkpredicate && value < predicate ){

          }
          else if ( equal_to == 1 && checkpredicate && value > predicate ) {

          }
          else{
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
           
           if ( equal_to == 0 && checkpredicate && val == predicate) {
           row_counter = ind;
           runfile << "with predicate row number: " << row_counter << " " << val << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }
          else if ( equal_to == -1 && checkpredicate && val < predicate ){

          }
          else if ( equal_to == 1 && checkpredicate && val > predicate ) {

          }
          else{
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
              float error_factor = 9*pow(10,15);
           int64_reader->callReadBatch(1,&val,&values_read);
           if ( checkpredicate && fabs(val-predicate)<=std::numeric_limits<double>::epsilon()*error_factor) {
           row_counter = ind;
           runfile << "with predicate row number: " << row_counter << " " << val << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }
          else if ( equal_to == -1 && checkpredicate && val < predicate ){

          }
          else if ( equal_to == 1 && checkpredicate && val > predicate ) {

          }
          
          else{
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
           double error_factor = 9*pow(10,15);

           if ( equal_to == 0 && checkpredicate && fabs(val-predicate)<=std::numeric_limits<double>::epsilon()*error_factor) {
           row_counter = ind;
           runfile << "with predicate row number: " << row_counter << " " << val << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }
          else if ( equal_to == -1 && checkpredicate && val < predicate ){

          }
          else if ( equal_to == 1 && checkpredicate && val > predicate ) {

          }
          
          else{
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
            uint32_t FIXED_LENGTH = 124;
            char dest[FIXED_LENGTH];
            for ( uint32_t i = 0; i < (FIXED_LENGTH-strlen(predicate));i++) dest[i] = '0';
            for ( uint32_t i = (FIXED_LENGTH-strlen(predicate)); i < FIXED_LENGTH;i++) dest[i] = predicate[i-(FIXED_LENGTH-strlen(predicate))];
            dest[FIXED_LENGTH] = '\0';
            std::string pstring(dest);
            int64_reader->callReadBatch(1,&str,&values_read);
            std::string result_value = parquet::ByteArrayToString(str);
            // std::string result(result_value.substr(result_value.length()-strlen(predicate),strlen(predicate)));
            row_counter = ind;
            // std::cout << "row number: " << row_counter << " " << result << "\n";
            if ( equal_to == 0 && checkpredicate && result_value.compare(pstring) == 0) {
           row_counter = ind;
           runfile << "with predicate row number: " << row_counter << " " << result_value << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }
          else if ( equal_to == -1 && checkpredicate && result_value.compare(pstring) < 0 ){

          }
          else if ( equal_to == 1 && checkpredicate && result_value.compare(pstring) > 0 ) {

          }
          else{
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
            std::string result_value = parquet::FixedLenByteArrayToString(str,sizeof(str));
            std::string result(result_value.substr(result_value.length()-strlen(predicate),strlen(predicate)));
            row_counter = ind;
            // std::cout << "row number: " << row_counter << " " << result << "\n";
            if ( equal_to == 0 && checkpredicate && strcmp(result.c_str(),predicate) == 0) {
           row_counter = ind;
           runfile << "with predicate row number: " << row_counter << " " << result << "\n";
           //std::cout << "predicate: " << *((int64_t*)predicate) << std::endl;
           return true;
          }
          else if ( equal_to == -1 && checkpredicate && strcmp(result.c_str(),predicate) < 0 ){

          }
          else if ( equal_to == 1 && checkpredicate && strcmp(result.c_str(),predicate) > 0 ) {

          }
          else{
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
