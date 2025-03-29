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

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <dirent.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <sstream>

/*
 * This file contains samples for writing and reading encrypted Parquet files in different
 * encryption and decryption configurations.
 * Each sample section is dedicated to an independent configuration and shows its creation
 * from beginning to end.
 * The samples have the following goals:
 * 1) Demonstrate usage of different options for data encryption and decryption.
 * 2) Produce encrypted files for interoperability tests with other (eg parquet-mr)
 *    readers that support encryption.
 * 3) Produce encrypted files with plaintext footer, for testing the ability of legacy
 *    readers to parse the footer and read unencrypted columns.
 * 4) Perform interoperability tests with other (eg parquet-mr) writers, by reading
 *    encrypted files produced by these writers.
 *
 * Each write sample produces new independent parquet file, encrypted with a different
 * encryption configuration as described below.
 * The name of each file is in the form of:
 * tester<encryption config number>.parquet.encrypted.
 *
 * The read sample creates a set of decryption configurations and then uses each of them
 * to read all encrypted files in the input directory.
 *
 * The different encryption and decryption configurations are listed below.
 *
 * Usage: ./encryption-interop-tests <write/read> <path-to-directory-of-parquet-files>
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * The write sample creates files with four columns in the following
 * encryption configurations:
 *
 *  - Encryption configuration 1:   Encrypt all columns and the footer with the same key.
 *                                  (uniform encryption)
 *  - Encryption configuration 2:   Encrypt two columns and the footer, with different
 *                                  keys.
 *  - Encryption configuration 3:   Encrypt two columns, with different keys.
 *                                  Don’t encrypt footer (to enable legacy readers)
 *                                  - plaintext footer mode.
 *  - Encryption configuration 4:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix for file identity
 *                                  verification.
 *  - Encryption configuration 5:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix, and call
 *                                  disable_aad_prefix_storage to prevent file
 *                                  identity storage in file metadata.
 *  - Encryption configuration 6:   Encrypt two columns and the footer, with different
 *                                  keys. Use the alternative (AES_GCM_CTR_V1) algorithm.
 *
 * The read sample uses each of the following decryption configurations to read every
 * encrypted files in the input directory:
 *
 *  - Decryption configuration 1:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key.
 *  - Decryption configuration 2:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key. Supplies
 *                                  aad_prefix to verify file identity.
 *  - Decryption configuration 3:   Decrypt using explicit column and footer keys
 *                                  (instead of key retrieval callback).
 */

constexpr int NUM_ROWS_PER_ROW_GROUP = 500;

const char* kFooterEncryptionKey = "0123456789012345";  // 128bit/16
const char* kColumnEncryptionKey1 = "1234567890123450";
const char* kColumnEncryptionKey2 = "1234567890123451";
const char* fileName = "tester";

using FileClass = ::arrow::io::FileOutputStream;
using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

void PrintDecryptionConfiguration(int configuration);
// Check that the decryption result is as expected.
void CheckResult(std::string file, int example_id, std::string exception_msg);
// Returns true if FileName ends with suffix. Otherwise returns false.
// Used to skip unencrypted parquet files.
bool FileNameEndsWith(std::string file_name, std::string suffix);

std::vector<std::string> GetDirectoryFiles(const std::string& path) {
  std::vector<std::string> files;
  struct dirent* entry;
  DIR* dir = opendir(path.c_str());

  if (dir == NULL) {
    exit(-1);
  }
  while ((entry = readdir(dir)) != NULL) {
    files.push_back(std::string(entry->d_name));
  }
  closedir(dir);
  return files;
}

static std::shared_ptr<GroupNode> SetupSchema() {
  parquet::schema::NodeVector fields;
  // Create a primitive node named 'boolean_field' with type:BOOLEAN,
  // repetition:REQUIRED
  fields.push_back(PrimitiveNode::Make("boolean_field", Repetition::REQUIRED,
                                       Type::BOOLEAN, ConvertedType::NONE));

  // Create a primitive node named 'int32_field' with type:INT32, repetition:REQUIRED,
  // logical type:TIME_MILLIS
  fields.push_back(PrimitiveNode::Make("int32_field", Repetition::REQUIRED, Type::INT32,
                                       ConvertedType::TIME_MILLIS));

  fields.push_back(PrimitiveNode::Make("float_field", Repetition::REQUIRED, Type::FLOAT,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make("double_field", Repetition::REQUIRED, Type::DOUBLE,
                                       ConvertedType::NONE));

  // Create a GroupNode named 'schema' using the primitive nodes defined above
  // This GroupNode is the root node of the schema tree
  return std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

void InteropTestWriteEncryptedParquetFiles(std::string root_path) {
  /**********************************************************************************
                         Creating a number of Encryption configurations
   **********************************************************************************/

  // This vector will hold various encryption configurations.
  std::vector<std::shared_ptr<parquet::FileEncryptionProperties>>
      vector_of_encryption_configurations;

  // Encryption configuration 1: Encrypt all columns and the footer with the same key.
  // (uniform encryption)
  parquet::FileEncryptionProperties::Builder file_encryption_builder_1(
      kFooterEncryptionKey);
  // Add to list of encryption configurations.
  vector_of_encryption_configurations.push_back(
      file_encryption_builder_1.footer_key_metadata("kf")->build());

  // Encryption configuration 2: Encrypt two columns and the footer, with different keys.
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols2;
  std::string path1 = "double_field";
  std::string path2 = "float_field";
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_20(path1);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_21(path2);
  encryption_col_builder_20.key(kColumnEncryptionKey1)->key_id("kc1");
  encryption_col_builder_21.key(kColumnEncryptionKey2)->key_id("kc2");

  encryption_cols2[path1] = encryption_col_builder_20.build();
  encryption_cols2[path2] = encryption_col_builder_21.build();

  parquet::FileEncryptionProperties::Builder file_encryption_builder_2(
      kFooterEncryptionKey);

  vector_of_encryption_configurations.push_back(
      file_encryption_builder_2.footer_key_metadata("kf")
          ->encrypted_columns(encryption_cols2)
          ->build());

  // Encryption configuration 3: Encrypt two columns, with different keys.
  // Don’t encrypt footer.
  // (plaintext footer mode, readable by legacy readers)
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols3;
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_30(path1);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_31(path2);
  encryption_col_builder_30.key(kColumnEncryptionKey1)->key_id("kc1");
  encryption_col_builder_31.key(kColumnEncryptionKey2)->key_id("kc2");

  encryption_cols3[path1] = encryption_col_builder_30.build();
  encryption_cols3[path2] = encryption_col_builder_31.build();
  parquet::FileEncryptionProperties::Builder file_encryption_builder_3(
      kFooterEncryptionKey);

  vector_of_encryption_configurations.push_back(
      file_encryption_builder_3.footer_key_metadata("kf")
          ->encrypted_columns(encryption_cols3)
          ->set_plaintext_footer()
          ->build());

  // Encryption configuration 4: Encrypt two columns and the footer, with different keys.
  // Use aad_prefix.
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols4;
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_40(path1);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_41(path2);
  encryption_col_builder_40.key(kColumnEncryptionKey1)->key_id("kc1");
  encryption_col_builder_41.key(kColumnEncryptionKey2)->key_id("kc2");

  encryption_cols4[path1] = encryption_col_builder_40.build();
  encryption_cols4[path2] = encryption_col_builder_41.build();
  parquet::FileEncryptionProperties::Builder file_encryption_builder_4(
      kFooterEncryptionKey);

  vector_of_encryption_configurations.push_back(
      file_encryption_builder_4.footer_key_metadata("kf")
          ->encrypted_columns(encryption_cols4)
          ->aad_prefix(fileName)
          ->build());

  // Encryption configuration 5: Encrypt two columns and the footer, with different keys.
  // Use aad_prefix and disable_aad_prefix_storage.
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols5;
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_50(path1);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_51(path2);
  encryption_col_builder_50.key(kColumnEncryptionKey1)->key_id("kc1");
  encryption_col_builder_51.key(kColumnEncryptionKey2)->key_id("kc2");

  encryption_cols5[path1] = encryption_col_builder_50.build();
  encryption_cols5[path2] = encryption_col_builder_51.build();
  parquet::FileEncryptionProperties::Builder file_encryption_builder_5(
      kFooterEncryptionKey);

  vector_of_encryption_configurations.push_back(
      file_encryption_builder_5.encrypted_columns(encryption_cols5)
          ->footer_key_metadata("kf")
          ->aad_prefix(fileName)
          ->disable_aad_prefix_storage()
          ->build());

  // Encryption configuration 6: Encrypt two columns and the footer, with different keys.
  // Use AES_GCM_CTR_V1 algorithm.
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols6;
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_60(path1);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_61(path2);
  encryption_col_builder_60.key(kColumnEncryptionKey1)->key_id("kc1");
  encryption_col_builder_61.key(kColumnEncryptionKey2)->key_id("kc2");

  encryption_cols6[path1] = encryption_col_builder_60.build();
  encryption_cols6[path2] = encryption_col_builder_61.build();
  parquet::FileEncryptionProperties::Builder file_encryption_builder_6(
      kFooterEncryptionKey);

  vector_of_encryption_configurations.push_back(
      file_encryption_builder_6.footer_key_metadata("kf")
          ->encrypted_columns(encryption_cols6)
          ->algorithm(parquet::ParquetCipher::AES_GCM_CTR_V1)
          ->build());

  /**********************************************************************************
                                 PARQUET WRITER EXAMPLE
   **********************************************************************************/

  // Iterate over the encryption configurations and for each one write a parquet file.
  for (unsigned example_id = 0; example_id < vector_of_encryption_configurations.size();
       ++example_id) {
    std::stringstream ss;
    ss << example_id + 1;
    std::string test_number_string = ss.str();
    try {
      // Create a local file output stream instance.
      std::shared_ptr<FileClass> out_file;
      std::string file =
          root_path + fileName + std::string(test_number_string) + ".parquet.encrypted";
      std::cout << "Write " << file << std::endl;
      PARQUET_ASSIGN_OR_THROW(out_file, FileClass::Open(file));

      // Setup the parquet schema
      std::shared_ptr<GroupNode> schema = SetupSchema();

      // Add writer properties
      parquet::WriterProperties::Builder builder;
      builder.compression(parquet::Compression::SNAPPY);

      // Add the current encryption configuration to WriterProperties.
      builder.encryption(vector_of_encryption_configurations[example_id]);

      std::shared_ptr<parquet::WriterProperties> props = builder.build();

      // Create a ParquetFileWriter instance
      std::shared_ptr<parquet::ParquetFileWriter> file_writer =
          parquet::ParquetFileWriter::Open(out_file, schema, props);

      // Append a RowGroup with a specific number of rows.
      parquet::RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

      // Write the Bool column
      parquet::BoolWriter* bool_writer =
          static_cast<parquet::BoolWriter*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        bool value = ((i % 2) == 0) ? true : false;
        bool_writer->WriteBatch(1, nullptr, nullptr, &value);
      }

      // Write the Int32 column
      parquet::Int32Writer* int32_writer =
          static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        int32_t value = i;
        int32_writer->WriteBatch(1, nullptr, nullptr, &value);
      }

      // Write the Float column
      parquet::FloatWriter* float_writer =
          static_cast<parquet::FloatWriter*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        float value = static_cast<float>(i) * 1.1f;
        float_writer->WriteBatch(1, nullptr, nullptr, &value);
      }

      // Write the Double column
      parquet::DoubleWriter* double_writer =
          static_cast<parquet::DoubleWriter*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        double value = i * 1.1111111;
        double_writer->WriteBatch(1, nullptr, nullptr, &value);
      }
      // Close the ParquetFileWriter
      file_writer->Close();

      // Write the bytes to file
      DCHECK(out_file->Close().ok());
    } catch (const std::exception& e) {
      std::cerr << "Parquet write error: " << e.what() << std::endl;
      return;
    }
  }
}

void InteropTestReadEncryptedParquetFiles(std::string root_path) {
  std::vector<std::string> files_in_directory = GetDirectoryFiles(root_path);

  /**********************************************************************************
                       Creating a number of Decryption configurations
   **********************************************************************************/

  // This vector will hold various decryption configurations.
  std::vector<std::shared_ptr<parquet::FileDecryptionProperties>>
      vector_of_decryption_configurations;

  // Decryption configuration 1: Decrypt using key retriever callback that holds the keys
  // of two encrypted columns and the footer key.
  std::shared_ptr<parquet::StringKeyIdRetriever> string_kr1 =
      std::make_shared<parquet::StringKeyIdRetriever>();
  string_kr1->PutKey("kf", kFooterEncryptionKey);
  string_kr1->PutKey("kc1", kColumnEncryptionKey1);
  string_kr1->PutKey("kc2", kColumnEncryptionKey2);
  std::shared_ptr<parquet::DecryptionKeyRetriever> kr1 =
      std::static_pointer_cast<parquet::StringKeyIdRetriever>(string_kr1);

  parquet::FileDecryptionProperties::Builder file_decryption_builder_1;
  vector_of_decryption_configurations.push_back(
      file_decryption_builder_1.key_retriever(kr1)->build());

  // Decryption configuration 2: Decrypt using key retriever callback that holds the keys
  // of two encrypted columns and the footer key. Supply aad_prefix.
  std::shared_ptr<parquet::StringKeyIdRetriever> string_kr2 =
      std::make_shared<parquet::StringKeyIdRetriever>();
  string_kr2->PutKey("kf", kFooterEncryptionKey);
  string_kr2->PutKey("kc1", kColumnEncryptionKey1);
  string_kr2->PutKey("kc2", kColumnEncryptionKey2);
  std::shared_ptr<parquet::DecryptionKeyRetriever> kr2 =
      std::static_pointer_cast<parquet::StringKeyIdRetriever>(string_kr2);

  parquet::FileDecryptionProperties::Builder file_decryption_builder_2;
  vector_of_decryption_configurations.push_back(
      file_decryption_builder_2.key_retriever(kr2)->aad_prefix(fileName)->build());

  // Decryption configuration 3: Decrypt using explicit column and footer keys.
  std::string path_double = "double_field";
  std::string path_float = "float_field";
  std::map<std::string, std::shared_ptr<parquet::ColumnDecryptionProperties>>
      decryption_cols;
  parquet::ColumnDecryptionProperties::Builder decryption_col_builder31(path_double);
  parquet::ColumnDecryptionProperties::Builder decryption_col_builder32(path_float);

  decryption_cols[path_double] =
      decryption_col_builder31.key(kColumnEncryptionKey1)->build();
  decryption_cols[path_float] =
      decryption_col_builder32.key(kColumnEncryptionKey2)->build();

  parquet::FileDecryptionProperties::Builder file_decryption_builder_3;
  vector_of_decryption_configurations.push_back(
      file_decryption_builder_3.footer_key(kFooterEncryptionKey)
          ->column_keys(decryption_cols)
          ->build());

  /**********************************************************************************
                             PARQUET READER EXAMPLE
  **********************************************************************************/

  // Iterate over the decryption configurations and use each one to read every files
  // in the input directory.
  for (unsigned example_id = 0; example_id < vector_of_decryption_configurations.size();
       ++example_id) {
    PrintDecryptionConfiguration(example_id + 1);
    for (auto const& file : files_in_directory) {
      std::string exception_msg = "";
      if (!FileNameEndsWith(file, "parquet.encrypted"))  // Skip non encrypted files
        continue;
      try {
        std::cout << "--> Read file " << file << std::endl;

        parquet::ReaderProperties reader_properties =
            parquet::default_reader_properties();

        // Add the current decryption configuration to ReaderProperties.
        reader_properties.file_decryption_properties(
            vector_of_decryption_configurations[example_id]);

        // Create a ParquetReader instance
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(root_path + file, false,
                                                 reader_properties);

        // Get the File MetaData
        std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

        // Get the number of RowGroups
        int num_row_groups = file_metadata->num_row_groups();
        assert(num_row_groups == 1);

        // Get the number of Columns
        int num_columns = file_metadata->num_columns();
        assert(num_columns == 4);

        // Iterate over all the RowGroups in the file
        for (int r = 0; r < num_row_groups; ++r) {
          // Get the RowGroup Reader
          std::shared_ptr<parquet::RowGroupReader> row_group_reader =
              parquet_reader->RowGroup(r);

          int64_t values_read = 0;
          int64_t rows_read = 0;
          int i;
          std::shared_ptr<parquet::ColumnReader> column_reader;

          // Get the Column Reader for the boolean column
          column_reader = row_group_reader->Column(0);
          parquet::BoolReader* bool_reader =
              static_cast<parquet::BoolReader*>(column_reader.get());

          // Read all the rows in the column
          i = 0;
          while (bool_reader->HasNext()) {
            bool value;
            // Read one value at a time. The number of rows read is returned. values_read
            // contains the number of non-null rows
            rows_read = bool_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
            // Ensure only one value is read
            assert(rows_read == 1);
            // There are no NULL values in the rows written
            assert(values_read == 1);
            // Verify the value written
            bool expected_value = ((i % 2) == 0) ? true : false;
            assert(value == expected_value);
            i++;
          }
          ARROW_UNUSED(rows_read);  // suppress compiler warning in release builds

          // Get the Column Reader for the Int32 column
          column_reader = row_group_reader->Column(1);
          parquet::Int32Reader* int32_reader =
              static_cast<parquet::Int32Reader*>(column_reader.get());
          // Read all the rows in the column
          i = 0;
          while (int32_reader->HasNext()) {
            int32_t value;
            // Read one value at a time. The number of rows read is returned. values_read
            // contains the number of non-null rows
            rows_read =
                int32_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
            // Ensure only one value is read
            assert(rows_read == 1);
            // There are no NULL values in the rows written
            assert(values_read == 1);
            // Verify the value written
            assert(value == i);
            i++;
          }

          // Get the Column Reader for the Float column
          column_reader = row_group_reader->Column(2);
          parquet::FloatReader* float_reader =
              static_cast<parquet::FloatReader*>(column_reader.get());
          // Read all the rows in the column
          i = 0;
          while (float_reader->HasNext()) {
            float value;
            // Read one value at a time. The number of rows read is returned. values_read
            // contains the number of non-null rows
            rows_read =
                float_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
            // Ensure only one value is read
            assert(rows_read == 1);
            // There are no NULL values in the rows written
            assert(values_read == 1);
            // Verify the value written
            float expected_value = static_cast<float>(i) * 1.1f;
            assert(value == expected_value);
            i++;
          }

          // Get the Column Reader for the Double column
          column_reader = row_group_reader->Column(3);
          parquet::DoubleReader* double_reader =
              static_cast<parquet::DoubleReader*>(column_reader.get());
          // Read all the rows in the column
          i = 0;
          while (double_reader->HasNext()) {
            double value;
            // Read one value at a time. The number of rows read is returned. values_read
            // contains the number of non-null rows
            rows_read =
                double_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
            // Ensure only one value is read
            assert(rows_read == 1);
            // There are no NULL values in the rows written
            assert(values_read == 1);
            // Verify the value written
            double expected_value = i * 1.1111111;
            assert(value == expected_value);
            i++;
          }
        }
      } catch (const std::exception& e) {
        exception_msg = e.what();
      }
      CheckResult(file, example_id, exception_msg);
      std::cout << "file [" << file << "] Parquet Reading Complete" << std::endl;
    }
  }
}

void PrintDecryptionConfiguration(int configuration) {
  std::cout << "\n\nDecryption configuration ";
  if (configuration == 1) {
    std::cout << "1: \n\nDecrypt using key retriever that holds"
                 " the keys of two encrypted columns and the footer key."
              << std::endl;
  } else if (configuration == 2) {
    std::cout << "2: \n\nDecrypt using key retriever that holds"
                 " the keys of two encrypted columns and the footer key. Pass aad_prefix."
              << std::endl;
  } else if (configuration == 3) {
    std::cout << "3: \n\nDecrypt using explicit column and footer keys." << std::endl;
  } else {
    std::cout << "Unknown configuration" << std::endl;
    exit(-1);
  }
  std::cout << std::endl;
}

// Check that the decryption result is as expected.
void CheckResult(std::string file, int example_id, std::string exception_msg) {
  int encryption_configuration_number;
  std::regex r("tester([0-9]+)\\.parquet.encrypted");
  std::smatch m;
  std::regex_search(file, m, r);
  if (m.size() == 0) {
    std::cerr
        << "Error: Error parsing filename to extract encryption configuration number. "
        << std::endl;
  }
  std::string encryption_configuration_number_str = m.str(1);
  encryption_configuration_number = atoi(encryption_configuration_number_str.c_str());
  if (encryption_configuration_number < 1 || encryption_configuration_number > 6) {
    std::cerr << "Error: Unknown encryption configuration number. " << std::endl;
  }

  int decryption_configuration_number = example_id + 1;

  // Encryption_configuration number five contains aad_prefix and
  // disable_aad_prefix_storage.
  // An exception is expected to be thrown if the file is not decrypted with aad_prefix.
  if (encryption_configuration_number == 5) {
    if (decryption_configuration_number == 1 || decryption_configuration_number == 3) {
      std::size_t found = exception_msg.find("AAD");
      if (found == std::string::npos)
        std::cout << "Error: Expecting AAD related exception.";
      return;
    }
  }
  // Decryption configuration number two contains aad_prefix. An exception is expected to
  // be thrown if the file was not encrypted with the same aad_prefix.
  if (decryption_configuration_number == 2) {
    if (encryption_configuration_number != 5 && encryption_configuration_number != 4) {
      std::size_t found = exception_msg.find("AAD");
      if (found == std::string::npos) {
        std::cout << "Error: Expecting AAD related exception." << std::endl;
      }
      return;
    }
  }
  if (!exception_msg.empty())
    std::cout << "Error: Unexpected exception was thrown." << exception_msg;
}

bool FileNameEndsWith(std::string file_name, std::string suffix) {
  std::string::size_type idx = file_name.find_first_of('.');

  if (idx != std::string::npos) {
    std::string extension = file_name.substr(idx + 1);
    if (extension.compare(suffix) == 0) return true;
  }
  return false;
}

int main(int argc, char** argv) {
  enum Operation { write, read };
  std::string root_path;
  Operation operation = write;
  if (argc < 3) {
    std::cout << "Usage: encryption-reader-writer-all-crypto-options <read/write> "
                 "<Path-to-parquet-files>"
              << std::endl;
    exit(1);
  }
  root_path = argv[1];
  if (root_path.compare("read") == 0) {
    operation = read;
  }

  root_path = argv[2];
  std::cout << "Root path is: " << root_path << std::endl;

  if (operation == write) {
    InteropTestWriteEncryptedParquetFiles(root_path);
  } else {
    InteropTestReadEncryptedParquetFiles(root_path);
  }
  return 0;
}
