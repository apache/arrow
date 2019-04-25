#ifndef INTERNAL_FILE_DECRYPTOR_H
#define INTERNAL_FILE_DECRYPTOR_H

#include <map>
#include "parquet/schema.h"

namespace parquet_encryption {
  class AesDecryptor;
  class AesEncryptor;
}

namespace parquet {

class FileDecryptionProperties;

class FooterSigningEncryptor {
 public:
  FooterSigningEncryptor(ParquetCipher::type algorithm, const std::string& key,
                         const std::string& file_aad, const std::string& aad);
  int CiphertextSizeDelta();
  int SignedFooterEncrypt(const uint8_t* footer, int footer_len,
                          uint8_t* nonce, uint8_t* encrypted_footer);

 private:
  ParquetCipher::type algorithm_;
  std::string key_;
  std::string file_aad_;
  std::string aad_;

  std::shared_ptr<parquet_encryption::AesEncryptor> aes_encryptor_;
};

class Decryptor {
 public:
  Decryptor(parquet_encryption::AesDecryptor* decryptor,
            const std::string& key, const std::string& file_aad,
            const std::string& aad);

  const std::string& fileAAD() const { return file_aad_; }
  void aad(const std::string& aad) { aad_ = aad; }

  int CiphertextSizeDelta();
  int Decrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* plaintext);

 private:
  parquet_encryption::AesDecryptor* aes_decryptor_;
  std::string key_;
  std::string file_aad_;
  std::string aad_;
};

class InternalFileDecryptor {
 public:
  explicit InternalFileDecryptor(FileDecryptionProperties* propperties);

  void file_aad(const std::string& file_aad) { file_aad_ = file_aad; }
  std::string& file_aad() { return file_aad_; }

  std::shared_ptr<FooterSigningEncryptor> GetFooterSigningEncryptor(
      ParquetCipher::type algorithm,
      const std::string& footer_key_metadata);

  std::shared_ptr<Decryptor> GetFooterDecryptor(
      ParquetCipher::type algorithm,
      const std::string& footer_key_metadata);
  std::shared_ptr<Decryptor> GetFooterDecryptorForColumnMeta(
      ParquetCipher::type algorithm,
      const std::string& footer_key_metadata,
      const std::string& aad);
  std::shared_ptr<Decryptor> GetFooterDecryptorForColumnData(
      ParquetCipher::type algorithm,
      const std::string& footer_key_metadata,
      const std::string& aad);
  std::shared_ptr<Decryptor> GetColumnMetaDecryptor(
      std::shared_ptr<schema::ColumnPath> column_path,
      ParquetCipher::type algorithm,
      const std::string& column_key_metadata,
      const std::string& aad);
  std::shared_ptr<Decryptor> GetColumnDataDecryptor(
      std::shared_ptr<schema::ColumnPath> column_path,
      ParquetCipher::type algorithm,
      const std::string& column_key_metadata,
      const std::string& aad);

 private:
  FileDecryptionProperties* properties_;
  std::string file_aad_;
  std::shared_ptr<std::map<std::shared_ptr<schema::ColumnPath>,
                  std::string, parquet::schema::ColumnPath::CmpColumnPath>> column_map_;

  std::unique_ptr<parquet_encryption::AesDecryptor> meta_decryptor_128_;
  std::unique_ptr<parquet_encryption::AesDecryptor> meta_decryptor_196_;
  std::unique_ptr<parquet_encryption::AesDecryptor> meta_decryptor_256_;
  std::unique_ptr<parquet_encryption::AesDecryptor> data_decryptor_128_;
  std::unique_ptr<parquet_encryption::AesDecryptor> data_decryptor_196_;
  std::unique_ptr<parquet_encryption::AesDecryptor> data_decryptor_256_;

  std::shared_ptr<Decryptor> GetFooterDecryptor(
      ParquetCipher::type algorithm,
      const std::string& footer_key_metadata,
      const std::string& aad, bool metadata);
  std::shared_ptr<Decryptor> GetColumnDecryptor(
      std::shared_ptr<schema::ColumnPath> column_path,
      ParquetCipher::type algorithm,
      const std::string& column_key_metadata,
      const std::string& aad, bool metadata = false);

  parquet_encryption::AesDecryptor* GetMetaAesDecryptor(ParquetCipher::type algorithm,
                                                        size_t key_size);
  parquet_encryption::AesDecryptor* GetDataAesDecryptor(ParquetCipher::type algorithm,
                                                        size_t key_size);
};

}

#endif // INTERNAL_FILE_ENCRYPTORS_H