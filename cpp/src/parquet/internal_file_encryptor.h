#ifndef INTERNAL_FILE_ENCRYPTOR_H
#define INTERNAL_FILE_ENCRYPTOR_H

namespace parquet_encryption {
  class AesEncryptor;
}

namespace parquet {

class FileEncryptionProperties;

class Encryptor {
 public:
  Encryptor(parquet_encryption::AesEncryptor* aes_encryptor,
            const std::string& key, const std::string& file_aad,
            const std::string& aad);
  const std::string& fileAAD() { return file_aad_; }
  void aad(const std::string& aad) { aad_ = aad; }

  int CiphertextSizeDelta();
  int Encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* ciphertext);

 private:
  parquet_encryption::AesEncryptor* aes_encryptor_;
  std::string key_;
  std::string file_aad_;
  std::string aad_;
};

class InternalFileEncryptor {
 public:
  explicit InternalFileEncryptor(FileEncryptionProperties* propperties);

  std::shared_ptr<Encryptor> GetFooterEncryptor();
  std::shared_ptr<Encryptor> GetFooterSigningEncryptor();
  std::shared_ptr<Encryptor> GetColumnMetaEncryptor(const std::shared_ptr<schema::ColumnPath>& column_path);
  std::shared_ptr<Encryptor> GetColumnDataEncryptor(const std::shared_ptr<schema::ColumnPath>& column_path);

 private:
  FileEncryptionProperties* properties_;

  std::unique_ptr<parquet_encryption::AesEncryptor> meta_encryptor_128_;
  std::unique_ptr<parquet_encryption::AesEncryptor> meta_encryptor_196_;
  std::unique_ptr<parquet_encryption::AesEncryptor> meta_encryptor_256_;
  std::unique_ptr<parquet_encryption::AesEncryptor> data_encryptor_128_;
  std::unique_ptr<parquet_encryption::AesEncryptor> data_encryptor_196_;
  std::unique_ptr<parquet_encryption::AesEncryptor> data_encryptor_256_;

  std::shared_ptr<Encryptor> GetColumnEncryptor(
      const std::shared_ptr<schema::ColumnPath>& column_path,
      bool metadata);

  parquet_encryption::AesEncryptor* GetMetaAesEncryptor(ParquetCipher::type algorithm,
                                                        size_t key_len);
  parquet_encryption::AesEncryptor* GetDataAesEncryptor(ParquetCipher::type algorithm,
                                                        size_t key_len);
};

}

#endif // INTERNAL_FILE_ENCRYPTORS_H