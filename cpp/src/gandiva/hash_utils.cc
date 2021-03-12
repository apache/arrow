#include <cstring>
#include "gandiva/hash_utils.h"
#include "openssl/evp.h"
#include "execution_context.h"

namespace gandiva {
  const char * HashUtils::hash_using_SHA256(int64_t context, const void *message, size_t message_length) {
    if(message == nullptr){
      HashUtils::error_message(context, "A null value was given to be hashed.");
      return "";
    }

    EVP_MD_CTX *md_ctx = EVP_MD_CTX_new();

    EVP_DigestInit_ex(md_ctx, EVP_sha256(), nullptr);

    EVP_DigestUpdate(md_ctx, message, message_length);

    int sha256_hash_size = EVP_MD_size(EVP_sha256());

    auto* result = static_cast<unsigned char*>(OPENSSL_malloc(sha256_hash_size));

    unsigned int result_length;

    EVP_DigestFinal_ex(md_ctx, result, &result_length);

    char* hex_buffer = new char[4];
    char* result_buffer = new char[65];

    clean_char_array(hex_buffer);
    clean_char_array(result_buffer);

    for (unsigned int j = 0; j < result_length; j++) {
      unsigned char hex_number = result[j];
      sprintf(hex_buffer, "%02x", hex_number);

      strcat(result_buffer, hex_buffer);
    }

    result_buffer[64] = '\0';

    // free the resources to avoid memory leaks
    EVP_MD_CTX_free(md_ctx);
    delete[] hex_buffer;
    free(result);

    return result_buffer;
  }
  const char * HashUtils::hash_using_SHA128(int64_t context, const void *message, size_t message_length) {
    if(message == nullptr){
      HashUtils::error_message(context, "A null value was given to be hashed.");
      return "";
    }

    EVP_MD_CTX *md_ctx = EVP_MD_CTX_new();

    EVP_DigestInit_ex(md_ctx, EVP_sha1(), nullptr);

    EVP_DigestUpdate(md_ctx, message, message_length);

    int sha128_hash_size = EVP_MD_size(EVP_sha1());

    auto* result = static_cast<unsigned char*>(OPENSSL_malloc(sha128_hash_size));

    unsigned int result_length;

    EVP_DigestFinal_ex(md_ctx, result, &result_length);

    char* hex_buffer = new char[4];
    char* result_buffer = new char[65];

    clean_char_array(hex_buffer);
    clean_char_array(result_buffer);

    for (unsigned int j = 0; j < result_length; j++) {
      unsigned char hex_number = result[j];
      sprintf(hex_buffer, "%02x", hex_number);

      strcat(result_buffer, hex_buffer);
    }

    result_buffer[64] = '\0';

    // free the resources to avoid memory leaks
    EVP_MD_CTX_free(md_ctx);
    delete[] hex_buffer;
    free(result);

    return result_buffer;
  }


  uint64_t HashUtils::double_to_long(double value) {
    uint64_t result;
    memcpy(&result, &value, sizeof(result));
    return result;
  }

  void HashUtils::clean_char_array(char *buffer) {
    buffer[0] = '\0';
  }

  void HashUtils::error_message(int64_t context_ptr, char const *err_msg){
    auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
    context->set_error_msg(err_msg);
  }
}  // namespace gandiva