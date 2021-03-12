#ifndef ARROW_SRC_HASH_UTILS_H_
#define ARROW_SRC_HASH_UTILS_H_

#include <cstdlib>
#include <cstdint>
#include "gandiva/visibility.h"

namespace gandiva {
  class GANDIVA_EXPORT HashUtils {
   public:
    static const char * hash_using_SHA256(int64_t context, const void *message, size_t message_length);

    static uint64_t double_to_long(double value);
   private:
    static inline void clean_char_array(char *buffer);

    static void error_message(int64_t context_ptr, char const *err_msg);
  };
}

#endif //ARROW_SRC_HASH_UTILS_H_
