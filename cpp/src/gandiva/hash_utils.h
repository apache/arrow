#ifndef ARROW_SRC_HASH_UTILS_H_
#define ARROW_SRC_HASH_UTILS_H_

#include <cstdlib>
#include "gandiva/visibility.h"

namespace gandiva {
  class GANDIVA_EXPORT HashUtils {
   public:
    static inline char *hash_using_SHA256(int64_t context, const void *message, size_t message_length);

   private:
    static inline void clean_char_array(char *buffer);
  };
};

#endif //ARROW_SRC_HASH_UTILS_H_
