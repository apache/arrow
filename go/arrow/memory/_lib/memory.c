#include "arch.h"
#include <memory.h>

void FULL_NAME(memset)(void *buf, size_t len, int v) {
    char *bytes = buf;
    char *end   = buf+len;
    const char val = v;
    while (bytes < end) {
        *bytes++ = val;
    }
}