#include <arch.h>
#include <memory.h>
#include <stdint.h>

void FULL_NAME(sum_int64)(int64_t buf[], size_t len, int64_t *res) {
    int64_t acc = 0;
    for(int i = 0; i < len; i++) {
        acc += buf[i];
    }
    *res = acc;
}