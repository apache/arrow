#include <arch.h>
#include <memory.h>
#include <stdint.h>

void FULL_NAME(sum_uint64)(uint64_t buf[], size_t len, uint64_t *res) {
    uint64_t acc = 0;
    for(int i = 0; i < len; i++) {
        acc += buf[i];
    }
    *res = acc;
}