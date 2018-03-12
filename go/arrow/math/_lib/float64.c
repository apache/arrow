#include <arch.h>
#include <memory.h>

void FULL_NAME(sum_float64)(double buf[], size_t len, double *res) {
    double acc = 0.0;
    for(int i = 0; i < len; i++) {
        acc += buf[i];
    }
    *res = acc;
}