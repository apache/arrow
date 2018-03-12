#undef FULL_NAME

#if  defined(__AVX2__)
    #define FULL_NAME(x) x##_avx2
#elif __SSE4_2__ == 1
    #define FULL_NAME(x) x##_sse4
#elif __SSE3__ == 1
    #define FULL_NAME(x) x##_sse3
#else
    #define FULL_NAME(x) x##_x86
#endif