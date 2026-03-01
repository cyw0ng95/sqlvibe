#include "vm_dispatch.h"
#include <cstdint>

extern "C" {

int svdb_dispatch_simd_level(void) {
#if defined(__AVX2__)
    return 3;
#elif defined(__AVX__)
    return 2;
#elif defined(__SSE4_1__)
    return 1;
#else
    return 0;
#endif
}

int svdb_dispatch_is_direct_threaded(void) {
    // C++ compiled dispatch table is always available
    return 1;
}

int svdb_dispatch_arith_int64(
    int            op,
    const int64_t* a,
    const int64_t* b,
    int64_t*       results,
    size_t         count
) {
    switch (op) {
    case 0: // add
        for (size_t i = 0; i < count; ++i) results[i] = a[i] + b[i];
        break;
    case 1: // sub
        for (size_t i = 0; i < count; ++i) results[i] = a[i] - b[i];
        break;
    case 2: // mul
        for (size_t i = 0; i < count; ++i) results[i] = a[i] * b[i];
        break;
    case 3: // div
        for (size_t i = 0; i < count; ++i) {
            if (b[i] == 0) return -1;
            results[i] = a[i] / b[i];
        }
        break;
    case 4: // rem
        for (size_t i = 0; i < count; ++i) {
            if (b[i] == 0) return -1;
            results[i] = a[i] % b[i];
        }
        break;
    default:
        return -2;
    }
    return 0;
}

void svdb_dispatch_arith_float64(
    int           op,
    const double* a,
    const double* b,
    double*       results,
    size_t        count
) {
    switch (op) {
    case 0:
        for (size_t i = 0; i < count; ++i) results[i] = a[i] + b[i];
        break;
    case 1:
        for (size_t i = 0; i < count; ++i) results[i] = a[i] - b[i];
        break;
    case 2:
        for (size_t i = 0; i < count; ++i) results[i] = a[i] * b[i];
        break;
    case 3:
        for (size_t i = 0; i < count; ++i) results[i] = (b[i] != 0.0) ? a[i] / b[i] : 0.0;
        break;
    }
}

} // extern "C"
