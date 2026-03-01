#include "math.h"
#include <cstring>
#include <random>

#ifdef __SSE__
#include <immintrin.h>
#endif

// Basic math functions
extern "C" {

int64_t svdb_abs_int(int64_t v) {
    return v < 0 ? -v : v;
}

double svdb_abs_double(double v) {
    return v < 0.0 ? -v : v;
}

double svdb_ceil(double v) {
    return std::ceil(v);
}

double svdb_floor(double v) {
    return std::floor(v);
}

double svdb_round(double v, int decimals) {
    double multiplier = std::pow(10.0, decimals);
    if (decimals < 0) {
        multiplier = std::pow(10.0, -decimals);
        return std::round(v / multiplier) * multiplier;
    }
    return std::round(v * multiplier) / multiplier;
}

double svdb_power(double base, double exp) {
    return std::pow(base, exp);
}

double svdb_sqrt(double v) {
    return std::sqrt(v);
}

double svdb_mod(double a, double b) {
    return std::fmod(a, b);
}

double svdb_exp(double v) {
    return std::exp(v);
}

double svdb_ln(double v) {
    return std::log(v);
}

double svdb_log(double base, double v) {
    return std::log(v) / std::log(base);
}

double svdb_log2(double v) {
    return std::log2(v);
}

double svdb_log10(double v) {
    return std::log10(v);
}

int64_t svdb_sign_int(int64_t v) {
    return (v > 0) - (v < 0);
}

double svdb_sign_double(double v) {
    return (v > 0.0) - (v < 0.0);
}

// Random functions
static std::mt19937_64 rng(std::random_device{}());

int64_t svdb_random() {
    return static_cast<int64_t>(rng());
}

void* svdb_randomblob(int64_t n) {
    if (n <= 0) {
        return nullptr;
    }
    char* buffer = static_cast<char*>(std::malloc(n));
    if (!buffer) {
        return nullptr;
    }
    for (int64_t i = 0; i < n; i++) {
        buffer[i] = static_cast<char>(rng() & 0xFF);
    }
    return buffer;
}

void* svdb_zeroblob(int64_t n) {
    if (n <= 0) {
        return nullptr;
    }
    char* buffer = static_cast<char*>(std::calloc(n, 1));
    return buffer;
}

// SIMD batch operations
#ifdef __AVX__
void svdb_batch_abs_double(double* data, int64_t n) {
    __m256d sign_mask = _mm256_set1_pd(-0.0);
    for (int64_t i = 0; i < n; i += 4) {
        __m256d vals = _mm256_loadu_pd(&data[i]);
        __m256d abs_vals = _mm256_andnot_pd(sign_mask, vals);
        _mm256_storeu_pd(&data[i], abs_vals);
    }
    // Handle remainder
    for (int64_t i = (n / 4) * 4; i < n; i++) {
        data[i] = std::fabs(data[i]);
    }
}

void svdb_batch_add_double(const double* a, const double* b, double* out, int64_t n) {
    for (int64_t i = 0; i < n; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vr = _mm256_add_pd(va, vb);
        _mm256_storeu_pd(&out[i], vr);
    }
    // Handle remainder
    for (int64_t i = (n / 4) * 4; i < n; i++) {
        out[i] = a[i] + b[i];
    }
}
#else
void svdb_batch_abs_double(double* data, int64_t n) {
    for (int64_t i = 0; i < n; i++) {
        data[i] = std::fabs(data[i]);
    }
}

void svdb_batch_add_double(const double* a, const double* b, double* out, int64_t n) {
    for (int64_t i = 0; i < n; i++) {
        out[i] = a[i] + b[i];
    }
}
#endif

} // extern "C"
