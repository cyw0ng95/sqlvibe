#ifndef SVDB_EXT_MATH_H
#define SVDB_EXT_MATH_H

#include <stdint.h>
#include <math.h>
#include <stdlib.h>

#define SVDB_PI 3.14159265358979323846

#ifdef __cplusplus
extern "C" {
#endif

// Basic math functions
int64_t svdb_abs_int(int64_t v);
double svdb_abs_double(double v);

double svdb_ceil(double v);
double svdb_floor(double v);
double svdb_round(double v, int decimals);

double svdb_power(double base, double exp);
double svdb_sqrt(double v);
double svdb_mod(double a, double b);

double svdb_exp(double v);
double svdb_ln(double v);
double svdb_log(double base, double v);
double svdb_log2(double v);
double svdb_log10(double v);

int64_t svdb_sign_int(int64_t v);
double svdb_sign_double(double v);

// Random functions
int64_t svdb_random();
void* svdb_randomblob(int64_t n);
void* svdb_zeroblob(int64_t n);

// SIMD batch operations
void svdb_batch_abs_double(double* data, int64_t n);
void svdb_batch_add_double(const double* a, const double* b, double* out, int64_t n);

#ifdef __cplusplus
}
#endif

#endif // SVDB_EXT_MATH_H
