#include "opt.h"

namespace svdb {
namespace sf {
namespace opt {
} // namespace opt
} // namespace sf
} // namespace svdb

// C-compatible wrapper functions
extern "C" {

void svdb_sf_vector_add_int64(int64_t* dst, const int64_t* a, const int64_t* b, size_t n) {
    svdb::sf::opt::VectorAddInt64(dst, a, b, n);
}

void svdb_sf_vector_sub_int64(int64_t* dst, const int64_t* a, const int64_t* b, size_t n) {
    svdb::sf::opt::VectorSubInt64(dst, a, b, n);
}

void svdb_sf_vector_mul_int64(int64_t* dst, const int64_t* a, const int64_t* b, size_t n) {
    svdb::sf::opt::VectorMulInt64(dst, a, b, n);
}

int64_t svdb_sf_vector_sum_int64(const int64_t* a, size_t n) {
    return svdb::sf::opt::VectorSumInt64(a, n);
}

void svdb_sf_vector_add_float64(double* dst, const double* a, const double* b, size_t n) {
    svdb::sf::opt::VectorAddFloat64(dst, a, b, n);
}

void svdb_sf_vector_sub_float64(double* dst, const double* a, const double* b, size_t n) {
    svdb::sf::opt::VectorSubFloat64(dst, a, b, n);
}

void svdb_sf_vector_mul_float64(double* dst, const double* a, const double* b, size_t n) {
    svdb::sf::opt::VectorMulFloat64(dst, a, b, n);
}

double svdb_sf_vector_sum_float64(const double* a, size_t n) {
    return svdb::sf::opt::VectorSumFloat64(a, n);
}

int64_t svdb_sf_vector_min_int64(const int64_t* a, size_t n) {
    return svdb::sf::opt::VectorMinInt64(a, n);
}

int64_t svdb_sf_vector_max_int64(const int64_t* a, size_t n) {
    return svdb::sf::opt::VectorMaxInt64(a, n);
}

double svdb_sf_vector_min_float64(const double* a, size_t n) {
    return svdb::sf::opt::VectorMinFloat64(a, n);
}

double svdb_sf_vector_max_float64(const double* a, size_t n) {
    return svdb::sf::opt::VectorMaxFloat64(a, n);
}

}
