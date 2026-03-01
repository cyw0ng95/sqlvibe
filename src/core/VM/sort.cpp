#include "sort.h"
#include <cstring>
#include <algorithm>
#include <vector>

extern "C" {

// Quick sort for int64
static void quick_sort_int64(int64_t* data, int64_t low, int64_t high) {
    if (low < high) {
        // Median of three pivot
        int64_t mid = (low + high) / 2;
        if (data[mid] < data[low]) std::swap(data[low], data[mid]);
        if (data[high] < data[low]) std::swap(data[low], data[high]);
        if (data[mid] < data[high]) std::swap(data[mid], data[high]);
        
        int64_t pivot = data[high];
        int64_t i = low - 1;
        
        for (int64_t j = low; j < high; j++) {
            if (data[j] <= pivot) {
                i++;
                std::swap(data[i], data[j]);
            }
        }
        std::swap(data[i + 1], data[high]);
        
        quick_sort_int64(data, low, i);
        quick_sort_int64(data, i + 2, high);
    }
}

void svdb_sort_int64(int64_t* data, size_t count) {
    if (count <= 1) return;
    quick_sort_int64(data, 0, static_cast<int64_t>(count) - 1);
}

void svdb_sort_int64_with_indices(int64_t* data, int64_t* indices, size_t count) {
    if (count <= 1) return;
    
    // Initialize indices if not provided
    std::vector<int64_t> idx(count);
    for (size_t i = 0; i < count; i++) {
        idx[i] = indices ? indices[i] : static_cast<int64_t>(i);
    }
    
    // Sort with indices
    std::sort(idx.begin(), idx.end(), [&data](int64_t a, int64_t b) {
        return data[a] < data[b];
    });
    
    // Rearrange data
    std::vector<int64_t> temp(count);
    for (size_t i = 0; i < count; i++) {
        temp[i] = data[idx[i]];
    }
    std::memcpy(data, temp.data(), count * sizeof(int64_t));
    
    if (indices) {
        for (size_t i = 0; i < count; i++) {
            indices[i] = idx[i];
        }
    }
}

// Radix sort for uint64
void svdb_radix_sort_uint64(uint64_t* data, size_t count) {
    if (count <= 1) return;
    
    std::vector<uint64_t> temp(count);
    
    for (int shift = 0; shift < 64; shift += 8) {
        size_t counts[256] = {0};
        
        // Count occurrences
        for (size_t i = 0; i < count; i++) {
            counts[(data[i] >> shift) & 0xFF]++;
        }
        
        // Cumulative count
        size_t total = 0;
        for (int i = 0; i < 256; i++) {
            size_t count_i = counts[i];
            counts[i] = total;
            total += count_i;
        }
        
        // Place in sorted order
        for (size_t i = 0; i < count; i++) {
            size_t idx = counts[(data[i] >> shift) & 0xFF]++;
            temp[idx] = data[i];
        }
        
        std::memcpy(data, temp.data(), count * sizeof(uint64_t));
    }
}

// String comparison for sorting
static int compare_strings(const char* a, const char* b) {
    return std::strcmp(a, b);
}

void svdb_sort_strings(const char** data, size_t count) {
    if (count <= 1) return;
    std::sort(data, data + count, compare_strings);
}

void svdb_sort_strings_with_indices(const char** data, int64_t* indices, size_t count) {
    if (count <= 1) return;
    
    std::vector<int64_t> idx(count);
    for (size_t i = 0; i < count; i++) {
        idx[i] = indices ? indices[i] : static_cast<int64_t>(i);
    }
    
    std::sort(idx.begin(), idx.end(), [&data](int64_t a, int64_t b) {
        return compare_strings(data[a], data[b]) < 0;
    });
    
    std::vector<const char*> temp(count);
    for (size_t i = 0; i < count; i++) {
        temp[i] = data[idx[i]];
    }
    std::memcpy(data, temp.data(), count * sizeof(const char*));
    
    if (indices) {
        for (size_t i = 0; i < count; i++) {
            indices[i] = idx[i];
        }
    }
}

// Byte slice comparison
static int compare_bytes(const uint8_t* a, size_t a_len, const uint8_t* b, size_t b_len) {
    size_t min_len = (a_len < b_len) ? a_len : b_len;
    int cmp = std::memcmp(a, b, min_len);
    if (cmp != 0) return cmp;
    return (a_len < b_len) ? -1 : (a_len > b_len) ? 1 : 0;
}

void svdb_sort_bytes(
    const uint8_t** data,
    const size_t* lengths,
    size_t count
) {
    if (count <= 1) return;
    
    std::vector<size_t> idx(count);
    for (size_t i = 0; i < count; i++) {
        idx[i] = i;
    }
    
    std::sort(idx.begin(), idx.end(), [&data, &lengths](size_t a, size_t b) {
        return compare_bytes(data[a], lengths[a], data[b], lengths[b]) < 0;
    });
    
    std::vector<const uint8_t*> temp_data(count);
    std::vector<size_t> temp_len(count);
    for (size_t i = 0; i < count; i++) {
        temp_data[i] = data[idx[i]];
        temp_len[i] = lengths[idx[i]];
    }
    std::memcpy(data, temp_data.data(), count * sizeof(const uint8_t*));
    std::memcpy(const_cast<size_t*>(lengths), temp_len.data(), count * sizeof(size_t));
}

void svdb_sort_bytes_with_indices(
    const uint8_t** data,
    const size_t* lengths,
    int64_t* indices,
    size_t count
) {
    if (count <= 1) return;
    
    std::vector<int64_t> idx(count);
    for (size_t i = 0; i < count; i++) {
        idx[i] = indices ? indices[i] : static_cast<int64_t>(i);
    }
    
    std::sort(idx.begin(), idx.end(), [&data, &lengths](int64_t a, int64_t b) {
        return compare_bytes(data[a], lengths[a], data[b], lengths[b]) < 0;
    });
    
    std::vector<const uint8_t*> temp_data(count);
    std::vector<size_t> temp_len(count);
    std::vector<int64_t> temp_idx(count);
    for (size_t i = 0; i < count; i++) {
        temp_data[i] = data[idx[i]];
        temp_len[i] = lengths[idx[i]];
        temp_idx[i] = idx[i];
    }
    std::memcpy(data, temp_data.data(), count * sizeof(const uint8_t*));
    std::memcpy(const_cast<size_t*>(lengths), temp_len.data(), count * sizeof(size_t));
    
    if (indices) {
        for (size_t i = 0; i < count; i++) {
            indices[i] = temp_idx[i];
        }
    }
}

void svdb_argsort_int64(
    const int64_t* data,
    size_t count,
    size_t* result
) {
    std::vector<size_t> idx(count);
    for (size_t i = 0; i < count; i++) {
        idx[i] = i;
    }
    
    std::sort(idx.begin(), idx.end(), [&data](size_t a, size_t b) {
        return data[a] < data[b];
    });
    
    std::memcpy(result, idx.data(), count * sizeof(size_t));
}

} // extern "C"
