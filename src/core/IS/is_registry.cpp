/* is_registry.cpp — Information Schema Registry Implementation */
#include "is_registry.h"
#include "schema.h"
#include <stdlib.h>
#include <string.h>
#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <unordered_map>
#include <mutex>
#include <atomic>

struct svdb_is_registry_s {
    void* btree_handle;
    svdb::is::Schema* schema;
    
    /* Table metadata cache for fast COUNT(*) 
     * v0.11.5: Thread-safe with atomic operations
     */
    std::unordered_map<std::string, svdb_is_table_metadata_t> table_metadata;
    std::mutex metadata_mutex;
    uint64_t global_modify_counter;
    uint32_t global_schema_version;
};

/* Helper: case-insensitive string comparison */
static bool ci_equal(const char* a, const char* b) {
    if (!a || !b) return false;
    while (*a && *b) {
        if (std::tolower((unsigned char)*a) != std::tolower((unsigned char)*b)) {
            return false;
        }
        a++;
        b++;
    }
    return *a == *b;
}

/* Helper: convert to lowercase */
static std::string to_lower(const char* str) {
    if (!str) return "";
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

svdb_is_registry_t* svdb_is_registry_create(void* btree_handle) {
    try {
        svdb_is_registry_t* reg = new svdb_is_registry_t();
        if (!reg) return nullptr;

        reg->btree_handle = btree_handle;
        reg->schema = new svdb::is::Schema();
        reg->global_modify_counter = 0;
        reg->global_schema_version = 0;
        /* table_metadata, metadata_mutex are properly constructed by new */

        return reg;
    } catch (...) {
        return nullptr;
    }
}

void svdb_is_registry_destroy(svdb_is_registry_t* reg) {
    if (!reg) return;
    
    if (reg->schema) {
        delete reg->schema;
    }
    
    delete reg;
}

int svdb_is_information_schema_table(const char* table_name) {
    if (!table_name) return 0;
    
    std::string lower_name = to_lower(table_name);
    
    if (lower_name.find("information_schema.") != 0) {
        return 0;
    }
    
    std::string view_name = lower_name.substr(19);  /* Remove "information_schema." */
    
    const char* valid_views[] = {
        "columns", "tables", "views", "table_constraints",
        "referential_constraints", nullptr
    };
    
    for (int i = 0; valid_views[i]; i++) {
        if (view_name == valid_views[i]) {
            return 1;
        }
    }
    
    return 0;
}

int svdb_is_query_columns(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result) {
    if (!reg || !result) return -1;
    
    memset(result, 0, sizeof(*result));
    
    /* Filter by schema - only support "main" */
    if (schema && *schema && !ci_equal(schema, "main")) {
        return 0;  /* Empty result */
    }
    
    /* TODO: Query actual column metadata from btree */
    /* For now, return empty result */
    
    return 0;
}

int svdb_is_query_tables(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result) {
    if (!reg || !result) return -1;
    
    memset(result, 0, sizeof(*result));
    
    /* Filter by schema - only support "main" */
    if (schema && *schema && !ci_equal(schema, "main")) {
        return 0;
    }
    
    /* TODO: Query actual table metadata from btree */
    /* For now, return empty result */
    
    return 0;
}

int svdb_is_query_views(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result) {
    if (!reg || !result) return -1;
    
    memset(result, 0, sizeof(*result));
    
    /* Filter by schema - only support "main" */
    if (schema && *schema && !ci_equal(schema, "main")) {
        return 0;
    }
    
    /* TODO: Query actual view metadata from btree */
    /* For now, return empty result */
    
    return 0;
}

int svdb_is_query_constraints(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result) {
    if (!reg || !result) return -1;
    
    memset(result, 0, sizeof(*result));
    
    /* Filter by schema - only support "main" */
    if (schema && *schema && !ci_equal(schema, "main")) {
        return 0;
    }
    
    /* TODO: Query actual constraint metadata from btree */
    /* For now, return empty result */
    
    return 0;
}

int svdb_is_query_referential(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result) {
    if (!reg || !result) return -1;
    
    memset(result, 0, sizeof(*result));
    
    /* Filter by schema - only support "main" */
    if (schema && *schema && !ci_equal(schema, "main")) {
        return 0;
    }
    
    /* TODO: Query actual FK metadata from btree */
    /* For now, return empty result */
    
    return 0;
}

/* ── Table metadata cache for fast COUNT(*) ────────────────────────────────── */

int svdb_is_get_table_metadata(svdb_is_registry_t* reg, const char* table_name, svdb_is_table_metadata_t* metadata) {
    if (!reg || !table_name || !metadata) return -1;
    
    /* Thread-safe read */
    std::lock_guard<std::mutex> lock(reg->metadata_mutex);
    
    std::string key = to_lower(table_name);
    auto it = reg->table_metadata.find(key);
    
    if (it != reg->table_metadata.end() && it->second.valid) {
        *metadata = it->second;
        return 0;
    }
    
    metadata->row_count = 0;
    metadata->row_count_version = 0;
    metadata->schema_version = 0;
    metadata->last_modified_counter = 0;
    metadata->last_modified_txn = 0;
    metadata->valid = 0;
    return -1;
}

void svdb_is_set_table_metadata(svdb_is_registry_t* reg, const char* table_name, uint64_t row_count) {
    if (!reg || !table_name) return;
    
    if (!reg->schema) return;
    
    std::lock_guard<std::mutex> lock(reg->metadata_mutex);
    
    std::string key = to_lower(table_name);
    svdb_is_table_metadata_t meta;
    meta.row_count = row_count;
    meta.row_count_version = 1;
    meta.schema_version = reg->global_schema_version;
    meta.last_modified_counter = ++reg->global_modify_counter;
    meta.last_modified_txn = 0;
    meta.valid = 1;
    
    reg->table_metadata[key] = meta;
}

void svdb_is_invalidate_table_metadata(svdb_is_registry_t* reg, const char* table_name) {
    if (!reg || !table_name) return;
    
    if (!reg->schema) return;
    
    std::lock_guard<std::mutex> lock(reg->metadata_mutex);
    
    std::string key = to_lower(table_name);
    auto it = reg->table_metadata.find(key);

    if (it != reg->table_metadata.end()) {
        it->second.valid = 0;
    }
}

void svdb_is_update_table_metadata_delta(svdb_is_registry_t* reg, const char* table_name, int64_t delta) {
    if (!reg || !table_name) return;
    
    if (!reg->schema) return;
    
    std::lock_guard<std::mutex> lock(reg->metadata_mutex);
    
    std::string key = to_lower(table_name);
    auto it = reg->table_metadata.find(key);

    if (it != reg->table_metadata.end() && it->second.valid) {
        int64_t new_count = (int64_t)it->second.row_count + delta;
        it->second.row_count = (new_count < 0) ? 0 : (uint64_t)new_count;
        it->second.last_modified_counter = ++reg->global_modify_counter;
        it->second.row_count_version++;
    } else {
        // Metadata doesn't exist or is invalid - create it with the delta as initial count
        // This handles the case where inserts happen before a SELECT sets the metadata
        svdb_is_table_metadata_t meta;
        int64_t new_count = delta;
        meta.row_count = (new_count < 0) ? 0 : (uint64_t)new_count;
        meta.row_count_version = 1;
        meta.schema_version = reg->global_schema_version;
        meta.last_modified_counter = ++reg->global_modify_counter;
        meta.last_modified_txn = 0;
        meta.valid = 1;
        
        reg->table_metadata[key] = meta;
    }
}

void svdb_is_result_free(svdb_is_result_t* result) {
    if (!result) return;
    
    /* Free columns */
    if (result->columns && result->num_columns > 0) {
        svdb_is_column_info_t* cols = (svdb_is_column_info_t*)result->columns;
        for (int i = 0; i < result->num_columns; i++) {
            /* String pointers are owned by registry, don't free */
        }
        free(result->columns);
    }
    
    /* Free tables */
    if (result->tables && result->num_tables > 0) {
        svdb_is_table_info_t* tables = (svdb_is_table_info_t*)result->tables;
        for (int i = 0; i < result->num_tables; i++) {
            /* String pointers are owned by registry, don't free */
        }
        free(result->tables);
    }
    
    /* Free views */
    if (result->views && result->num_views > 0) {
        svdb_is_view_info_t* views = (svdb_is_view_info_t*)result->views;
        for (int i = 0; i < result->num_views; i++) {
            /* String pointers are owned by registry, don't free */
        }
        free(result->views);
    }
    
    result->columns = nullptr;
    result->tables = nullptr;
    result->views = nullptr;
    result->num_columns = 0;
    result->num_tables = 0;
    result->num_views = 0;
}
