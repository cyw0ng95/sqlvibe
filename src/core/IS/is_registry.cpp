/* is_registry.cpp — Information Schema Registry Implementation */
#include "is_registry.h"
#include "schema.h"
#include <stdlib.h>
#include <string.h>
#include <string>
#include <vector>
#include <algorithm>
#include <cctype>

struct svdb_is_registry_s {
    void* btree_handle;
    svdb::is::Schema* schema;
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
    svdb_is_registry_t* reg = (svdb_is_registry_t*)calloc(1, sizeof(svdb_is_registry_t));
    if (!reg) return nullptr;
    
    reg->btree_handle = btree_handle;
    reg->schema = new svdb::is::Schema();
    
    return reg;
}

void svdb_is_registry_destroy(svdb_is_registry_t* reg) {
    if (!reg) return;
    
    if (reg->schema) {
        delete reg->schema;
    }
    
    free(reg);
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
