#ifndef SVDB_EXT_JSON_H
#define SVDB_EXT_JSON_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// JSON validation and parsing
int svdb_json_validate(const char* json_str);
char* svdb_json_minify(const char* json_str);
char* svdb_json_pretty(const char* json_str);

// JSON type functions
char* svdb_json_type(const char* json_str, const char* path);
int64_t svdb_json_length(const char* json_str, const char* path);

// JSON extraction
char* svdb_json_extract(const char* json_str, const char* path);
char* svdb_json_extract_multi(const char* json_str, const char** paths, int n_paths);

// JSON creation
char* svdb_json_array(const char** values, int n_values);
char* svdb_json_object(const char** keys, const char** values, int n_pairs);

// JSON modification
char* svdb_json_set(const char* json_str, const char** path_value_pairs, int n_pairs);
char* svdb_json_replace(const char* json_str, const char** path_value_pairs, int n_pairs);
char* svdb_json_remove(const char* json_str, const char** paths, int n_paths);

// JSON utilities
char* svdb_json_quote(const char* value);
char* svdb_json_keys(const char* json_str, const char* path);
int svdb_json_patch(char* dest, size_t dest_size, const char* target, const char* patch);

// Memory management - caller must free returned strings
void svdb_json_free(char* ptr);

#ifdef __cplusplus
}
#endif

#endif // SVDB_EXT_JSON_H
