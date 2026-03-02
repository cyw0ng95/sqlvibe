#ifndef SVDB_CG_PLAN_CACHE_H
#define SVDB_CG_PLAN_CACHE_H

#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct svdb_cg_cache svdb_cg_cache_t;

svdb_cg_cache_t* svdb_cg_cache_create(void);
void             svdb_cg_cache_free(svdb_cg_cache_t* cache);

void        svdb_cg_cache_put_json(svdb_cg_cache_t* cache, const char* sql, const char* json_data, size_t json_len);
const char* svdb_cg_cache_get_json(svdb_cg_cache_t* cache, const char* sql, size_t* out_len);
void        svdb_cg_cache_erase(svdb_cg_cache_t* cache);
/*
 * Thread-safe copy variant: copies the JSON string into a caller-provided
 * buffer under the mutex.  Returns true on hit, false on miss.
 * On hit with buf_size > json_len the buffer is NUL-terminated.
 */
bool svdb_cg_cache_copy_json(
    svdb_cg_cache_t* cache,
    const char*      sql,
    char*            buf,
    size_t           buf_size,
    size_t*          out_len
);

size_t      svdb_cg_cache_count(svdb_cg_cache_t* cache);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_CG_PLAN_CACHE_H */
