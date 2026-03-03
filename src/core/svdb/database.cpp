#include "svdb.h"
#include "svdb_types.h"
#include <cstring>
#include <cstdlib>

extern "C" {

svdb_code_t svdb_open(const char *path, svdb_db_t **db) {
    if (!path || !db) return SVDB_ERR;
    svdb_db_t *d = new (std::nothrow) svdb_db_t();
    if (!d) return SVDB_NOMEM;
    d->path = path;
    *db = d;
    return SVDB_OK;
}

svdb_code_t svdb_close(svdb_db_t *db) {
    if (!db) return SVDB_ERR;
    delete db;
    return SVDB_OK;
}

const char *svdb_errmsg(svdb_db_t *db) {
    if (!db) return "";
    return db->last_error.c_str();
}

const char *svdb_version(void) {
    return "0.11.2";
}

int svdb_version_number(void) {
    return 112;
}

} /* extern "C" */
