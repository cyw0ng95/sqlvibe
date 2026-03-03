#include "svdb.h"
#include "svdb_types.h"
#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <sys/stat.h>

static bool path_accessible(const char *path) {
    /* ":memory:" is always valid */
    if (strcmp(path, ":memory:") == 0) return true;
    /* Check that the parent directory exists and is writable */
    std::string p(path);
    size_t slash = p.rfind('/');
    std::string dir = (slash == std::string::npos) ? "." : p.substr(0, slash);
    if (dir.empty()) dir = ".";
    struct stat st;
    return stat(dir.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

extern "C" {

svdb_code_t svdb_open(const char *path, svdb_db_t **db) {
    if (!path || !db) return SVDB_ERR;
    if (!path_accessible(path)) return SVDB_ERR;
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
