#include "datetime.h"
#include <cstring>
#include <cstdio>

extern "C" {

/* Parse "YYYY-MM-DD" (optionally with " HH:MM:SS") into Julian Day Number. */
double svdb_julianday(const char* timestr, size_t len) {
    if (!timestr || len < 10) return 0.0;
    int Y = 0, M = 0, D = 0;
    if (sscanf(timestr, "%d-%d-%d", &Y, &M, &D) < 3) return 0.0;
    if (M <= 0 || M > 12 || D <= 0 || D > 31) return 0.0;

    /* Gregorian-to-Julian Day algorithm */
    int a = (14 - M) / 12;
    int y = Y + 4800 - a;
    int m = M + 12 * a - 3;
    double jdn = (double)(D + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045);

    /* Parse optional time component */
    int h = 0, mi = 0, s = 0;
    if (len > 10) {
        sscanf(timestr + 10, " %d:%d:%d", &h, &mi, &s);
    }
    double frac = (double)(h * 3600 + mi * 60 + s) / 86400.0;
    /* JDN starts at noon; adjust so midnight = 0.5 frac */
    return jdn - 0.5 + frac;
}

int64_t svdb_unixepoch(const char* timestr, size_t len) {
    if (!timestr || len < 10) return 0;
    int Y = 0, M = 0, D = 0, h = 0, mi = 0, s = 0;
    if (sscanf(timestr, "%d-%d-%d %d:%d:%d", &Y, &M, &D, &h, &mi, &s) < 3) {
        if (sscanf(timestr, "%d-%d-%d", &Y, &M, &D) < 3) return 0;
    }

    /* Days since Unix epoch using Julian Day */
    const double JD_UNIX = 2440587.5; /* Julian Day of 1970-01-01T00:00:00Z */
    double jd = svdb_julianday(timestr, len);
    /* Rebuild JD for date-only when time was missing */
    int a = (14 - M) / 12;
    int y = Y + 4800 - a;
    int m = M + 12 * a - 3;
    double jdn = (double)(D + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045);
    jd = jdn - 0.5 + (double)(h * 3600 + mi * 60 + s) / 86400.0;

    return (int64_t)((jd - JD_UNIX) * 86400.0 + 0.5);
}

void svdb_julianday_batch(
    const char** strs,
    size_t*      lens,
    double*      results,
    int*         ok,
    size_t       count
) {
    for (size_t i = 0; i < count; ++i) {
        if (!strs[i]) { results[i] = 0.0; ok[i] = 0; continue; }
        double v = svdb_julianday(strs[i], lens[i]);
        ok[i] = (v != 0.0) ? 1 : 0;
        results[i] = v;
    }
}

void svdb_unixepoch_batch(
    const char** strs,
    size_t*      lens,
    int64_t*     results,
    int*         ok,
    size_t       count
) {
    for (size_t i = 0; i < count; ++i) {
        if (!strs[i]) { results[i] = 0; ok[i] = 0; continue; }
        int64_t v = svdb_unixepoch(strs[i], lens[i]);
        ok[i] = 1;
        results[i] = v;
    }
}

} // extern "C"
