/* svdb_assert.h — Linux kernel-style debug assertions for the svdb engine.
 *
 * Lives in src/core/SF/ (shared foundations).
 *
 * When SVDB_BUILD_DEBUG is defined (build.sh -d), assertions are active and
 * abort the process on violation, printing file/line/function diagnostics.
 * In release builds every macro compiles away to nothing.
 *
 * Macros (uppercase, no prefix — kernel style for external callers):
 *   BUG_ON(cond)               — abort if cond is true
 *   WARN_ON(cond)              — print warning (no abort) if cond is true
 *   BUG()                      — unconditional abort (unreachable path)
 *
 * Internal/verbose forms (keep svdb_ prefix for in-file use):
 *   svdb_assert(cond)
 *   svdb_assert_msg(cond, fmt, ...)
 */
#pragma once

#ifdef SVDB_BUILD_DEBUG

#include <cstdio>
#include <cstdlib>

/* Internal helper: print location and abort */
[[noreturn]] static inline void svdb_assert_fail(
        const char *expr, const char *file, int line, const char *func) {
    std::fprintf(stderr,
        "\nSVDB ASSERTION FAILED: %s\n"
        "  at %s:%d  %s\n",
        expr, file, line, func);
    std::abort();
}

/* Internal helper: print warning without aborting */
static inline bool svdb_warn_on_helper(
        bool cond, const char *expr, const char *file, int line, const char *func) {
    if (cond) {
        std::fprintf(stderr,
            "\nSVDB WARNING: %s\n"
            "  at %s:%d  %s\n",
            expr, file, line, func);
    }
    return cond;
}

/* svdb_assert(cond) — abort if condition is false */
#define svdb_assert(cond)                                                   \
    do {                                                                    \
        if (__builtin_expect(!(cond), 0))                                   \
            svdb_assert_fail(#cond, __FILE__, __LINE__, __func__);          \
    } while (0)

/* svdb_assert_msg(cond, fmt, ...) — abort with custom message */
#define svdb_assert_msg(cond, fmt, ...)                                     \
    do {                                                                    \
        if (__builtin_expect(!(cond), 0)) {                                 \
            std::fprintf(stderr,                                            \
                "\nSVDB ASSERTION FAILED: %s\n"                            \
                "  at %s:%d  %s\n"                                         \
                "  detail: " fmt "\n",                                     \
                #cond, __FILE__, __LINE__, __func__, ##__VA_ARGS__);        \
            std::abort();                                                   \
        }                                                                   \
    } while (0)

/* BUG_ON(cond) — abort if condition is true (kernel BUG_ON style) */
#define BUG_ON(cond)                                                        \
    do {                                                                    \
        if (__builtin_expect((cond), 0))                                    \
            svdb_assert_fail("BUG_ON(" #cond ")", __FILE__, __LINE__, __func__); \
    } while (0)

/* WARN_ON(cond) — print warning but continue */
#define WARN_ON(cond) \
    svdb_warn_on_helper((cond), "WARN_ON(" #cond ")", __FILE__, __LINE__, __func__)

/* BUG() — unconditional abort for unreachable code paths */
#define BUG()                                                               \
    svdb_assert_fail("BUG() unreachable path", __FILE__, __LINE__, __func__)

/* Compatibility aliases (old SVDB_ prefix) */
#define SVDB_BUG_ON(cond)  BUG_ON(cond)
#define SVDB_WARN_ON(cond) WARN_ON(cond)
#define SVDB_BUG()         BUG()

#else /* !SVDB_BUILD_DEBUG — all macros compile away */

#define svdb_assert(cond)               ((void)0)
#define svdb_assert_msg(cond, fmt, ...) ((void)0)
#define BUG_ON(cond)                    ((void)0)
#define WARN_ON(cond)                   ((void)0)
#define BUG()                           ((void)0)
#define SVDB_BUG_ON(cond)               ((void)0)
#define SVDB_WARN_ON(cond)              ((void)0)
#define SVDB_BUG()                      ((void)0)

#endif /* SVDB_BUILD_DEBUG */
