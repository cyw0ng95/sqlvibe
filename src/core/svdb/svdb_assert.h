/* svdb_assert.h — Linux kernel-style debug assertions for the svdb engine.
 *
 * When SVDB_BUILD_DEBUG is defined (build.sh -d), assertions are active and
 * abort the process on violation, printing file/line/function diagnostics.
 * In release builds every macro compiles away to nothing.
 *
 * Macros:
 *   svdb_assert(cond)               — abort if cond is false
 *   svdb_assert_msg(cond, fmt, ...) — abort with printf-style message
 *   SVDB_BUG_ON(cond)               — abort if cond is true (like kernel BUG_ON)
 *   SVDB_WARN_ON(cond)              — print warning (no abort) if cond is true
 *   SVDB_BUG()                      — unconditional abort (unreachable path)
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

/* SVDB_BUG_ON(cond) — abort if condition is true (kernel BUG_ON style) */
#define SVDB_BUG_ON(cond)                                                   \
    do {                                                                    \
        if (__builtin_expect((cond), 0))                                    \
            svdb_assert_fail("BUG_ON(" #cond ")", __FILE__, __LINE__, __func__); \
    } while (0)

/* SVDB_WARN_ON(cond) — print warning but continue */
#define SVDB_WARN_ON(cond) \
    svdb_warn_on_helper((cond), "WARN_ON(" #cond ")", __FILE__, __LINE__, __func__)

/* SVDB_BUG() — unconditional abort for unreachable code paths */
#define SVDB_BUG()                                                          \
    svdb_assert_fail("BUG() unreachable path", __FILE__, __LINE__, __func__)

#else /* !SVDB_BUILD_DEBUG — all macros compile away */

#define svdb_assert(cond)               ((void)0)
#define svdb_assert_msg(cond, fmt, ...) ((void)0)
#define SVDB_BUG_ON(cond)               ((void)0)
#define SVDB_WARN_ON(cond)              (false)
#define SVDB_BUG()                      ((void)0)

#endif /* SVDB_BUILD_DEBUG */
