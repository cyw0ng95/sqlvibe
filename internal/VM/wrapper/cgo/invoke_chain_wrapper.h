#ifndef SVDB_INVOKE_CHAIN_WRAPPER_H
#define SVDB_INVOKE_CHAIN_WRAPPER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Phase 4.2: Invoke Chain Wrapper
 *
 * These thin wrapper functions sequence multiple C-level subsystem calls
 * into a single CGO boundary crossing, reducing Go<->CGO context switches.
 */

/* ── Phase 4.2: Invoke chain wrapper ────────────────────────────── */

/*
 * svdb_pipeline_hash_filter
 *
 * Sequences: hash(keys) → compare(keys, threshold) → write matching indices
 *
 * Combines svdb_xxhash64_batch + threshold comparison into one CGO call.
 * For each key[i], compute hash and include index i in out_indices[] when
 * hash(key[i]) % bucket_count == target_bucket.
 *
 * Returns number of matching indices written to out_indices.
 */
size_t svdb_pipeline_hash_filter(
    const uint8_t** keys,        /* input key pointers               */
    const size_t*   key_lens,    /* input key lengths                */
    size_t          count,       /* number of keys                   */
    uint64_t        seed,        /* hash seed                        */
    uint64_t        bucket_count,/* modulus for bucket selection     */
    uint64_t        target_bucket,/* bucket index to keep            */
    size_t*         out_indices  /* output: indices of matching keys */
);

/* ── Phase 4.3: Expression batch wrapper ────────────────────────── */

/*
 * svdb_batch_eval_compare_int64
 *
 * Sequences: compare_int64_batch → build bitmask
 *
 * For each pair (a[i], b[i]), applies the comparison op and sets
 * out_mask[i] = 1 when the predicate is true.
 *
 * op: 0=EQ  1=NE  2=LT  3=LE  4=GT  5=GE
 *
 * Returns number of rows that passed the filter.
 */
size_t svdb_batch_eval_compare_int64(
    const int64_t* a,       /* left operand array           */
    const int64_t* b,       /* right operand array          */
    size_t         count,   /* number of elements           */
    int            op,      /* comparison operator code     */
    uint8_t*       out_mask /* output: 1=pass, 0=fail       */
);

/*
 * svdb_batch_eval_compare_float64
 *
 * Same as svdb_batch_eval_compare_int64 but for double values.
 */
size_t svdb_batch_eval_compare_float64(
    const double* a,
    const double* b,
    size_t        count,
    int           op,
    uint8_t*      out_mask
);

/*
 * svdb_batch_arith_and_compare_int64
 *
 * Sequences: add_int64_batch → compare_int64_batch → bitmask
 *
 * Computes tmp[i] = a[i] + b[i], then out_mask[i] = (tmp[i] op threshold).
 * arith_op: 0=add  1=sub  2=mul
 * cmp_op:   0=EQ   1=NE   2=LT  3=LE  4=GT  5=GE
 *
 * Returns number of rows that passed.
 */
size_t svdb_batch_arith_and_compare_int64(
    const int64_t* a,
    const int64_t* b,
    size_t         count,
    int            arith_op,
    int64_t        threshold,
    int            cmp_op,
    uint8_t*       out_mask
);

/* ── Phase 4.4: Storage access wrapper ──────────────────────────── */

/*
 * svdb_scan_filter_int64
 *
 * Sequences: iterate column → compare each value against threshold →
 *            collect matching row indices.
 *
 * Used for simple WHERE col <op> literal scans without per-row CGO calls.
 *
 * op: 0=EQ  1=NE  2=LT  3=LE  4=GT  5=GE
 *
 * Returns number of matching indices written to out_indices.
 */
size_t svdb_scan_filter_int64(
    const int64_t* column,      /* column data array             */
    size_t         row_count,   /* number of rows                */
    int            op,          /* comparison operator           */
    int64_t        threshold,   /* comparison threshold          */
    size_t*        out_indices  /* output: matching row indices  */
);

/*
 * svdb_scan_filter_float64
 *
 * Same as svdb_scan_filter_int64 but for double columns.
 */
size_t svdb_scan_filter_float64(
    const double* column,
    size_t        row_count,
    int           op,
    double        threshold,
    size_t*       out_indices
);

/*
 * svdb_scan_aggregate_int64
 *
 * Sequences: scan column → filter → aggregate (sum/min/max/count)
 *
 * agg_op: 0=SUM  1=MIN  2=MAX  3=COUNT
 *
 * Returns number of rows that passed the filter; writes aggregate to *out_agg.
 */
size_t svdb_scan_aggregate_int64(
    const int64_t* column,
    size_t         row_count,
    int            filter_op,
    int64_t        filter_threshold,
    int            agg_op,
    int64_t*       out_agg
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_INVOKE_CHAIN_WRAPPER_H */
