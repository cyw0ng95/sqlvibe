#ifndef SVDB_VM_CURSOR_H
#define SVDB_VM_CURSOR_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SVDB_MAX_CURSORS 256

typedef struct {
    int     id;
    int     table_id;
    char    table_name[256];
    int64_t row_id;
    int     eof;
    int     index;      /* current row index; -1 = before first */
    int64_t index_key;
    int     num_rows;   /* total rows in the table (for iteration) */
    int     valid;      /* 1 if this cursor slot is open */
} svdb_cursor_t;

typedef void* svdb_cursor_array_t;

/* Create an array capable of holding SVDB_MAX_CURSORS cursors */
svdb_cursor_array_t svdb_cursor_array_create(void);

/* Destroy and free all resources */
void svdb_cursor_array_destroy(svdb_cursor_array_t ca);

/* Open next available cursor slot for table_id; returns cursor ID or -1 */
int svdb_cursor_array_open(svdb_cursor_array_t ca, int table_id);

/* Open a cursor by table name and row count; returns cursor ID or -1 */
int svdb_cursor_array_open_table(svdb_cursor_array_t ca,
                                  const char*         table_name,
                                  int                 num_rows);

/* Open (or re-open) a specific cursor slot */
void svdb_cursor_array_open_at_id(svdb_cursor_array_t ca,
                                   int                 cursor_id,
                                   const char*         table_name,
                                   int                 num_rows);

/* Close a cursor slot */
void svdb_cursor_array_close(svdb_cursor_array_t ca, int cursor_id);

/* Close all open cursors */
void svdb_cursor_array_reset(svdb_cursor_array_t ca);

/* Advance cursor; returns 1 if advanced, 0 if EOF */
int svdb_cursor_array_next(svdb_cursor_array_t ca, int cursor_id);

/* Copy cursor state into out_cursor; returns 1 if valid, 0 if invalid/EOF */
int svdb_cursor_get(svdb_cursor_array_t ca,
                    int                 cursor_id,
                    svdb_cursor_t*      out_cursor);

void svdb_cursor_set_eof(svdb_cursor_array_t ca, int cursor_id, int eof);
void svdb_cursor_set_index(svdb_cursor_array_t ca, int cursor_id, int idx);
void svdb_cursor_set_rowid(svdb_cursor_array_t ca, int cursor_id, int64_t row_id);

/* Return count of currently open cursors */
int svdb_cursor_array_get_count(svdb_cursor_array_t ca);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_CURSOR_H */
