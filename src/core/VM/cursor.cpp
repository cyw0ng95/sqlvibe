#include "cursor.h"
#include <cstring>
#include <cstdlib>
#include <vector>

/* ------------------------------------------------------------------ types */

struct CursorArray {
    svdb_cursor_t slots[SVDB_MAX_CURSORS];
    CursorArray() {
        memset(slots, 0, sizeof(slots));
        for (int i = 0; i < SVDB_MAX_CURSORS; ++i) {
            slots[i].id    = i;
            slots[i].index = -1;
            slots[i].valid = 0;
        }
    }
};

/* ------------------------------------------------------------------ helpers */

static CursorArray* ca_cast(svdb_cursor_array_t ca)
{
    return static_cast<CursorArray*>(ca);
}

static svdb_cursor_t* get_slot(CursorArray* ca, int id)
{
    if (!ca || id < 0 || id >= SVDB_MAX_CURSORS) return nullptr;
    return &ca->slots[id];
}

/* ------------------------------------------------------------------ API */

extern "C" {

svdb_cursor_array_t svdb_cursor_array_create(void)
{
    return new CursorArray();
}

void svdb_cursor_array_destroy(svdb_cursor_array_t ca)
{
    delete ca_cast(ca);
}

int svdb_cursor_array_open(svdb_cursor_array_t ca, int table_id)
{
    CursorArray* a = ca_cast(ca);
    if (!a) return -1;
    for (int i = 0; i < SVDB_MAX_CURSORS; ++i) {
        if (!a->slots[i].valid) {
            a->slots[i].table_id  = table_id;
            a->slots[i].table_name[0] = '\0';
            a->slots[i].row_id    = 0;
            a->slots[i].eof       = 0;
            a->slots[i].index     = -1;
            a->slots[i].index_key = 0;
            a->slots[i].num_rows  = 0;
            a->slots[i].valid     = 1;
            return i;
        }
    }
    return -1; /* no free slot */
}

int svdb_cursor_array_open_table(svdb_cursor_array_t ca,
                                  const char*         table_name,
                                  int                 num_rows)
{
    CursorArray* a = ca_cast(ca);
    if (!a) return -1;
    for (int i = 0; i < SVDB_MAX_CURSORS; ++i) {
        if (!a->slots[i].valid) {
            svdb_cursor_array_open_at_id(ca, i, table_name, num_rows);
            return i;
        }
    }
    return -1;
}

void svdb_cursor_array_open_at_id(svdb_cursor_array_t ca,
                                   int                 cursor_id,
                                   const char*         table_name,
                                   int                 num_rows)
{
    svdb_cursor_t* s = get_slot(ca_cast(ca), cursor_id);
    if (!s) return;
    s->table_id  = -1;
    s->row_id    = 0;
    s->eof       = 0;
    s->index     = -1;
    s->index_key = 0;
    s->num_rows  = num_rows;
    s->valid     = 1;
    if (table_name) {
        strncpy(s->table_name, table_name, sizeof(s->table_name) - 1);
        s->table_name[sizeof(s->table_name) - 1] = '\0';
    } else {
        s->table_name[0] = '\0';
    }
}

void svdb_cursor_array_close(svdb_cursor_array_t ca, int cursor_id)
{
    svdb_cursor_t* s = get_slot(ca_cast(ca), cursor_id);
    if (s) {
        memset(s, 0, sizeof(*s));
        s->id    = cursor_id;
        s->index = -1;
        s->valid = 0;
    }
}

void svdb_cursor_array_reset(svdb_cursor_array_t ca)
{
    CursorArray* a = ca_cast(ca);
    if (!a) return;
    for (int i = 0; i < SVDB_MAX_CURSORS; ++i) {
        a->slots[i].valid = 0;
        a->slots[i].index = -1;
        a->slots[i].eof   = 0;
    }
}

int svdb_cursor_array_next(svdb_cursor_array_t ca, int cursor_id)
{
    svdb_cursor_t* s = get_slot(ca_cast(ca), cursor_id);
    if (!s || !s->valid || s->eof) return 0;
    s->index++;
    if (s->num_rows > 0 && s->index >= s->num_rows) {
        s->eof = 1;
        return 0;
    }
    s->row_id = (int64_t)(s->index + 1);
    return 1;
}

int svdb_cursor_get(svdb_cursor_array_t ca,
                    int                 cursor_id,
                    svdb_cursor_t*      out_cursor)
{
    svdb_cursor_t* s = get_slot(ca_cast(ca), cursor_id);
    if (!s || !s->valid || s->eof) return 0;
    if (out_cursor) *out_cursor = *s;
    return 1;
}

void svdb_cursor_set_eof(svdb_cursor_array_t ca, int cursor_id, int eof)
{
    svdb_cursor_t* s = get_slot(ca_cast(ca), cursor_id);
    if (s) s->eof = eof;
}

void svdb_cursor_set_index(svdb_cursor_array_t ca, int cursor_id, int idx)
{
    svdb_cursor_t* s = get_slot(ca_cast(ca), cursor_id);
    if (s) s->index = idx;
}

void svdb_cursor_set_rowid(svdb_cursor_array_t ca, int cursor_id, int64_t row_id)
{
    svdb_cursor_t* s = get_slot(ca_cast(ca), cursor_id);
    if (s) s->row_id = row_id;
}

int svdb_cursor_array_get_count(svdb_cursor_array_t ca)
{
    CursorArray* a = ca_cast(ca);
    if (!a) return 0;
    int count = 0;
    for (int i = 0; i < SVDB_MAX_CURSORS; ++i)
        if (a->slots[i].valid) ++count;
    return count;
}

} /* extern "C" */
