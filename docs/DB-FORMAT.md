# DB-FORMAT.md — SQLVIBE v1.0.0 Binary Database Format

This document describes the on-disk binary format used by sqlvibe starting with v0.8.0.
The format is **not compatible** with SQLite.

---

## File Structure Overview

```
┌────────────────────────────────────┐
│  Header          (256 bytes)       │
├────────────────────────────────────┤
│  Schema Section  (variable)        │  ← JSON blob
├────────────────────────────────────┤
│  Column 0 Data   (variable)        │  ← null bitmap + values
│  Column 1 Data   (variable)        │
│  ...                               │
├────────────────────────────────────┤
│  Footer          (32 bytes)        │
└────────────────────────────────────┘
```

All multi-byte integers are stored **little-endian**.

---

## Header (256 bytes)

The header occupies the first 256 bytes of the file.

| Offset | Size | Type      | Field             | Description                              |
|--------|------|-----------|-------------------|------------------------------------------|
| 0      | 8    | byte[8]   | Magic             | `"SQLVIBE\x01"` — identifies file type  |
| 8      | 4    | uint32 LE | VersionMajor      | Format major version (currently `1`)     |
| 12     | 4    | uint32 LE | VersionMinor      | Format minor version (currently `0`)     |
| 16     | 4    | uint32 LE | VersionPatch      | Format patch version (currently `0`)     |
| 20     | 4    | uint32 LE | Flags             | Reserved feature flags (currently `0`)   |
| 24     | 4    | uint32 LE | SchemaOffset      | Byte offset of the Schema Section (`256`)|
| 28     | 4    | uint32 LE | SchemaLength      | Byte length of the Schema JSON           |
| 32     | 4    | uint32 LE | ColumnCount       | Number of columns                        |
| 36     | 4    | uint32 LE | RowCount          | Number of live rows                      |
| 40     | 4    | uint32 LE | IndexCount        | Number of indexes stored (currently `0`) |
| 44     | 4    | uint32 LE | CreatedAt         | Unix timestamp of file creation          |
| 48     | 4    | uint32 LE | ModifiedAt        | Unix timestamp of last modification      |
| 52     | 4    | uint32 LE | CompressionType   | Compression algorithm (`0` = none)       |
| 56     | 4    | uint32 LE | PageSize          | Reserved page size hint (`0` = n/a)      |
| 60     | 188  | byte[188] | Reserved          | Zero-filled; reserved for future use     |
| 248    | 8    | uint64 LE | HeaderCRC64       | CRC64/ECMA of header bytes 0–247         |

### Header CRC

`HeaderCRC64` is the CRC64 (ECMA polynomial) of bytes `[0, 248)` of the header.
Readers must verify this checksum before trusting any other header field.

---

## Schema Section (JSON)

Immediately follows the header at offset `SchemaOffset` (always `256`).
The section is exactly `SchemaLength` bytes of UTF-8 JSON.

### Mandatory fields

```json
{
  "column_names": ["col1", "col2", "..."],
  "column_types": [1, 3, 2]
}
```

| Field          | Type          | Description                                       |
|----------------|---------------|---------------------------------------------------|
| `column_names` | `[]string`    | Ordered list of column names                      |
| `column_types` | `[]int`       | Ordered list of column type codes (see below)     |

### Optional fields

Additional fields (e.g. `"tables"`, `"version"`) are preserved verbatim and returned
unchanged by `ReadDatabase`.

### Column type codes

| Code | Go constant   | Description       |
|------|---------------|-------------------|
| 0    | `TypeNull`    | No type / unknown |
| 1    | `TypeInt`     | 64-bit integer    |
| 2    | `TypeFloat`   | 64-bit float      |
| 3    | `TypeString`  | UTF-8 string      |
| 4    | `TypeBytes`   | Raw byte array    |
| 5    | `TypeBool`    | Boolean           |

---

## Column Data Section

Starts immediately after the last byte of the Schema Section and extends until
the Footer. Columns are written in the order they appear in `column_names`.

### Per-column layout

```
┌──────────────────────────────┐
│ Null Bitmap                  │  ceil(RowCount / 8) bytes
├──────────────────────────────┤
│ Value[0]                     │
│ Value[1]                     │  RowCount values, one per row
│ ...                          │  (zero-value written for null slots)
└──────────────────────────────┘
```

#### Null Bitmap

A packed bit array of `ceil(RowCount / 8)` bytes.
Bit `i % 8` of byte `i / 8` is **1** when row `i` holds a NULL value.

#### Per-type value encoding

| Type      | Bytes per slot | Encoding                                          |
|-----------|----------------|---------------------------------------------------|
| TypeInt   | 8              | `int64` little-endian                             |
| TypeFloat | 8              | `float64` IEEE 754, little-endian bits             |
| TypeBool  | 8              | `0` = false, non-zero = true (little-endian)      |
| TypeString| 4 + len(s)     | `uint32 LE` length prefix, then UTF-8 bytes       |
| TypeBytes | 4 + len(b)     | `uint32 LE` length prefix, then raw bytes         |
| TypeNull  | 0              | No bytes written                                  |

Null slots always write the **zero value** for their type (e.g. `0` for integers,
empty length prefix for strings). The Null Bitmap is the authoritative source
of NULL information.

---

## Index Section

Currently unused (`IndexCount` = 0). Reserved for future B-Tree or bitmap index
serialization. When present, indexes will follow the column data section.

---

## Footer (32 bytes)

The footer is the last 32 bytes of the file.

| Offset | Size | Type      | Field       | Description                                       |
|--------|------|-----------|-------------|---------------------------------------------------|
| 0      | 8    | byte[8]   | Magic       | `"SQLVIB\xFE\x01"` — footer sentinel             |
| 8      | 8    | uint64 LE | FileCRC     | CRC64/ECMA of all file bytes before the footer    |
| 16     | 4    | uint32 LE | RowCount    | Redundant copy of header RowCount                 |
| 20     | 4    | uint32 LE | ColumnCount | Redundant copy of header ColumnCount              |
| 24     | 8    | byte[8]   | Reserved    | Zero-filled; reserved for future use              |

### File CRC

`FileCRC` is the CRC64 (ECMA polynomial) of bytes `[0, fileSize − 32)`.
Readers must verify this checksum first, before checking the header CRC,
to detect any corruption in any section of the file.

---

## Version Scheme

| Field         | Meaning                                                                 |
|---------------|-------------------------------------------------------------------------|
| VersionMajor  | Incremented on breaking format changes. Old readers must reject higher. |
| VersionMinor  | Incremented on backward-compatible additions.                           |
| VersionPatch  | Incremented on bug-fix / documentation changes.                         |

The current format version is **1.0.0**.

---

## Reading Algorithm

1. Read the last 32 bytes as the Footer.
2. Validate `footer.Magic == "SQLVIB\xFE\x01"`.
3. Compute `CRC64(file[0 : fileSize-32])` and compare to `footer.FileCRC`.
4. Read the first 256 bytes as the Header.
5. Validate `header.Magic == "SQLVIBE\x01"`.
6. Compute `CRC64(header[0:248])` and compare to `header.HeaderCRC64`.
7. Read `header.SchemaLength` bytes at offset `header.SchemaOffset` as JSON.
8. Parse `column_names` and `column_types` from the JSON.
9. Starting at offset `header.SchemaOffset + header.SchemaLength`, read each
   column in order: null bitmap (`ceil(RowCount/8)` bytes) then `RowCount` values.
10. Reconstruct rows and return.

---

## Writing Algorithm

1. Scan all live rows from the store.
2. Encode each column to its binary representation (bitmap + values).
3. Build the Schema JSON with `column_names` and `column_types` (and any extra fields).
4. Fill the Header: set offsets, lengths, counts, timestamps.
5. Compute `HeaderCRC64 = CRC64(header[0:248])` and store at offset 248.
6. Write: `[header][schema_json][col0_data][col1_data]...[footer]`.
7. Compute `FileCRC = CRC64(everything before footer)` and embed in footer.

---

## Compression

`CompressionType = 0` means no compression is applied. Future versions may define:

| Code | Algorithm |
|------|-----------|
| 0    | None      |
| 1    | LZ4       |
| 2    | Zstd      |
| 3    | Snappy    |
