/* is_registry.h — Information Schema Registry C API */
#pragma once
#ifndef SVDB_IS_REGISTRY_H
#define SVDB_IS_REGISTRY_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque registry handle */
typedef struct svdb_is_registry_s svdb_is_registry_t;

/* Column info for information_schema.columns */
typedef struct {
    const char* column_name;
    const char* table_name;
    const char* table_schema;
    const char* data_type;
    int is_nullable;      /* 1 = YES, 0 = NO */
    const char* column_default;
    int ordinal_position;
} svdb_is_column_info_t;

/* Table info for information_schema.tables */
typedef struct {
    const char* table_name;
    const char* table_schema;
    const char* table_type;  /* "BASE TABLE" or "VIEW" */
} svdb_is_table_info_t;

/* View info for information_schema.views */
typedef struct {
    const char* table_name;
    const char* table_schema;
    const char* view_definition;
} svdb_is_view_info_t;

/* Constraint info for information_schema.table_constraints */
typedef struct {
    const char* constraint_name;
    const char* table_name;
    const char* table_schema;
    const char* constraint_type;  /* "PRIMARY KEY", "UNIQUE", "CHECK", "FOREIGN KEY" */
} svdb_is_constraint_info_t;

/* Referential constraint info */
typedef struct {
    const char* constraint_name;
    const char* unique_constraint_schema;
    const char* unique_constraint_name;
} svdb_is_referential_info_t;

/* Result set for queries */
typedef struct {
    void* columns;      /* svdb_is_column_info_t* array */
    int num_columns;
    void* tables;       /* svdb_is_table_info_t* array */
    int num_tables;
    void* views;        /* svdb_is_view_info_t* array */
    int num_views;
} svdb_is_result_t;

/* Create registry */
svdb_is_registry_t* svdb_is_registry_create(void* btree_handle);

/* Destroy registry */
void svdb_is_registry_destroy(svdb_is_registry_t* reg);

/* Query columns view */
int svdb_is_query_columns(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result);

/* Query tables view */
int svdb_is_query_tables(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result);

/* Query views view */
int svdb_is_query_views(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result);

/* Query constraints view */
int svdb_is_query_constraints(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result);

/* Query referential constraints view */
int svdb_is_query_referential(svdb_is_registry_t* reg, const char* schema, const char* table_name, svdb_is_result_t* result);

/* Check if table is information_schema table */
int svdb_is_information_schema_table(const char* table_name);

/* Free result */
void svdb_is_result_free(svdb_is_result_t* result);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_IS_REGISTRY_H */
