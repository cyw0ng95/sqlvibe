#ifndef SVDB_QP_DAG_H
#define SVDB_QP_DAG_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque DAG handle */
typedef void* svdb_dag_t;

/* Create an empty DAG */
svdb_dag_t svdb_dag_create(void);

/* Destroy a DAG and free all resources */
void svdb_dag_destroy(svdb_dag_t dag);

/* Add a node with the given operation name; returns node ID (>= 0) */
int svdb_dag_add_node(svdb_dag_t dag, const char* op_name);

/* Add a directed edge from src_id to dst_id */
void svdb_dag_add_edge(svdb_dag_t dag, int src_id, int dst_id);

/* Return total number of nodes */
int svdb_dag_get_node_count(svdb_dag_t dag);

/* Copy node name into out_buf; returns name length, or -1 on error */
int svdb_dag_get_node_name(svdb_dag_t  dag,
                            int         node_id,
                            char*       out_buf,
                            int         out_buf_size);

/* Returns 1 if all predecessor nodes are marked done */
int svdb_dag_is_ready(svdb_dag_t dag, int node_id);

/* Mark a node as completed */
void svdb_dag_mark_done(svdb_dag_t dag, int node_id);

/* Reset a node's done flag (for re-execution) */
void svdb_dag_mark_undone(svdb_dag_t dag, int node_id);

/*
 * Fill out_ids with IDs of nodes that are ready (all predecessors done
 * and node itself not yet done).  Returns count written.
 */
int svdb_dag_get_ready_nodes(svdb_dag_t dag, int* out_ids, int max_out);

/*
 * Fill out_ids with node IDs in topological order (Kahn's algorithm).
 * Returns number of nodes written, or -1 if the graph has a cycle.
 */
int svdb_dag_topological_sort(svdb_dag_t dag, int* out_ids, int max_out);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_DAG_H */
