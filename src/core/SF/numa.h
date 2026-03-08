/* numa.h — NUMA awareness utilities for memory allocation and thread binding
 *
 * NUMA (Non-Uniform Memory Access) systems have multiple memory domains with
 * different access latencies. Proper NUMA awareness can improve performance
 * by:
 *   - Allocating memory on the local NUMA node
 *   - Binding worker threads to specific NUMA nodes
 *   - Using interleaved allocation for shared data
 */
#ifndef SVDB_SF_NUMA_H
#define SVDB_SF_NUMA_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Maximum number of NUMA nodes supported */
#define SVDB_NUMA_MAX_NODES     64

/* NUMA allocation policies */
typedef enum {
    SVDB_NUMA_POLICY_DEFAULT,      /* Allocate on the default node */
    SVDB_NUMA_POLICY_LOCAL,        /* Allocate on the current thread's node */
    SVDB_NUMA_POLICY_INTERLEAVED,  /* Interleave across all nodes */
    SVDB_NUMA_POLICY_PREFERRED     /* Allocate on the preferred node */
} svdb_numa_policy_t;

/* NUMA node information */
typedef struct {
    int node_id;
    uint64_t total_memory;
    uint64_t free_memory;
    int cpu_count;
    uint32_t cpu_mask[SVDB_NUMA_MAX_NODES / 32];  /* CPUs belonging to this node */
} svdb_numa_node_info_t;

/* NUMA topology */
typedef struct {
    int num_nodes;
    int num_cpus;
    svdb_numa_node_info_t nodes[SVDB_NUMA_MAX_NODES];
} svdb_numa_topology_t;

/* ============================================================================
 * NUMA Detection
 * ============================================================================ */

/*
 * Initialize NUMA support. Must be called before other NUMA functions.
 * Returns 1 if NUMA is available, 0 if not (or single-node system).
 */
int svdb_numa_init(void);

/*
 * Check if NUMA is available on this system.
 */
int svdb_numa_available(void);

/*
 * Get the NUMA topology.
 * Returns 0 on success, -1 if NUMA is not available.
 */
int svdb_numa_get_topology(svdb_numa_topology_t* topo);

/*
 * Get the number of NUMA nodes.
 */
int svdb_numa_node_count(void);

/*
 * Get the number of CPUs.
 */
int svdb_numa_cpu_count(void);

/*
 * Get the NUMA node for the current thread.
 * Returns -1 if NUMA is not available.
 */
int svdb_numa_current_node(void);

/*
 * Get the preferred NUMA node for a given CPU.
 */
int svdb_numa_cpu_to_node(int cpu);

/*
 * Get the memory size of a NUMA node.
 */
uint64_t svdb_numa_node_memory(int node);

/*
 * Get the free memory of a NUMA node.
 */
uint64_t svdb_numa_node_free_memory(int node);

/* ============================================================================
 * Thread Binding
 * ============================================================================ */

/*
 * Bind the current thread to a specific NUMA node.
 * This sets both the CPU affinity and memory policy.
 * Returns 0 on success, -1 on failure.
 */
int svdb_numa_bind_to_node(int node);

/*
 * Bind the current thread to specific CPUs.
 * cpu_mask is a bitmask of CPUs.
 * Returns 0 on success, -1 on failure.
 */
int svdb_numa_bind_to_cpus(const uint32_t* cpu_mask, int mask_size);

/*
 * Set the memory allocation policy for the current thread.
 * Returns 0 on success, -1 on failure.
 */
int svdb_numa_set_policy(svdb_numa_policy_t policy, int preferred_node);

/*
 * Get the current memory allocation policy.
 */
svdb_numa_policy_t svdb_numa_get_policy(int* preferred_node);

/* ============================================================================
 * NUMA-Aware Memory Allocation
 * ============================================================================ */

/*
 * Allocate memory on a specific NUMA node.
 * Returns NULL on failure.
 */
void* svdb_numa_alloc_on_node(size_t size, int node);

/*
 * Allocate memory on the local NUMA node.
 * Returns NULL on failure.
 */
void* svdb_numa_alloc_local(size_t size);

/*
 * Allocate memory interleaved across all NUMA nodes.
 * Good for shared data structures accessed by multiple threads.
 * Returns NULL on failure.
 */
void* svdb_numa_alloc_interleaved(size_t size);

/*
 * Free NUMA-allocated memory.
 */
void svdb_numa_free(void* ptr, size_t size);

/*
 * Reallocate NUMA memory (may change node if resized).
 */
void* svdb_numa_realloc(void* ptr, size_t old_size, size_t new_size, int node);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/*
 * Get a recommended NUMA node for a new worker thread.
 * Uses round-robin distribution across nodes.
 */
int svdb_numa_get_worker_node(int worker_index);

/*
 * Get the CPUs for a given NUMA node.
 * Returns the number of CPUs, or -1 on error.
 */
int svdb_numa_get_node_cpus(int node, uint32_t* cpu_mask, int mask_size);

/*
 * Print NUMA topology (for debugging).
 */
void svdb_numa_print_topology(void);

/*
 * Check if a memory address is on a specific NUMA node.
 * Returns the node number, or -1 if not found.
 */
int svdb_numa_get_address_node(void* addr);

#ifdef __cplusplus
}
#endif

#endif // SVDB_SF_NUMA_H