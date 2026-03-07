/* numa.cpp — NUMA awareness utilities implementation */
#include "numa.h"
#include "../SF/svdb_assert.h"

#include <cstring>
#include <cstdlib>
#include <atomic>
#include <vector>
#include <mutex>

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/mman.h>

#ifdef __linux__
#include <sched.h>
#include <dirent.h>
#include <fstream>
#include <sstream>
#endif

extern "C" {

/* Global state */
static std::atomic<bool> g_numa_initialized(false);
static std::atomic<bool> g_numa_available(false);
static svdb_numa_topology_t g_numa_topology;
static std::mutex g_numa_mutex;
static std::atomic<int> g_next_worker_node(0);

#ifdef __linux__

/* Read a sysfs file and return its contents as a string */
static std::string read_sysfs(const char* path) {
    std::ifstream f(path);
    if (!f.is_open()) return "";
    std::string content;
    std::getline(f, content);
    return content;
}

/* Parse a CPU mask from sysfs format (hex with commas) */
static int parse_cpu_mask(const std::string& mask, uint32_t* out_mask, int mask_size) {
    if (mask.empty()) return 0;

    /* Remove commas */
    std::string clean_mask;
    for (char c : mask) {
        if (c != ',') clean_mask += c;
    }

    /* Parse hex values */
    int cpu = 0;
    for (int i = static_cast<int>(clean_mask.size()) - 1; i >= 0; i -= 8) {
        std::string chunk;
        for (int j = std::max(0, i - 7); j <= i; j++) {
            chunk += clean_mask[j];
        }

        try {
            uint32_t val = std::stoul(chunk, nullptr, 16);
            int idx = (clean_mask.size() - 1 - i) / 8;
            if (idx < mask_size) {
                out_mask[idx] = val;
            }

            /* Count set bits */
            for (int b = 0; b < 32 && cpu < SVDB_NUMA_MAX_NODES; b++) {
                if (val & (1u << b)) {
                    cpu++;
                }
            }
        } catch (...) {
            break;
        }
    }

    return cpu;
}

/* Detect NUMA topology from sysfs */
static int detect_numa_topology(svdb_numa_topology_t* topo) {
    memset(topo, 0, sizeof(*topo));

    /* Check if NUMA is available via /sys/devices/system/node */
    DIR* node_dir = opendir("/sys/devices/system/node");
    if (!node_dir) {
        return -1;
    }

    struct dirent* entry;
    while ((entry = readdir(node_dir)) != nullptr) {
        if (strncmp(entry->d_name, "node", 4) == 0) {
            int node_id = atoi(entry->d_name + 4);
            if (node_id >= 0 && node_id < SVDB_NUMA_MAX_NODES) {
                svdb_numa_node_info_t* node = &topo->nodes[topo->num_nodes];
                node->node_id = node_id;

                /* Read memory info */
                char meminfo_path[256];
                snprintf(meminfo_path, sizeof(meminfo_path),
                         "/sys/devices/system/node/node%d/meminfo", node_id);
                std::string meminfo = read_sysfs(meminfo_path);

                /* Parse memory values */
                size_t pos;
                if ((pos = meminfo.find("MemTotal:")) != std::string::npos) {
                    node->total_memory = std::stoull(meminfo.substr(pos + 9)) * 1024;
                }
                if ((pos = meminfo.find("MemFree:")) != std::string::npos) {
                    node->free_memory = std::stoull(meminfo.substr(pos + 8)) * 1024;
                }

                /* Read CPU mask */
                char cpulist_path[256];
                snprintf(cpulist_path, sizeof(cpulist_path),
                         "/sys/devices/system/node/node%d/cpulist", node_id);
                std::string cpulist = read_sysfs(cpulist_path);

                /* Parse CPU list (e.g., "0-3,8-11") */
                node->cpu_count = 0;
                if (!cpulist.empty()) {
                    char* p = const_cast<char*>(cpulist.c_str());
                    while (*p) {
                        int start = strtol(p, &p, 10);
                        int end = start;
                        if (*p == '-') {
                            p++;
                            end = strtol(p, &p, 10);
                        }
                        for (int cpu = start; cpu <= end; cpu++) {
                            if (cpu < SVDB_NUMA_MAX_NODES) {
                                node->cpu_mask[cpu / 32] |= (1u << (cpu % 32));
                                node->cpu_count++;
                                topo->num_cpus = std::max(topo->num_cpus, cpu + 1);
                            }
                        }
                        if (*p == ',') p++;
                    }
                }

                topo->num_nodes++;
            }
        }
    }

    closedir(node_dir);

    /* Sort nodes by node_id */
    for (int i = 0; i < topo->num_nodes - 1; i++) {
        for (int j = i + 1; j < topo->num_nodes; j++) {
            if (topo->nodes[j].node_id < topo->nodes[i].node_id) {
                svdb_numa_node_info_t tmp = topo->nodes[i];
                topo->nodes[i] = topo->nodes[j];
                topo->nodes[j] = tmp;
            }
        }
    }

    return (topo->num_nodes > 1) ? 0 : -1;
}

/* Get current CPU using getcpu syscall */
static int get_current_cpu(void) {
    unsigned int cpu, node;
    if (syscall(__NR_getcpu, &cpu, &node, nullptr) == 0) {
        return static_cast<int>(cpu);
    }
    return -1;
}

#endif // __linux__

/* ============================================================================
 * NUMA Detection
 * ============================================================================ */

int svdb_numa_init(void) {
    bool expected = false;
    if (!g_numa_initialized.compare_exchange_strong(expected, true)) {
        /* Already initialized */
        return g_numa_available ? 1 : 0;
    }

#ifdef __linux__
    if (detect_numa_topology(&g_numa_topology) == 0) {
        g_numa_available = true;
        return 1;
    }
#endif

    g_numa_available = false;
    return 0;
}

int svdb_numa_available(void) {
    return g_numa_available ? 1 : 0;
}

int svdb_numa_get_topology(svdb_numa_topology_t* topo) {
    if (!g_numa_available || !topo) return -1;

    std::lock_guard<std::mutex> lock(g_numa_mutex);
    *topo = g_numa_topology;
    return 0;
}

int svdb_numa_node_count(void) {
    return g_numa_available ? g_numa_topology.num_nodes : 1;
}

int svdb_numa_cpu_count(void) {
    return g_numa_available ? g_numa_topology.num_cpus : static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
}

int svdb_numa_current_node(void) {
#ifdef __linux__
    unsigned int cpu, node;
    if (syscall(__NR_getcpu, &cpu, &node, nullptr) == 0) {
        return static_cast<int>(node);
    }
#endif
    return 0;
}

int svdb_numa_cpu_to_node(int cpu) {
    if (!g_numa_available || cpu < 0) return -1;

    for (int n = 0; n < g_numa_topology.num_nodes; n++) {
        if (g_numa_topology.nodes[n].cpu_mask[cpu / 32] & (1u << (cpu % 32))) {
            return g_numa_topology.nodes[n].node_id;
        }
    }
    return -1;
}

uint64_t svdb_numa_node_memory(int node) {
    if (!g_numa_available) return 0;

    for (int n = 0; n < g_numa_topology.num_nodes; n++) {
        if (g_numa_topology.nodes[n].node_id == node) {
            return g_numa_topology.nodes[n].total_memory;
        }
    }
    return 0;
}

uint64_t svdb_numa_node_free_memory(int node) {
    if (!g_numa_available) return 0;

    for (int n = 0; n < g_numa_topology.num_nodes; n++) {
        if (g_numa_topology.nodes[n].node_id == node) {
            return g_numa_topology.nodes[n].free_memory;
        }
    }
    return 0;
}

/* ============================================================================
 * Thread Binding
 * ============================================================================ */

int svdb_numa_bind_to_node(int node) {
#ifdef __linux__
    if (!g_numa_available) return -1;

    /* Find the node info */
    svdb_numa_node_info_t* node_info = nullptr;
    for (int n = 0; n < g_numa_topology.num_nodes; n++) {
        if (g_numa_topology.nodes[n].node_id == node) {
            node_info = &g_numa_topology.nodes[n];
            break;
        }
    }

    if (!node_info) return -1;

    /* Build CPU set */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    for (int cpu = 0; cpu < SVDB_NUMA_MAX_NODES; cpu++) {
        if (node_info->cpu_mask[cpu / 32] & (1u << (cpu % 32))) {
            CPU_SET(cpu, &cpuset);
        }
    }

    /* Set CPU affinity */
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
        return -1;
    }

    /* Set memory policy to local */
    svdb_numa_set_policy(SVDB_NUMA_POLICY_LOCAL, node);

    return 0;
#else
    (void)node;
    return -1;
#endif
}

int svdb_numa_bind_to_cpus(const uint32_t* cpu_mask, int mask_size) {
#ifdef __linux__
    if (!cpu_mask || mask_size <= 0) return -1;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    for (int i = 0; i < mask_size * 32; i++) {
        if (cpu_mask[i / 32] & (1u << (i % 32))) {
            CPU_SET(i, &cpuset);
        }
    }

    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
        return -1;
    }

    return 0;
#else
    (void)cpu_mask;
    (void)mask_size;
    return -1;
#endif
}

int svdb_numa_set_policy(svdb_numa_policy_t policy, int preferred_node) {
#ifdef __linux__
    /* Use set_mempolicy syscall */
    #ifndef __NR_set_mempolicy
    #define __NR_set_mempolicy 238
    #endif

    int mode = 0;
    unsigned long nodemask = 0;

    switch (policy) {
        case SVDB_NUMA_POLICY_DEFAULT:
            mode = 0;  /* MPOL_DEFAULT */
            break;
        case SVDB_NUMA_POLICY_LOCAL:
            mode = 1;  /* MPOL_PREFERRED */
            nodemask = (preferred_node >= 0) ? (1UL << preferred_node) : 0;
            break;
        case SVDB_NUMA_POLICY_INTERLEAVED:
            mode = 3;  /* MPOL_INTERLEAVE */
            /* Set all nodes in mask */
            for (int n = 0; n < g_numa_topology.num_nodes; n++) {
                nodemask |= (1UL << g_numa_topology.nodes[n].node_id);
            }
            break;
        case SVDB_NUMA_POLICY_PREFERRED:
            mode = 1;  /* MPOL_PREFERRED */
            if (preferred_node >= 0) {
                nodemask = (1UL << preferred_node);
            }
            break;
    }

    long ret = syscall(__NR_set_mempolicy, mode, &nodemask, SVDB_NUMA_MAX_NODES);
    return (ret == 0) ? 0 : -1;
#else
    (void)policy;
    (void)preferred_node;
    return -1;
#endif
}

svdb_numa_policy_t svdb_numa_get_policy(int* preferred_node) {
    if (preferred_node) *preferred_node = 0;

#ifdef __linux__
    #ifndef __NR_get_mempolicy
    #define __NR_get_mempolicy 239
    #endif

    int mode = 0;
    unsigned long nodemask = 0;

    long ret = syscall(__NR_get_mempolicy, &mode, &nodemask, SVDB_NUMA_MAX_NODES, 0, 0);
    if (ret != 0) {
        return SVDB_NUMA_POLICY_DEFAULT;
    }

    if (preferred_node) {
        *preferred_node = __builtin_ffsll(nodemask) - 1;
    }

    switch (mode) {
        case 0: return SVDB_NUMA_POLICY_DEFAULT;
        case 1: return SVDB_NUMA_POLICY_PREFERRED;
        case 3: return SVDB_NUMA_POLICY_INTERLEAVED;
        default: return SVDB_NUMA_POLICY_DEFAULT;
    }
#else
    return SVDB_NUMA_POLICY_DEFAULT;
#endif
}

/* ============================================================================
 * NUMA-Aware Memory Allocation
 * ============================================================================ */

void* svdb_numa_alloc_on_node(size_t size, int node) {
#ifdef __linux__
    #ifndef __NR_mbind
    #define __NR_mbind 237
    #endif

    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        return nullptr;
    }

    unsigned long nodemask = (1UL << node);
    long ret = syscall(__NR_mbind, ptr, size, 1 /* MPOL_PREFERRED */,
                       &nodemask, SVDB_NUMA_MAX_NODES, 0);

    if (ret != 0) {
        munmap(ptr, size);
        return nullptr;
    }

    return ptr;
#else
    (void)node;
    return malloc(size);
#endif
}

void* svdb_numa_alloc_local(size_t size) {
    int node = svdb_numa_current_node();
    return svdb_numa_alloc_on_node(size, node >= 0 ? node : 0);
}

void* svdb_numa_alloc_interleaved(size_t size) {
#ifdef __linux__
    #ifndef __NR_mbind
    #define __NR_mbind 237
    #endif

    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        return nullptr;
    }

    unsigned long nodemask = 0;
    for (int n = 0; n < g_numa_topology.num_nodes; n++) {
        nodemask |= (1UL << g_numa_topology.nodes[n].node_id);
    }

    long ret = syscall(__NR_mbind, ptr, size, 3 /* MPOL_INTERLEAVE */,
                       &nodemask, SVDB_NUMA_MAX_NODES, 0);

    if (ret != 0) {
        munmap(ptr, size);
        return nullptr;
    }

    return ptr;
#else
    return malloc(size);
#endif
}

void svdb_numa_free(void* ptr, size_t size) {
    if (!ptr) return;

#ifdef __linux__
    munmap(ptr, size);
#else
    (void)size;
    free(ptr);
#endif
}

void* svdb_numa_realloc(void* ptr, size_t old_size, size_t new_size, int node) {
    void* new_ptr = svdb_numa_alloc_on_node(new_size, node);
    if (!new_ptr) return nullptr;

    if (ptr && old_size > 0) {
        memcpy(new_ptr, ptr, old_size < new_size ? old_size : new_size);
        svdb_numa_free(ptr, old_size);
    }

    return new_ptr;
}

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

int svdb_numa_get_worker_node(int worker_index) {
    if (!g_numa_available || g_numa_topology.num_nodes == 0) {
        return 0;
    }

    /* Round-robin distribution */
    int current = g_next_worker_node.fetch_add(1) % g_numa_topology.num_nodes;
    return g_numa_topology.nodes[current].node_id;
}

int svdb_numa_get_node_cpus(int node, uint32_t* cpu_mask, int mask_size) {
    if (!g_numa_available || !cpu_mask || mask_size <= 0) return -1;

    memset(cpu_mask, 0, mask_size * sizeof(uint32_t));

    for (int n = 0; n < g_numa_topology.num_nodes; n++) {
        if (g_numa_topology.nodes[n].node_id == node) {
            int copy_size = std::min(mask_size, SVDB_NUMA_MAX_NODES / 32);
            memcpy(cpu_mask, g_numa_topology.nodes[n].cpu_mask,
                   copy_size * sizeof(uint32_t));
            return g_numa_topology.nodes[n].cpu_count;
        }
    }

    return -1;
}

void svdb_numa_print_topology(void) {
    if (!g_numa_available) {
        return;
    }

    for (int n = 0; n < g_numa_topology.num_nodes; n++) {
        svdb_numa_node_info_t* node = &g_numa_topology.nodes[n];
    }
}

int svdb_numa_get_address_node(void* addr) {
    if (!addr || !g_numa_available) return -1;

#ifdef __linux__
    #ifndef __NR_move_pages
    #define __NR_move_pages 279
    #endif

    int node = -1;
    long ret = syscall(__NR_move_pages, 0, 1, &addr, nullptr, &node, 0);

    if (ret == 0 && node >= 0) {
        return node;
    }
#endif

    (void)addr;
    return -1;
}

} // extern "C"