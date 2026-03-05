#include "dag.h"
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <queue>

/* ------------------------------------------------------------------ types */

struct DagNode {
    std::string name;
    bool        done;
    DagNode(const std::string& n) : name(n), done(false) {}
};

struct Dag {
    std::vector<DagNode>          nodes;
    std::vector<std::vector<int>> preds; /* preds[i] = list of predecessor IDs */
    std::vector<std::vector<int>> succs; /* succs[i] = list of successor  IDs */
};

/* ------------------------------------------------------------------ API */

extern "C" {

svdb_dag_t svdb_dag_create(void)
{
    return new Dag();
}

void svdb_dag_destroy(svdb_dag_t dag)
{
    delete static_cast<Dag*>(dag);
}

int svdb_dag_add_node(svdb_dag_t dag, const char* op_name)
{
    if (!dag) return -1;
    Dag* d = static_cast<Dag*>(dag);
    int id = (int)d->nodes.size();
    d->nodes.emplace_back(op_name ? op_name : "");
    d->preds.emplace_back();
    d->succs.emplace_back();
    return id;
}

void svdb_dag_add_edge(svdb_dag_t dag, int src_id, int dst_id)
{
    if (!dag) return;
    Dag* d = static_cast<Dag*>(dag);
    int n = (int)d->nodes.size();
    if (src_id < 0 || src_id >= n || dst_id < 0 || dst_id >= n) return;
    d->succs[src_id].push_back(dst_id);
    d->preds[dst_id].push_back(src_id);
}

int svdb_dag_get_node_count(svdb_dag_t dag)
{
    if (!dag) return 0;
    return (int)static_cast<Dag*>(dag)->nodes.size();
}

int svdb_dag_get_node_name(svdb_dag_t dag,
                            int        node_id,
                            char*      out_buf,
                            int        out_buf_size)
{
    if (!dag || !out_buf || out_buf_size <= 0) return -1;
    Dag* d = static_cast<Dag*>(dag);
    if (node_id < 0 || node_id >= (int)d->nodes.size()) return -1;
    const std::string& name = d->nodes[node_id].name;
    int len = (int)name.size();
    if (out_buf_size < len + 1) return -1;
    memcpy(out_buf, name.c_str(), (size_t)(len + 1));
    return len;
}

int svdb_dag_is_ready(svdb_dag_t dag, int node_id)
{
    if (!dag) return 0;
    Dag* d = static_cast<Dag*>(dag);
    if (node_id < 0 || node_id >= (int)d->nodes.size()) return 0;
    if (d->nodes[node_id].done) return 0;
    for (int p : d->preds[node_id])
        if (!d->nodes[p].done) return 0;
    return 1;
}

void svdb_dag_mark_done(svdb_dag_t dag, int node_id)
{
    if (!dag) return;
    Dag* d = static_cast<Dag*>(dag);
    if (node_id >= 0 && node_id < (int)d->nodes.size())
        d->nodes[node_id].done = true;
}

void svdb_dag_mark_undone(svdb_dag_t dag, int node_id)
{
    if (!dag) return;
    Dag* d = static_cast<Dag*>(dag);
    if (node_id >= 0 && node_id < (int)d->nodes.size())
        d->nodes[node_id].done = false;
}

int svdb_dag_get_ready_nodes(svdb_dag_t dag, int* out_ids, int max_out)
{
    if (!dag || !out_ids || max_out <= 0) return 0;
    Dag* d = static_cast<Dag*>(dag);
    int count = 0;
    for (int i = 0; i < (int)d->nodes.size() && count < max_out; ++i)
        if (svdb_dag_is_ready(dag, i))
            out_ids[count++] = i;
    return count;
}

int svdb_dag_topological_sort(svdb_dag_t dag, int* out_ids, int max_out)
{
    if (!dag || !out_ids || max_out <= 0) return 0;
    Dag* d = static_cast<Dag*>(dag);
    int n = (int)d->nodes.size();

    std::vector<int> indegree(n, 0);
    for (int i = 0; i < n; ++i)
        indegree[i] = (int)d->preds[i].size();

    std::queue<int> q;
    for (int i = 0; i < n; ++i)
        if (indegree[i] == 0) q.push(i);

    int count = 0;
    while (!q.empty() && count < max_out) {
        int cur = q.front(); q.pop();
        out_ids[count++] = cur;
        for (int s : d->succs[cur]) {
            if (--indegree[s] == 0)
                q.push(s);
        }
    }

    /* If not all nodes processed, there is a cycle */
    if (count < n) return -1;
    return count;
}

} /* extern "C" */
