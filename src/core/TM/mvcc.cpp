#include "mvcc.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <cstdint>
#include <cstring>

namespace svdb {
namespace tm {

/* Versioned value stored in MVCC. */
struct VersionedValue {
    uint64_t commit_id;
    bool deleted;
    std::string data;  // Store data inline for simplicity
    
    VersionedValue() : commit_id(0), deleted(false) {}
    VersionedValue(uint64_t cid, bool del, const void* d, size_t len)
        : commit_id(cid), deleted(del) {
        if (d && len > 0) {
            data.assign(static_cast<const char*>(d), len);
        }
    }
};

/* Snapshot for read-consistent views. */
struct Snapshot {
    uint64_t commit_id;
    std::unordered_map<uint64_t, bool> active_txns;
    
    Snapshot() : commit_id(0) {}
};

/* MVCC store implementation. */
class MVCCStore {
public:
    MVCCStore() : commit_id_(0) {}
    
    /* Create a snapshot at current commit ID. */
    Snapshot* create_snapshot() {
        Snapshot* snap = new Snapshot();
        snap->commit_id = commit_id_.load(std::memory_order_acquire);
        return snap;
    }
    
    /* Free a snapshot. */
    void free_snapshot(Snapshot* snap) {
        delete snap;
    }
    
    /*
     * Get value for key under snapshot visibility.
     * Returns true if found, false otherwise.
     */
    bool get(Snapshot* snap, const std::string& key, 
             const char** data, size_t* data_len) {
        std::shared_lock<std::shared_mutex> lock(mu_);
        
        auto it = versions_.find(key);
        if (it == versions_.end()) {
            return false;
        }
        
        const auto& chain = it->second;
        
        // Walk from newest to oldest to find first visible version
        for (int i = static_cast<int>(chain.size()) - 1; i >= 0; --i) {
            const auto& v = chain[i];
            
            // Skip versions written after our snapshot
            if (v.commit_id > snap->commit_id) {
                continue;
            }
            
            // Skip versions written by still-active transactions
            if (snap->active_txns.count(v.commit_id)) {
                continue;
            }
            
            // Found visible version
            if (v.deleted) {
                return false;
            }
            
            *data = v.data.c_str();
            *data_len = v.data.size();
            return true;
        }
        
        return false;
    }
    
    /*
     * Put a new version for key.
     * Returns the new commit ID.
     */
    uint64_t put(const std::string& key, const void* data, size_t data_len) {
        uint64_t cid = commit_id_.fetch_add(1, std::memory_order_acq_rel) + 1;
        
        VersionedValue vv(cid, false, data, data_len);
        
        {
            std::unique_lock<std::shared_mutex> lock(mu_);
            versions_[key].push_back(std::move(vv));
        }
        
        return cid;
    }
    
    /*
     * Mark key as deleted.
     * Returns the new commit ID.
     */
    uint64_t remove(const std::string& key) {
        uint64_t cid = commit_id_.fetch_add(1, std::memory_order_acq_rel) + 1;
        
        VersionedValue vv(cid, true, nullptr, 0);
        
        {
            std::unique_lock<std::shared_mutex> lock(mu_);
            versions_[key].push_back(std::move(vv));
        }
        
        return cid;
    }
    
    /*
     * Garbage collect old versions.
     * keep_below: minimum commit ID to retain.
     * Returns number of versions pruned.
     */
    size_t gc(uint64_t keep_below) {
        std::unique_lock<std::shared_mutex> lock(mu_);
        
        size_t pruned = 0;
        
        for (auto it = versions_.begin(); it != versions_.end(); ) {
            auto& chain = it->second;
            
            // Find the last version whose commit_id < keep_below (the baseline)
            int baseline = -1;
            for (size_t i = 0; i < chain.size(); ++i) {
                if (chain[i].commit_id < keep_below) {
                    baseline = static_cast<int>(i);
                }
            }
            
            // Keep from baseline onward
            if (baseline > 0) {
                pruned += baseline;
                chain.erase(chain.begin(), chain.begin() + baseline);
            }
            
            // If chain is just one deleted version below keep_below, remove key
            if (chain.size() == 1 && 
                chain[0].deleted && 
                chain[0].commit_id < keep_below) {
                it = versions_.erase(it);
            } else {
                ++it;
            }
        }
        
        return pruned;
    }
    
    /* Get current commit ID. */
    uint64_t commit_id() const {
        return commit_id_.load(std::memory_order_acquire);
    }
    
    /* Get number of keys. */
    size_t key_count() const {
        std::shared_lock<std::shared_mutex> lock(mu_);
        return versions_.size();
    }

private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::vector<VersionedValue>> versions_;
    std::atomic<uint64_t> commit_id_;
};

} // namespace tm
} // namespace svdb

/* C API implementation. */
extern "C" {

struct svdb_mvcc_store_t {
    svdb::tm::MVCCStore store;
};

struct svdb_mvcc_snapshot_t {
    svdb::tm::Snapshot snapshot;
};

svdb_mvcc_store_t* svdb_mvcc_store_create(void) {
    return new (std::nothrow) svdb_mvcc_store_t();
}

void svdb_mvcc_store_destroy(svdb_mvcc_store_t* store) {
    delete store;
}

svdb_mvcc_snapshot_t* svdb_mvcc_store_snapshot(svdb_mvcc_store_t* store) {
    if (!store) return nullptr;
    
    svdb_mvcc_snapshot_t* snap = new (std::nothrow) svdb_mvcc_snapshot_t();
    if (!snap) return nullptr;
    
    snap->snapshot = *store->store.create_snapshot();
    return snap;
}

void svdb_mvcc_snapshot_free(svdb_mvcc_snapshot_t* snapshot) {
    delete snapshot;
}

int svdb_mvcc_store_get(
    svdb_mvcc_store_t* store,
    svdb_mvcc_snapshot_t* snapshot,
    const char* key,
    size_t key_len,
    const char** data,
    size_t* data_len
) {
    if (!store || !snapshot || !key || !data || !data_len) {
        return 0;
    }

    std::string k(key, key_len);
    const char* result_data;
    size_t result_len;

    if (store->store.get(&snapshot->snapshot, k, &result_data, &result_len)) {
        *data = result_data;
        *data_len = result_len;
        return 1;
    }

    return 0;
}

uint64_t svdb_mvcc_store_put(
    svdb_mvcc_store_t* store,
    const char* key,
    size_t key_len,
    const void* data,
    size_t data_len
) {
    if (!store || !key) {
        return 0;
    }
    
    std::string k(key, key_len);
    return store->store.put(k, data, data_len);
}

uint64_t svdb_mvcc_store_delete(
    svdb_mvcc_store_t* store,
    const char* key,
    size_t key_len
) {
    if (!store || !key) {
        return 0;
    }
    
    std::string k(key, key_len);
    return store->store.remove(k);
}

size_t svdb_mvcc_store_gc(svdb_mvcc_store_t* store, uint64_t keep_below) {
    if (!store) {
        return 0;
    }
    
    return store->store.gc(keep_below);
}

uint64_t svdb_mvcc_store_commit_id(svdb_mvcc_store_t* store) {
    if (!store) {
        return 0;
    }
    
    return store->store.commit_id();
}

size_t svdb_mvcc_store_key_count(svdb_mvcc_store_t* store) {
    if (!store) {
        return 0;
    }
    
    return store->store.key_count();
}

} /* extern "C" */
