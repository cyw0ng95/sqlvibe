#ifndef SVDB_TM_TRANSACTION_H
#define SVDB_TM_TRANSACTION_H

#include <cstdint>
#include <string>
#include <vector>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <atomic>

namespace svdb {
namespace tm {

enum class TransactionType : int {
    Deferred = 0,
    Immediate = 1,
    Exclusive = 2
};

enum class TransactionState : int {
    None = 0,
    Active = 1,
    Committed = 2,
    RolledBack = 3
};

enum class LockType : int {
    None = 0,
    Shared = 1,
    Reserved = 2,
    Exclusive = 3
};

struct Change {
    std::string type;
    std::string table_name;
    uint64_t row_id;
    std::vector<uint8_t> old_data;
    std::vector<uint8_t> new_data;
};

class Transaction {
public:
    Transaction(uint64_t id, TransactionType type, const std::string& db_path);
    ~Transaction();

    uint64_t GetID() const { return id_; }
    TransactionType GetType() const { return type_; }
    TransactionState GetState() const { return state_; }
    LockType GetLockType() const { return lock_type_; }
    
    void SetState(TransactionState state) { state_ = state; }
    void SetLockType(LockType lock) { lock_type_ = lock; }
    
    void AddChange(const Change& change);
    const std::vector<Change>& GetChanges() const { return changes_; }
    
    int64_t GetStartTime() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            start_time_.time_since_epoch()).count();
    }

private:
    uint64_t id_;
    TransactionType type_;
    TransactionState state_;
    LockType lock_type_;
    std::string db_path_;
    std::chrono::steady_clock::time_point start_time_;
    std::vector<Change> changes_;
};

// WS8: WAL batch commit buffer.
// Accumulates committed changes from multiple transactions and flushes them
// to the WAL file in a single write + fsync call, reducing write amplification.
struct WalBatchEntry {
    uint64_t tx_id;
    std::vector<Change> changes;
};

class TransactionManager {
public:
    static constexpr size_t DEFAULT_WAL_BATCH_SIZE = 64;   // flush every 64 commits
    static constexpr size_t DEFAULT_WAL_BATCH_BYTES = 4 * 1024 * 1024;  // or 4 MB

    TransactionManager();
    ~TransactionManager();

    uint64_t BeginTransaction(TransactionType type, const std::string& db_path);
    int CommitTransaction(uint64_t tx_id);
    int RollbackTransaction(uint64_t tx_id);
    
    // WS8: Flush the WAL batch immediately (called on checkpoint or explicit sync).
    void FlushWalBatch();

    // WS8: Configure WAL batch parameters.
    void SetWalBatchSize(size_t max_entries) { wal_batch_max_entries_ = max_entries; }
    void SetWalBatchBytes(size_t max_bytes)  { wal_batch_max_bytes_  = max_bytes; }

    Transaction* GetTransaction(uint64_t tx_id);
    void ReleaseTransaction(uint64_t tx_id);
    
    int AcquireLock(uint64_t tx_id, const std::string& resource, LockType lock);
    int ReleaseLock(uint64_t tx_id, const std::string& resource);
    
    bool HasActiveTransaction() const;
    size_t GetActiveTransactionCount() const;

    // WS8: MVCC GC – remove versions older than keep_below_tx_id.
    // Returns number of version records pruned.
    size_t PruneOldVersions(uint64_t keep_below_tx_id);

private:
    // Accumulate entry into WAL batch; flush if thresholds exceeded.
    void AppendToWalBatch(uint64_t tx_id, const std::vector<Change>& changes);

    uint64_t next_tx_id_;
    std::mutex tx_mutex_;
    std::vector<Transaction*> transactions_;

    // WS8: WAL batch state (protected by tx_mutex_)
    std::vector<WalBatchEntry> wal_batch_;
    size_t wal_batch_bytes_;
    size_t wal_batch_max_entries_;
    size_t wal_batch_max_bytes_;

    // WS8: committed-change archive for MVCC GC (compact representation)
    // key = tx_id, value = total change count
    std::vector<std::pair<uint64_t, size_t>> committed_tx_log_;
};

const char* TransactionTypeToString(TransactionType type);
const char* TransactionStateToString(TransactionState state);
const char* LockTypeToString(LockType lock);

} // namespace tm
} // namespace svdb

#endif // SVDB_TM_TRANSACTION_H
