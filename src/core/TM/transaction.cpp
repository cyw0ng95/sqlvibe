#include "transaction.h"
#include <stdexcept>
#include <cstring>

namespace svdb {
namespace tm {

Transaction::Transaction(uint64_t id, TransactionType type, const std::string& db_path)
    : id_(id), type_(type), state_(TransactionState::Active), 
      lock_type_(LockType::None), db_path_(db_path), start_time_(std::chrono::steady_clock::now()) {
}

Transaction::~Transaction() {
}

void Transaction::AddChange(const Change& change) {
    changes_.push_back(change);
}

TransactionManager::TransactionManager() : next_tx_id_(1) {
}

TransactionManager::~TransactionManager() {
    for (auto tx : transactions_) {
        delete tx;
    }
}

uint64_t TransactionManager::BeginTransaction(TransactionType type, const std::string& db_path) {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    uint64_t tx_id = next_tx_id_++;
    Transaction* tx = new Transaction(tx_id, type, db_path);
    transactions_.push_back(tx);
    return tx_id;
}

int TransactionManager::CommitTransaction(uint64_t tx_id) {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    for (auto tx : transactions_) {
        if (tx->GetID() == tx_id) {
            tx->SetState(TransactionState::Committed);
            return 0;
        }
    }
    return -1;
}

int TransactionManager::RollbackTransaction(uint64_t tx_id) {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    for (auto tx : transactions_) {
        if (tx->GetID() == tx_id) {
            tx->SetState(TransactionState::RolledBack);
            return 0;
        }
    }
    return -1;
}

Transaction* TransactionManager::GetTransaction(uint64_t tx_id) {
    for (auto tx : transactions_) {
        if (tx->GetID() == tx_id) {
            return tx;
        }
    }
    return nullptr;
}

void TransactionManager::ReleaseTransaction(uint64_t tx_id) {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    for (auto it = transactions_.begin(); it != transactions_.end(); ++it) {
        if ((*it)->GetID() == tx_id) {
            delete *it;
            transactions_.erase(it);
            return;
        }
    }
}

int TransactionManager::AcquireLock(uint64_t tx_id, const std::string& resource, LockType lock) {
    return 0;
}

int TransactionManager::ReleaseLock(uint64_t tx_id, const std::string& resource) {
    return 0;
}

bool TransactionManager::HasActiveTransaction() const {
    for (auto tx : transactions_) {
        if (tx->GetState() == TransactionState::Active) {
            return true;
        }
    }
    return false;
}

size_t TransactionManager::GetActiveTransactionCount() const {
    size_t count = 0;
    for (auto tx : transactions_) {
        if (tx->GetState() == TransactionState::Active) {
            count++;
        }
    }
    return count;
}

const char* TransactionTypeToString(TransactionType type) {
    switch (type) {
        case TransactionType::Deferred: return "DEFERRED";
        case TransactionType::Immediate: return "IMMEDIATE";
        case TransactionType::Exclusive: return "EXCLUSIVE";
        default: return "UNKNOWN";
    }
}

const char* TransactionStateToString(TransactionState state) {
    switch (state) {
        case TransactionState::None: return "NONE";
        case TransactionState::Active: return "ACTIVE";
        case TransactionState::Committed: return "COMMITTED";
        case TransactionState::RolledBack: return "ROLLED_BACK";
        default: return "UNKNOWN";
    }
}

const char* LockTypeToString(LockType lock) {
    switch (lock) {
        case LockType::None: return "NONE";
        case LockType::Shared: return "SHARED";
        case LockType::Reserved: return "RESERVED";
        case LockType::Exclusive: return "EXCLUSIVE";
        default: return "UNKNOWN";
    }
}

} // namespace tm
} // namespace svdb

// C-compatible wrapper functions for CGO
extern "C" {

static svdb::tm::TransactionManager* g_mgr = nullptr;

int32_t SVDB_TM_TRANSACTION_DEFERRED() { 
    return static_cast<int32_t>(svdb::tm::TransactionType::Deferred); 
}
int32_t SVDB_TM_TRANSACTION_IMMEDIATE() { 
    return static_cast<int32_t>(svdb::tm::TransactionType::Immediate); 
}
int32_t SVDB_TM_TRANSACTION_EXCLUSIVE() { 
    return static_cast<int32_t>(svdb::tm::TransactionType::Exclusive); 
}

int32_t SVDB_TM_STATE_NONE() { 
    return static_cast<int32_t>(svdb::tm::TransactionState::None); 
}
int32_t SVDB_TM_STATE_ACTIVE() { 
    return static_cast<int32_t>(svdb::tm::TransactionState::Active); 
}
int32_t SVDB_TM_STATE_COMMITTED() { 
    return static_cast<int32_t>(svdb::tm::TransactionState::Committed); 
}
int32_t SVDB_TM_STATE_ROLLED_BACK() { 
    return static_cast<int32_t>(svdb::tm::TransactionState::RolledBack); 
}

int32_t SVDB_TM_LOCK_NONE() { 
    return static_cast<int32_t>(svdb::tm::LockType::None); 
}
int32_t SVDB_TM_LOCK_SHARED() { 
    return static_cast<int32_t>(svdb::tm::LockType::Shared); 
}
int32_t SVDB_TM_LOCK_RESERVED() { 
    return static_cast<int32_t>(svdb::tm::LockType::Reserved); 
}
int32_t SVDB_TM_LOCK_EXCLUSIVE() { 
    return static_cast<int32_t>(svdb::tm::LockType::Exclusive); 
}

void* SVDB_TM_Create() {
    if (!g_mgr) {
        g_mgr = new svdb::tm::TransactionManager();
    }
    return g_mgr;
}

void SVDB_TM_Destroy(void* mgr) {
    if (mgr == g_mgr) {
        delete g_mgr;
        g_mgr = nullptr;
    }
}

uint64_t SVDB_TM_Begin(void* mgr, int32_t type, const char* db_path) {
    auto* tx_mgr = static_cast<svdb::tm::TransactionManager*>(mgr);
    auto tx_type = static_cast<svdb::tm::TransactionType>(type);
    return tx_mgr->BeginTransaction(tx_type, std::string(db_path));
}

int32_t SVDB_TM_Commit(void* mgr, uint64_t tx_id) {
    auto* tx_mgr = static_cast<svdb::tm::TransactionManager*>(mgr);
    return tx_mgr->CommitTransaction(tx_id);
}

int32_t SVDB_TM_Rollback(void* mgr, uint64_t tx_id) {
    auto* tx_mgr = static_cast<svdb::tm::TransactionManager*>(mgr);
    return tx_mgr->RollbackTransaction(tx_id);
}

uint64_t SVDB_TM_HasActive(void* mgr) {
    auto* tx_mgr = static_cast<svdb::tm::TransactionManager*>(mgr);
    return tx_mgr->HasActiveTransaction() ? 1 : 0;
}

uint64_t SVDB_TM_GetActiveCount(void* mgr) {
    auto* tx_mgr = static_cast<svdb::tm::TransactionManager*>(mgr);
    return tx_mgr->GetActiveTransactionCount();
}

}
