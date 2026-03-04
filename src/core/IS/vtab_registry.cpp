/* vtab_registry.cpp — C++ Virtual Table Registry Implementation */
#include "vtab_registry.h"
#include <algorithm>
#include <cctype>

namespace svdb {

/* ── VTabRegistry Implementation ────────────────────────────────────────── */

VTabRegistry& VTabRegistry::Instance() {
    static VTabRegistry instance;
    return instance;
}

int VTabRegistry::RegisterModule(const std::string& name, VTabModule* module) {
    if (!module) {
        return -1;
    }
    
    std::string lower_name = name;
    std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (modules_.find(lower_name) != modules_.end()) {
        return -2; /* Already exists */
    }
    
    modules_[lower_name] = module;
    return 0;
}

VTabModule* VTabRegistry::GetModule(const std::string& name) {
    std::string lower_name = name;
    std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = modules_.find(lower_name);
    if (it == modules_.end()) {
        return nullptr;
    }
    
    return it->second;
}

int VTabRegistry::UnregisterModule(const std::string& name) {
    std::string lower_name = name;
    std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = modules_.find(lower_name);
    if (it == modules_.end()) {
        return -1; /* Not found */
    }
    
    modules_.erase(it);
    return 0;
}

bool VTabRegistry::HasModule(const std::string& name) {
    std::string lower_name = name;
    std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    
    std::lock_guard<std::mutex> lock(mutex_);
    return modules_.find(lower_name) != modules_.end();
}

std::vector<std::string> VTabRegistry::GetModuleNames() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<std::string> names;
    names.reserve(modules_.size());
    
    for (const auto& pair : modules_) {
        names.push_back(pair.first);
    }
    
    return names;
}

} /* namespace svdb */
