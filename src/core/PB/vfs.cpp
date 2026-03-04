#include "vfs.h"
#include <cstring>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

namespace svdb {
namespace pb {

VFSFile::VFSFile(const std::string& path, OpenFlags flags)
    : path_(path), flags_(flags), cached_size_(0), is_valid_(false) {

    std::ios_base::openmode mode = std::ios::binary;

    int flag_val = static_cast<int>(flags);

    // Check for ReadWrite first (0x03) before ReadOnly (0x01) or WriteOnly (0x02)
    bool read_write = (flag_val & 0x03) == 0x03;
    bool read_only = !read_write && (flag_val & 0x01) != 0;
    bool write_only = !read_write && (flag_val & 0x02) != 0;
    bool create = (flag_val & 0x04) != 0;

    // Check if file exists
    struct stat st;
    bool file_exists = (stat(path.c_str(), &st) == 0);

    if (read_only) {
        mode |= std::ios::in;
    } else if (write_only) {
        mode |= std::ios::out;
        if (create && !file_exists) {
            mode |= std::ios::trunc;
        }
    } else if (read_write) {
        if (create && !file_exists) {
            // Create new file
            mode |= std::ios::out | std::ios::in | std::ios::trunc;
        } else {
            // Open existing file for read-write
            mode |= std::ios::in | std::ios::out;
        }
    }

    stream_.open(path, mode);
    if (stream_.is_open()) {
        is_valid_ = true;
        stream_.seekg(0, std::ios::end);
        cached_size_ = stream_.tellg();
        stream_.seekg(0, std::ios::beg);
    }
}

VFSFile::~VFSFile() {
    if (stream_.is_open()) {
        stream_.close();
    }
}

int64_t VFSFile::ReadAt(uint8_t* buf, int64_t len, int64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return -1;
    }
    
    stream_.seekg(offset);
    if (!stream_.good()) {
        return -1;
    }
    
    stream_.read(reinterpret_cast<char*>(buf), len);
    int64_t bytes_read = stream_.gcount();
    return bytes_read;
}

int64_t VFSFile::WriteAt(const uint8_t* data, int64_t len, int64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return -1;
    }

    // Clear any error flags before seeking
    stream_.clear();
    
    // Seek to the end to get current size
    stream_.seekp(0, std::ios::end);
    int64_t current_size = stream_.tellp();
    
    // If writing beyond current size, we need to extend the file first
    if (offset + len > current_size) {
        // Seek to position where we want to write
        stream_.seekp(offset, std::ios::beg);
        if (!stream_.good()) {
            return -1;
        }
        // Write data - this will extend the file
        stream_.write(reinterpret_cast<const char*>(data), len);
        if (!stream_.good()) {
            return -1;
        }
        // Flush to ensure data is written
        stream_.flush();
    } else {
        // Normal write within current file size
        stream_.seekp(offset, std::ios::beg);
        if (!stream_.good()) {
            return -1;
        }
        stream_.write(reinterpret_cast<const char*>(data), len);
        if (!stream_.good()) {
            return -1;
        }
        stream_.flush();
    }

    int64_t new_pos = offset + len;
    if (new_pos > cached_size_) {
        cached_size_ = new_pos;
    }

    return len;
}

int VFSFile::Sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return -1;
    }
    
    stream_.flush();
    return stream_.good() ? 0 : -1;
}

int VFSFile::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return -1;
    }
    
    stream_.close();
    is_valid_ = false;
    return 0;
}

int64_t VFSFile::GetSize() const {
    return cached_size_;
}

int VFSFile::Truncate(int64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return -1;
    }
    
    // Close and reopen with truncate
    stream_.close();
    
    if (truncate(path_.c_str(), size) != 0) {
        // Reopen in appropriate mode
        std::ios_base::openmode mode = std::ios::binary | std::ios::in | std::ios::out;
        stream_.open(path_, mode);
        return -1;
    }
    
    // Reopen file
    std::ios_base::openmode mode = std::ios::binary | std::ios::in | std::ios::out;
    stream_.open(path_, mode);
    cached_size_ = size;
    return 0;
}

VFS::VFS() {
    temp_dir_ = "/tmp";
}

VFS::~VFS() {
}

VFSFile* VFS::OpenFile(const std::string& path, OpenFlags flags) {
    VFSFile* file = new VFSFile(path, flags);
    if (!file->IsValid()) {
        delete file;
        return nullptr;
    }
    return file;
}

int VFS::DeleteFile(const std::string& path) {
    return std::remove(path.c_str()) == 0 ? 0 : -1;
}

int64_t VFS::GetFileSize(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
        return st.st_size;
    }
    return -1;
}

bool VFS::FileExists(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

} // namespace pb
} // namespace svdb

// C-compatible wrapper functions
extern "C" {

static svdb::pb::VFS* g_vfs = nullptr;

void* SVDB_PB_VFS_Create() {
    if (!g_vfs) {
        g_vfs = new svdb::pb::VFS();
    }
    return g_vfs;
}

void SVDB_PB_VFS_Destroy(void* vfs) {
    if (vfs == g_vfs) {
        delete g_vfs;
        g_vfs = nullptr;
    }
}

void* SVDB_PB_VFS_Open(void* vfs, const char* path, int flags) {
    auto* vfs_ptr = static_cast<svdb::pb::VFS*>(vfs);
    auto open_flags = static_cast<svdb::pb::OpenFlags>(flags);
    return vfs_ptr->OpenFile(std::string(path), open_flags);
}

int SVDB_PB_VFS_Close(void* file) {
    auto* f = static_cast<svdb::pb::VFSFile*>(file);
    int result = f->Close();
    delete f;
    return result;
}

int64_t SVDB_PB_VFS_Read(void* file, uint8_t* buf, int64_t len, int64_t offset) {
    auto* f = static_cast<svdb::pb::VFSFile*>(file);
    return f->ReadAt(buf, len, offset);
}

int64_t SVDB_PB_VFS_Write(void* file, const uint8_t* data, int64_t len, int64_t offset) {
    auto* f = static_cast<svdb::pb::VFSFile*>(file);
    return f->WriteAt(data, len, offset);
}

int SVDB_PB_VFS_Sync(void* file) {
    auto* f = static_cast<svdb::pb::VFSFile*>(file);
    return f->Sync();
}

int64_t SVDB_PB_VFS_GetSize(void* file) {
    auto* f = static_cast<svdb::pb::VFSFile*>(file);
    return f->GetSize();
}

int SVDB_PB_VFS_Truncate(void* file, int64_t size) {
    auto* f = static_cast<svdb::pb::VFSFile*>(file);
    return f->Truncate(size);
}

int SVDB_PB_VFS_Delete(void* vfs, const char* path) {
    auto* vfs_ptr = static_cast<svdb::pb::VFS*>(vfs);
    return vfs_ptr->DeleteFile(std::string(path));
}

int SVDB_PB_VFS_Exists(void* vfs, const char* path) {
    auto* vfs_ptr = static_cast<svdb::pb::VFS*>(vfs);
    return vfs_ptr->FileExists(std::string(path)) ? 1 : 0;
}

}
