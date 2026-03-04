#ifndef SVDB_PB_VFS_H
#define SVDB_PB_VFS_H

#include <cstdint>
#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <fstream>

namespace svdb {
namespace pb {

enum class OpenFlags : int {
    ReadOnly = 0x00000001,
    WriteOnly = 0x00000002,
    ReadWrite = 0x00000003,
    Create = 0x00000004,
    Exclusive = 0x00000008,
    Truncate = 0x00000010,
    Append = 0x00000020
};

class VFSFile {
public:
    VFSFile(const std::string& path, OpenFlags flags);
    ~VFSFile();

    int64_t ReadAt(uint8_t* buf, int64_t len, int64_t offset);
    int64_t WriteAt(const uint8_t* data, int64_t len, int64_t offset);
    int Sync();
    int Close();
    int64_t GetSize() const;
    int Truncate(int64_t size);

    bool IsValid() const { return is_valid_; }
    const std::string& GetPath() const { return path_; }

private:
    std::string path_;
    OpenFlags flags_;
    std::fstream stream_;
    int64_t cached_size_;
    bool is_valid_;
    std::mutex mutex_;
};

class VFS {
public:
    VFS();
    ~VFS();

    VFSFile* OpenFile(const std::string& path, OpenFlags flags);
    int DeleteFile(const std::string& path);
    int64_t GetFileSize(const std::string& path);

    bool FileExists(const std::string& path);

private:
    std::string temp_dir_;
    std::mutex mutex_;
};

} // namespace pb
} // namespace svdb

// C-compatible wrapper functions (outside namespace)
extern "C" {

/* Open flags for C API */
#define SVDB_PB_OPEN_READONLY   0x00000001
#define SVDB_PB_OPEN_WRITEONLY  0x00000002
#define SVDB_PB_OPEN_READWRITE  0x00000003
#define SVDB_PB_OPEN_CREATE     0x00000004
#define SVDB_PB_OPEN_EXCLUSIVE  0x00000008
#define SVDB_PB_OPEN_TRUNC      0x00000010
#define SVDB_PB_OPEN_APPEND     0x00000020

void* SVDB_PB_VFS_Create();
void SVDB_PB_VFS_Destroy(void* vfs);

void* SVDB_PB_VFS_Open(void* vfs, const char* path, int flags);
int SVDB_PB_VFS_Close(void* file);

int64_t SVDB_PB_VFS_Read(void* file, uint8_t* buf, int64_t len, int64_t offset);
int64_t SVDB_PB_VFS_Write(void* file, const uint8_t* data, int64_t len, int64_t offset);
int SVDB_PB_VFS_Sync(void* file);
int64_t SVDB_PB_VFS_GetSize(void* file);
int SVDB_PB_VFS_Truncate(void* file, int64_t size);

int SVDB_PB_VFS_Delete(void* vfs, const char* path);
int SVDB_PB_VFS_Exists(void* vfs, const char* path);

} // extern "C"

#endif // SVDB_PB_VFS_H
