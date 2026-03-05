#ifndef SVDB_IS_SCHEMA_H
#define SVDB_IS_SCHEMA_H

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

namespace svdb {
namespace is {

enum class TableType {
    BaseTable,
    View,
    SystemTable
};

enum class ConstraintType {
    PrimaryKey,
    Unique,
    Check,
    ForeignKey
};

struct ColumnInfo {
    std::string column_name;
    std::string table_name;
    std::string table_schema;
    std::string data_type;
    bool is_nullable;
    std::string column_default;
    int ordinal_position;
};

struct TableInfo {
    std::string table_name;
    std::string table_schema;
    TableType table_type;
    std::vector<ColumnInfo> columns;
    std::vector<std::string> primary_key_columns;
};

struct ConstraintInfo {
    std::string constraint_name;
    std::string table_name;
    std::string table_schema;
    ConstraintType constraint_type;
    std::vector<std::string> columns;
};

struct ForeignKeyInfo {
    std::string constraint_name;
    std::string table_name;
    std::vector<std::string> child_columns;
    std::string parent_table;
    std::vector<std::string> parent_columns;
};

class Schema {
public:
    Schema();
    ~Schema();

    void AddTable(const TableInfo& table);
    void AddConstraint(const ConstraintInfo& constraint);
    void AddForeignKey(const ForeignKeyInfo& fk);

    std::vector<TableInfo> GetTables() const;
    std::vector<ColumnInfo> GetColumns(const std::string& table_name) const;
    std::vector<ConstraintInfo> GetConstraints(const std::string& table_name) const;
    std::vector<ForeignKeyInfo> GetForeignKeys(const std::string& table_name) const;

    bool HasTable(const std::string& table_name) const;
    const TableInfo* GetTable(const std::string& table_name) const;

    void Clear();

private:
    std::unordered_map<std::string, TableInfo> tables_;
    std::vector<ConstraintInfo> constraints_;
    std::vector<ForeignKeyInfo> foreign_keys_;
};

// C-compatible wrapper functions
extern "C" {

void* SVDB_IS_Create();
void SVDB_IS_Destroy(void* schema);

void SVDB_IS_AddTable(void* schema, const char* table_name, const char* table_schema, int table_type);
void SVDB_IS_AddColumn(void* schema, const char* table_name, const char* column_name, 
                       const char* data_type, int is_nullable, const char* default_value, int position);
void SVDB_IS_AddConstraint(void* schema, const char* table_name, const char* constraint_name, 
                           int constraint_type);
void SVDB_IS_AddPrimaryKey(void* schema, const char* table_name, const char* column_name);

int SVDB_IS_HasTable(void* schema, const char* table_name);

int SVDB_IS_GetTableCount(void* schema);
int SVDB_IS_GetColumnCount(void* schema, const char* table_name);

void SVDB_IS_Clear(void* schema);

} // extern "C"

} // namespace is
} // namespace svdb

#endif // SVDB_IS_SCHEMA_H
