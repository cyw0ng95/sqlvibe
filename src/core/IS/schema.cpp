#include "schema.h"
#include <cstring>

namespace svdb {
namespace is {

Schema::Schema() {
}

Schema::~Schema() {
}

void Schema::AddTable(const TableInfo& table) {
    tables_[table.table_name] = table;
}

void Schema::AddConstraint(const ConstraintInfo& constraint) {
    constraints_.push_back(constraint);
}

void Schema::AddForeignKey(const ForeignKeyInfo& fk) {
    foreign_keys_.push_back(fk);
}

std::vector<TableInfo> Schema::GetTables() const {
    std::vector<TableInfo> result;
    for (const auto& pair : tables_) {
        result.push_back(pair.second);
    }
    return result;
}

std::vector<ColumnInfo> Schema::GetColumns(const std::string& table_name) const {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        return it->second.columns;
    }
    return std::vector<ColumnInfo>();
}

std::vector<ConstraintInfo> Schema::GetConstraints(const std::string& table_name) const {
    std::vector<ConstraintInfo> result;
    for (const auto& c : constraints_) {
        if (c.table_name == table_name) {
            result.push_back(c);
        }
    }
    return result;
}

std::vector<ForeignKeyInfo> Schema::GetForeignKeys(const std::string& table_name) const {
    std::vector<ForeignKeyInfo> result;
    for (const auto& fk : foreign_keys_) {
        if (fk.table_name == table_name) {
            result.push_back(fk);
        }
    }
    return result;
}

bool Schema::HasTable(const std::string& table_name) const {
    return tables_.find(table_name) != tables_.end();
}

const TableInfo* Schema::GetTable(const std::string& table_name) const {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        return &it->second;
    }
    return nullptr;
}

void Schema::Clear() {
    tables_.clear();
    constraints_.clear();
    foreign_keys_.clear();
}

} // namespace is
} // namespace svdb

// C-compatible wrapper functions
extern "C" {

static std::unique_ptr<svdb::is::Schema> g_schema;

void* SVDB_IS_Create() {
    g_schema = std::make_unique<svdb::is::Schema>();
    return g_schema.get();
}

void SVDB_IS_Destroy(void* schema) {
    if (schema == g_schema.get()) {
        g_schema.reset();
    }
}

void SVDB_IS_AddTable(void* schema, const char* table_name, const char* table_schema, int table_type) {
    auto* s = static_cast<svdb::is::Schema*>(schema);
    svdb::is::TableInfo table;
    table.table_name = table_name;
    table.table_schema = table_schema;
    table.table_type = static_cast<svdb::is::TableType>(table_type);
    s->AddTable(table);
}

void SVDB_IS_AddColumn(void* schema, const char* table_name, const char* column_name, 
                       const char* data_type, int is_nullable, const char* default_value, int position) {
    auto* s = static_cast<svdb::is::Schema*>(schema);
    svdb::is::ColumnInfo col;
    col.column_name = column_name;
    col.table_name = table_name;
    col.data_type = data_type;
    col.is_nullable = (is_nullable != 0);
    col.column_default = default_value ? default_value : "";
    col.ordinal_position = position;
    
    auto* table = const_cast<svdb::is::TableInfo*>(s->GetTable(table_name));
    if (table) {
        table->columns.push_back(col);
    }
}

void SVDB_IS_AddConstraint(void* schema, const char* table_name, const char* constraint_name, 
                           int constraint_type) {
    auto* s = static_cast<svdb::is::Schema*>(schema);
    svdb::is::ConstraintInfo constraint;
    constraint.table_name = table_name;
    constraint.constraint_name = constraint_name;
    constraint.constraint_type = static_cast<svdb::is::ConstraintType>(constraint_type);
    s->AddConstraint(constraint);
}

void SVDB_IS_AddPrimaryKey(void* schema, const char* table_name, const char* column_name) {
    auto* s = static_cast<svdb::is::Schema*>(schema);
    auto* table = const_cast<svdb::is::TableInfo*>(s->GetTable(table_name));
    if (table) {
        table->primary_key_columns.push_back(column_name);
    }
}

int SVDB_IS_HasTable(void* schema, const char* table_name) {
    auto* s = static_cast<svdb::is::Schema*>(schema);
    return s->HasTable(table_name) ? 1 : 0;
}

int SVDB_IS_GetTableCount(void* schema) {
    auto* s = static_cast<svdb::is::Schema*>(schema);
    return s->GetTables().size();
}

int SVDB_IS_GetColumnCount(void* schema, const char* table_name) {
    auto* s = static_cast<svdb::is::Schema*>(schema);
    return s->GetColumns(table_name).size();
}

void SVDB_IS_Clear(void* schema) {
    auto* s = static_cast<svdb::is::Schema*>(schema);
    s->Clear();
}

}
