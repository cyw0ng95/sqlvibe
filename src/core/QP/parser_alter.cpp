#include "parser_alter.h"
#include "parser_internal.h"
#include <cstring>
#include <cctype>

namespace svdb {
namespace qp {

static bool is_space(unsigned char c) {
    return std::isspace(c);
}

static std::string to_upper_str(const std::string& s) {
    std::string r = s;
    for (auto& c : r) c = (char)std::toupper((unsigned char)c);
    return r;
}

} // namespace qp
} // namespace svdb

extern "C" {

svdb_ast_node_t* svdb_parser_parse_alter(svdb_parser_t* parser,
                                          const char* sql, size_t sql_len) {
    if (!sql || sql_len == 0) return nullptr;
    
    std::string sql_str(sql, sql_len);
    std::string upper = svdb::qp::to_upper_str(sql_str);
    
    /* Check for ALTER TABLE */
    if (upper.find("ALTER TABLE") != 0) {
        return nullptr;
    }
    
    /* Parse: ALTER TABLE table_name action */
    size_t pos = 11; /* after "ALTER TABLE" */
    while (pos < sql_str.size() && svdb::qp::is_space(sql_str[pos])) pos++;
    
    if (pos >= sql_str.size()) return nullptr;
    
    /* Read table name */
    size_t table_start = pos;
    while (pos < sql_str.size() && !svdb::qp::is_space(sql_str[pos])) pos++;
    std::string table_name = sql_str.substr(table_start, pos - table_start);
    
    /* Skip whitespace */
    while (pos < sql_str.size() && svdb::qp::is_space(sql_str[pos])) pos++;
    
    if (pos >= sql_str.size()) return nullptr;
    
    /* Read action keyword */
    size_t action_start = pos;
    while (pos < sql_str.size() && !svdb::qp::is_space(sql_str[pos])) pos++;
    std::string action = svdb::qp::to_upper_str(sql_str.substr(action_start, pos - action_start));
    
    /* The rest is stored in where_text as extra info */
    while (pos < sql_str.size() && svdb::qp::is_space(sql_str[pos])) pos++;
    std::string extra = sql_str.substr(pos);
    
    svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_ALTER);
    svdb_ast_node_set_table(node, table_name);
    svdb_ast_node_set_where(node, action + " " + extra);
    
    return node;
}

} /* extern "C" */
