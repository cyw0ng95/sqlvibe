#include "parser_txn.h"
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

svdb_ast_node_t* svdb_parser_parse_txn(svdb_parser_t* parser,
                                       const char* sql, size_t sql_len) {
    if (!sql || sql_len == 0) return nullptr;
    
    std::string sql_str(sql, sql_len);
    std::string upper = svdb::qp::to_upper_str(sql_str);
    
    /* Check for transaction keywords */
    if (upper.find("BEGIN") == 0) {
        svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_BEGIN);
        svdb_ast_node_set_sql(node, sql, sql_len);
        return node;
    }
    
    if (upper.find("COMMIT") == 0) {
        svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_COMMIT);
        svdb_ast_node_set_sql(node, sql, sql_len);
        return node;
    }
    
    if (upper.find("ROLLBACK") == 0) {
        svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_ROLLBACK);
        svdb_ast_node_set_sql(node, sql, sql_len);
        return node;
    }
    
    /* Not a transaction statement */
    return nullptr;
}

} /* extern "C" */
