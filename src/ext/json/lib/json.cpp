#include "json.h"
#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <algorithm>
#include <cctype>
#include <sstream>
#include <iomanip>

// Simple JSON value representation
namespace svdb_json {

enum class Type {
    Null,
    Boolean,
    Number,
    String,
    Array,
    Object
};

struct Value;

using Object = std::map<std::string, Value>;
using Array = std::vector<Value>;

struct Value {
    Type type = Type::Null;
    bool boolean = false;
    double number = 0;
    std::string string;
    Array array;
    Object object;

    Value() : type(Type::Null) {}
    
    static Value makeNull() { return Value(); }
    
    static Value makeBool(bool b) {
        Value v;
        v.type = Type::Boolean;
        v.boolean = b;
        return v;
    }
    
    static Value makeNumber(double n) {
        Value v;
        v.type = Type::Number;
        v.number = n;
        return v;
    }
    
    static Value makeString(const std::string& s) {
        Value v;
        v.type = Type::String;
        v.string = s;
        return v;
    }
    
    static Value makeArray() {
        Value v;
        v.type = Type::Array;
        return v;
    }
    
    static Value makeObject() {
        Value v;
        v.type = Type::Object;
        return v;
    }

    bool isNull() const { return type == Type::Null; }
    bool isBool() const { return type == Type::Boolean; }
    bool isNumber() const { return type == Type::Number; }
    bool isString() const { return type == Type::String; }
    bool isArray() const { return type == Type::Array; }
    bool isObject() const { return type == Type::Object; }
    
    size_t size() const {
        if (type == Type::Array) return array.size();
        if (type == Type::Object) return object.size();
        return 1;
    }
};

// JSON Parser
class Parser {
public:
    Parser(const std::string& input) : input_(input), pos_(0) {}
    
    Value parse() {
        skipWhitespace();
        return parseValue();
    }

private:
    std::string input_;
    size_t pos_;

    char peek() const {
        if (pos_ >= input_.size()) return '\0';
        return input_[pos_];
    }

    char get() {
        if (pos_ >= input_.size()) return '\0';
        return input_[pos_++];
    }

    void skipWhitespace() {
        while (pos_ < input_.size() && std::isspace(input_[pos_])) {
            pos_++;
        }
    }

    Value parseValue() {
        skipWhitespace();
        char c = peek();
        
        if (c == 'n') return parseNull();
        if (c == 't' || c == 'f') return parseBool();
        if (c == '"') return parseString();
        if (c == '[') return parseArray();
        if (c == '{') return parseObject();
        if (c == '-' || std::isdigit(c)) return parseNumber();
        
        return Value::makeNull();
    }

    Value parseNull() {
        if (input_.substr(pos_, 4) == "null") {
            pos_ += 4;
            return Value::makeNull();
        }
        return Value::makeNull();
    }

    Value parseBool() {
        if (input_.substr(pos_, 4) == "true") {
            pos_ += 4;
            return Value::makeBool(true);
        }
        if (input_.substr(pos_, 5) == "false") {
            pos_ += 5;
            return Value::makeBool(false);
        }
        return Value::makeNull();
    }

    Value parseNumber() {
        size_t start = pos_;
        if (peek() == '-') pos_++;
        
        while (std::isdigit(peek())) pos_++;
        
        if (peek() == '.') {
            pos_++;
            while (std::isdigit(peek())) pos_++;
        }
        
        if (peek() == 'e' || peek() == 'E') {
            pos_++;
            if (peek() == '+' || peek() == '-') pos_++;
            while (std::isdigit(peek())) pos_++;
        }
        
        std::string numStr = input_.substr(start, pos_ - start);
        double num = std::stod(numStr);
        return Value::makeNumber(num);
    }

    Value parseString() {
        get(); // consume opening quote
        std::string result;
        
        while (pos_ < input_.size()) {
            char c = get();
            if (c == '"') {
                return Value::makeString(result);
            }
            if (c == '\\') {
                char escaped = get();
                switch (escaped) {
                    case '"': result += '"'; break;
                    case '\\': result += '\\'; break;
                    case '/': result += '/'; break;
                    case 'b': result += '\b'; break;
                    case 'f': result += '\f'; break;
                    case 'n': result += '\n'; break;
                    case 'r': result += '\r'; break;
                    case 't': result += '\t'; break;
                    case 'u': {
                        // Parse 4 hex digits
                        std::string hex = input_.substr(pos_, 4);
                        pos_ += 4;
                        unsigned int codepoint = std::stoul(hex, nullptr, 16);
                        if (codepoint < 0x80) {
                            result += static_cast<char>(codepoint);
                        } else if (codepoint < 0x800) {
                            result += static_cast<char>(0xC0 | (codepoint >> 6));
                            result += static_cast<char>(0x80 | (codepoint & 0x3F));
                        } else {
                            result += static_cast<char>(0xE0 | (codepoint >> 12));
                            result += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
                            result += static_cast<char>(0x80 | (codepoint & 0x3F));
                        }
                        break;
                    }
                    default:
                        result += escaped;
                }
            } else {
                result += c;
            }
        }
        
        return Value::makeString(result);
    }

    Value parseArray() {
        get(); // consume '['
        Value arr = Value::makeArray();
        
        skipWhitespace();
        if (peek() == ']') {
            get();
            return arr;
        }
        
        while (true) {
            arr.array.push_back(parseValue());
            skipWhitespace();
            
            if (peek() == ']') {
                get();
                break;
            }
            if (peek() != ',') break;
            get(); // consume ','
        }
        
        return arr;
    }

    Value parseObject() {
        get(); // consume '{'
        Value obj = Value::makeObject();
        
        skipWhitespace();
        if (peek() == '}') {
            get();
            return obj;
        }
        
        while (true) {
            skipWhitespace();
            Value key = parseString();
            
            skipWhitespace();
            if (peek() != ':') break;
            get(); // consume ':'
            
            skipWhitespace();
            Value value = parseValue();
            
            obj.object[key.string] = value;
            
            skipWhitespace();
            if (peek() == '}') {
                get();
                break;
            }
            if (peek() != ',') break;
            get(); // consume ','
        }
        
        return obj;
    }
};

// JSON Serializer
class Serializer {
public:
    static std::string serialize(const Value& v, bool pretty = false, int indent = 0) {
        std::ostringstream oss;
        serializeValue(v, oss, pretty, indent);
        return oss.str();
    }

private:
    static void serializeValue(const Value& v, std::ostringstream& oss, bool pretty, int indent) {
        switch (v.type) {
            case Type::Null:
                oss << "null";
                break;
            case Type::Boolean:
                oss << (v.boolean ? "true" : "false");
                break;
            case Type::Number: {
                // Check if it's an integer
                if (v.number == static_cast<int64_t>(v.number)) {
                    oss << static_cast<int64_t>(v.number);
                } else {
                    oss << std::setprecision(15) << v.number;
                }
                break;
            }
            case Type::String:
                serializeString(v.string, oss);
                break;
            case Type::Array:
                oss << "[";
                for (size_t i = 0; i < v.array.size(); i++) {
                    if (i > 0) oss << ",";
                    if (pretty) oss << "\n" << std::string(indent + 2, ' ');
                    serializeValue(v.array[i], oss, pretty, indent + 2);
                }
                if (pretty && !v.array.empty()) oss << "\n" << std::string(indent, ' ');
                oss << "]";
                break;
            case Type::Object:
                oss << "{";
                {
                    bool first = true;
                    for (const auto& kv : v.object) {
                        if (!first) oss << ",";
                        first = false;
                        if (pretty) oss << "\n" << std::string(indent + 2, ' ');
                        serializeString(kv.first, oss);
                        oss << ":";
                        if (pretty) oss << " ";
                        serializeValue(kv.second, oss, pretty, indent + 2);
                    }
                    if (pretty && !v.object.empty()) oss << "\n" << std::string(indent, ' ');
                }
                oss << "}";
                break;
        }
    }

    static void serializeString(const std::string& s, std::ostringstream& oss) {
        oss << '"';
        for (char c : s) {
            switch (c) {
                case '"': oss << "\\\""; break;
                case '\\': oss << "\\\\"; break;
                case '\b': oss << "\\b"; break;
                case '\f': oss << "\\f"; break;
                case '\n': oss << "\\n"; break;
                case '\r': oss << "\\r"; break;
                case '\t': oss << "\\t"; break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        oss << "\\u" << std::hex << std::setfill('0') << std::setw(4) << static_cast<int>(c);
                    } else {
                        oss << c;
                    }
            }
        }
        oss << '"';
    }
};

// Path parsing and navigation
struct PathSegment {
    bool isArray;
    std::string key;
    int index;
    
    static PathSegment makeKey(const std::string& k) {
        PathSegment s;
        s.isArray = false;
        s.key = k;
        s.index = -1;
        return s;
    }
    
    static PathSegment makeIndex(int i) {
        PathSegment s;
        s.isArray = true;
        s.key = "";
        s.index = i;
        return s;
    }
};

std::vector<PathSegment> parsePath(const std::string& path) {
    std::vector<PathSegment> segments;
    
    if (path.empty() || path == "$") {
        return segments;
    }
    
    size_t pos = 1; // skip '$'
    
    while (pos < path.size()) {
        if (path[pos] == '.') {
            pos++;
            size_t end = path.find_first_of(".[", pos);
            if (end == std::string::npos) end = path.size();
            std::string key = path.substr(pos, end - pos);
            segments.push_back(PathSegment::makeKey(key));
            pos = end;
        } else if (path[pos] == '[') {
            pos++;
            size_t end = path.find(']', pos);
            if (end == std::string::npos) break;
            
            std::string idxStr = path.substr(pos, end - pos);
            if (idxStr == "#") {
                segments.push_back(PathSegment::makeIndex(-1));
            } else if (!idxStr.empty() && idxStr[0] == '#') {
                // #-N syntax for reverse index
                int idx = std::stoi(idxStr.substr(1));
                segments.push_back(PathSegment::makeIndex(idx));
            } else {
                int idx = std::stoi(idxStr);
                segments.push_back(PathSegment::makeIndex(idx));
            }
            pos = end + 1;
        } else {
            break;
        }
    }
    
    return segments;
}

Value* getAtPath(Value* root, const std::vector<PathSegment>& segments) {
    Value* current = root;
    
    for (const auto& seg : segments) {
        if (!current) return nullptr;
        
        if (seg.isArray) {
            if (!current->isArray()) return nullptr;
            
            int idx = seg.index;
            if (idx < 0) {
                idx = current->array.size() + idx;
            }
            
            if (idx < 0 || static_cast<size_t>(idx) >= current->array.size()) {
                return nullptr;
            }
            current = &current->array[idx];
        } else {
            if (!current->isObject()) return nullptr;
            
            auto it = current->object.find(seg.key);
            if (it == current->object.end()) {
                return nullptr;
            }
            current = &it->second;
        }
    }
    
    return current;
}

const Value* getAtPathConst(const Value* root, const std::vector<PathSegment>& segments) {
    const Value* current = root;
    
    for (const auto& seg : segments) {
        if (!current) return nullptr;
        
        if (seg.isArray) {
            if (!current->isArray()) return nullptr;
            
            int idx = seg.index;
            if (idx < 0) {
                idx = current->array.size() + idx;
            }
            
            if (idx < 0 || static_cast<size_t>(idx) >= current->array.size()) {
                return nullptr;
            }
            current = &current->array[idx];
        } else {
            if (!current->isObject()) return nullptr;
            
            auto it = current->object.find(seg.key);
            if (it == current->object.end()) {
                return nullptr;
            }
            current = &it->second;
        }
    }
    
    return current;
}

std::string getTypeString(const Value& v) {
    switch (v.type) {
        case Type::Null: return "null";
        case Type::Boolean: return v.boolean ? "true" : "false";
        case Type::Number:
            if (v.number == static_cast<int64_t>(v.number)) {
                return "integer";
            }
            return "real";
        case Type::String: return "text";
        case Type::Array: return "array";
        case Type::Object: return "object";
    }
    return "null";
}

} // namespace svdb_json

// C API Implementation
extern "C" {

int svdb_json_validate(const char* json_str) {
    if (!json_str) return 0;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value v = parser.parse();
        return !v.isNull() || strcmp(json_str, "null") == 0;
    } catch (...) {
        return 0;
    }
}

char* svdb_json_minify(const char* json_str) {
    if (!json_str) return nullptr;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value v = parser.parse();
        std::string result = svdb_json::Serializer::serialize(v, false);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

char* svdb_json_pretty(const char* json_str) {
    if (!json_str) return nullptr;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value v = parser.parse();
        std::string result = svdb_json::Serializer::serialize(v, true);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

char* svdb_json_type(const char* json_str, const char* path) {
    if (!json_str || !path) return nullptr;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value v = parser.parse();
        
        std::vector<svdb_json::PathSegment> segments = svdb_json::parsePath(path);
        const svdb_json::Value* target = svdb_json::getAtPathConst(&v, segments);
        
        if (!target) return nullptr;
        
        std::string typeStr = svdb_json::getTypeString(*target);
        char* out = static_cast<char*>(std::malloc(typeStr.size() + 1));
        if (out) {
            std::strcpy(out, typeStr.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

int64_t svdb_json_length(const char* json_str, const char* path) {
    if (!json_str) return -1;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value v = parser.parse();
        
        const svdb_json::Value* target = &v;
        if (path && strlen(path) > 0 && strcmp(path, "$") != 0) {
            std::vector<svdb_json::PathSegment> segments = svdb_json::parsePath(path);
            target = svdb_json::getAtPathConst(&v, segments);
            if (!target) return -1;
        }
        
        return static_cast<int64_t>(target->size());
    } catch (...) {
        return -1;
    }
}

char* svdb_json_extract(const char* json_str, const char* path) {
    if (!json_str || !path) return nullptr;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value v = parser.parse();
        
        std::vector<svdb_json::PathSegment> segments = svdb_json::parsePath(path);
        const svdb_json::Value* target = svdb_json::getAtPathConst(&v, segments);
        
        if (!target) return nullptr;
        
        // For scalar values, return the value directly
        if (target->isString()) {
            char* out = static_cast<char*>(std::malloc(target->string.size() + 1));
            if (out) {
                std::strcpy(out, target->string.c_str());
            }
            return out;
        }
        
        if (target->isNumber()) {
            std::ostringstream oss;
            if (target->number == static_cast<int64_t>(target->number)) {
                oss << static_cast<int64_t>(target->number);
            } else {
                oss << std::setprecision(15) << target->number;
            }
            std::string result = oss.str();
            char* out = static_cast<char*>(std::malloc(result.size() + 1));
            if (out) {
                std::strcpy(out, result.c_str());
            }
            return out;
        }
        
        if (target->isBool()) {
            std::string result = target->boolean ? "true" : "false";
            char* out = static_cast<char*>(std::malloc(result.size() + 1));
            if (out) {
                std::strcpy(out, result.c_str());
            }
            return out;
        }
        
        if (target->isNull()) {
            char* out = static_cast<char*>(std::malloc(5));
            if (out) {
                std::strcpy(out, "null");
            }
            return out;
        }
        
        // For arrays and objects, return JSON string
        std::string result = svdb_json::Serializer::serialize(*target, false);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

char* svdb_json_extract_multi(const char* json_str, const char** paths, int n_paths) {
    if (!json_str || !paths || n_paths < 1) return nullptr;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value v = parser.parse();
        
        svdb_json::Value result = svdb_json::Value::makeArray();
        
        for (int i = 0; i < n_paths; i++) {
            std::vector<svdb_json::PathSegment> segments = svdb_json::parsePath(paths[i]);
            const svdb_json::Value* target = svdb_json::getAtPathConst(&v, segments);
            
            if (target) {
                result.array.push_back(*target);
            } else {
                result.array.push_back(svdb_json::Value::makeNull());
            }
        }
        
        std::string serialized = svdb_json::Serializer::serialize(result, false);
        char* out = static_cast<char*>(std::malloc(serialized.size() + 1));
        if (out) {
            std::strcpy(out, serialized.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

char* svdb_json_array(const char** values, int n_values) {
    if (!values || n_values < 0) return nullptr;

    try {
        svdb_json::Value arr = svdb_json::Value::makeArray();

        for (int i = 0; i < n_values; i++) {
            if (values[i]) {
                // Try to parse as JSON, otherwise treat as string
                const char* val = values[i];
                bool parsed = false;
                
                // Check for object, array, null, boolean
                if (val[0] == '{' || val[0] == '[' ||
                    strcmp(val, "null") == 0 || strcmp(val, "true") == 0 ||
                    strcmp(val, "false") == 0) {
                    svdb_json::Parser parser(val);
                    svdb_json::Value v = parser.parse();
                    arr.array.push_back(v);
                    parsed = true;
                }
                
                // Check for number (integer or real)
                if (!parsed) {
                    char* endptr = nullptr;
                    double num = strtod(val, &endptr);
                    if (endptr > val && *endptr == '\0') {
                        // Valid number
                        arr.array.push_back(svdb_json::Value::makeNumber(num));
                        parsed = true;
                    }
                }
                
                // Default: treat as string
                if (!parsed) {
                    arr.array.push_back(svdb_json::Value::makeString(val));
                }
            } else {
                arr.array.push_back(svdb_json::Value::makeNull());
            }
        }

        std::string result = svdb_json::Serializer::serialize(arr, false);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

char* svdb_json_object(const char** keys, const char** values, int n_pairs) {
    if (!keys || !values || n_pairs < 0) return nullptr;
    
    try {
        svdb_json::Value obj = svdb_json::Value::makeObject();
        
        for (int i = 0; i < n_pairs; i++) {
            if (keys[i] && values[i]) {
                // Try to parse value as JSON, otherwise treat as string
                if (values[i][0] == '{' || values[i][0] == '[' || 
                    strcmp(values[i], "null") == 0 || strcmp(values[i], "true") == 0 ||
                    strcmp(values[i], "false") == 0) {
                    svdb_json::Parser parser(values[i]);
                    svdb_json::Value v = parser.parse();
                    obj.object[keys[i]] = v;
                } else {
                    obj.object[keys[i]] = svdb_json::Value::makeString(values[i]);
                }
            }
        }
        
        std::string result = svdb_json::Serializer::serialize(obj, false);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

// Deep copy a value
svdb_json::Value deepCopy(const svdb_json::Value& v) {
    svdb_json::Value copy;
    copy.type = v.type;
    copy.boolean = v.boolean;
    copy.number = v.number;
    copy.string = v.string;
    
    if (v.type == svdb_json::Type::Array) {
        for (const auto& elem : v.array) {
            copy.array.push_back(deepCopy(elem));
        }
    } else if (v.type == svdb_json::Type::Object) {
        for (const auto& kv : v.object) {
            copy.object[kv.first] = deepCopy(kv.second);
        }
    }
    
    return copy;
}

// Set value at path
bool setAtPath(svdb_json::Value& root, const std::vector<svdb_json::PathSegment>& segments, 
               const svdb_json::Value& newValue, bool replaceOnly) {
    if (segments.empty()) {
        root = newValue;
        return true;
    }
    
    const auto& first = segments[0];
    std::vector<svdb_json::PathSegment> rest(segments.begin() + 1, segments.end());
    
    if (first.isArray) {
        if (!root.isArray()) return false;
        
        int idx = first.index;
        if (idx < 0) {
            idx = root.array.size() + idx;
        }
        
        if (idx < 0 || static_cast<size_t>(idx) >= root.array.size()) {
            return !replaceOnly; // Can only append if not replace-only
        }
        
        if (rest.empty()) {
            if (first.index == -1) {
                root.array.push_back(newValue);
            } else {
                root.array[idx] = newValue;
            }
        } else {
            return setAtPath(root.array[idx], rest, newValue, replaceOnly);
        }
    } else {
        if (!root.isObject()) return false;
        
        auto it = root.object.find(first.key);
        bool exists = (it != root.object.end());
        
        if (replaceOnly && !exists) {
            return false;
        }
        
        if (rest.empty()) {
            root.object[first.key] = newValue;
        } else {
            if (!exists) {
                root.object[first.key] = svdb_json::Value::makeObject();
            }
            return setAtPath(root.object[first.key], rest, newValue, replaceOnly);
        }
    }
    
    return true;
}

// Remove value at path
bool removeAtPath(svdb_json::Value& root, const std::vector<svdb_json::PathSegment>& segments) {
    if (segments.empty()) {
        root = svdb_json::Value::makeNull();
        return true;
    }
    
    const auto& first = segments[0];
    std::vector<svdb_json::PathSegment> rest(segments.begin() + 1, segments.end());
    
    if (first.isArray) {
        if (!root.isArray()) return false;
        
        int idx = first.index;
        if (idx < 0) {
            idx = root.array.size() + idx;
        }
        
        if (idx < 0 || static_cast<size_t>(idx) >= root.array.size()) {
            return false;
        }
        
        if (rest.empty()) {
            root.array.erase(root.array.begin() + idx);
        } else {
            return removeAtPath(root.array[idx], rest);
        }
    } else {
        if (!root.isObject()) return false;
        
        auto it = root.object.find(first.key);
        if (it == root.object.end()) return false;
        
        if (rest.empty()) {
            root.object.erase(it);
        } else {
            return removeAtPath(it->second, rest);
        }
    }
    
    return true;
}

char* svdb_json_set(const char* json_str, const char** path_value_pairs, int n_pairs) {
    if (!json_str || !path_value_pairs || n_pairs < 1) return nullptr;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value root = parser.parse();
        
        for (int i = 0; i < n_pairs; i++) {
            std::string pathStr(path_value_pairs[i * 2]);
            std::string valueStr(path_value_pairs[i * 2 + 1]);
            
            std::vector<svdb_json::PathSegment> segments = svdb_json::parsePath(pathStr);
            
            // Parse value
            svdb_json::Value value;
            if (valueStr[0] == '{' || valueStr[0] == '[' || 
                valueStr == "null" || valueStr == "true" || valueStr == "false") {
                svdb_json::Parser vparser(valueStr.c_str());
                value = vparser.parse();
            } else if ((valueStr[0] >= '0' && valueStr[0] <= '9') || valueStr[0] == '-') {
                value = svdb_json::Value::makeNumber(std::stod(valueStr));
            } else {
                value = svdb_json::Value::makeString(valueStr);
            }
            
            setAtPath(root, segments, value, false);
        }
        
        std::string result = svdb_json::Serializer::serialize(root, false);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

char* svdb_json_replace(const char* json_str, const char** path_value_pairs, int n_pairs) {
    if (!json_str || !path_value_pairs || n_pairs < 1) return nullptr;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value root = parser.parse();
        
        for (int i = 0; i < n_pairs; i++) {
            std::string pathStr(path_value_pairs[i * 2]);
            std::string valueStr(path_value_pairs[i * 2 + 1]);
            
            std::vector<svdb_json::PathSegment> segments = svdb_json::parsePath(pathStr);
            
            // Parse value
            svdb_json::Value value;
            if (valueStr[0] == '{' || valueStr[0] == '[' || 
                valueStr == "null" || valueStr == "true" || valueStr == "false") {
                svdb_json::Parser vparser(valueStr.c_str());
                value = vparser.parse();
            } else if ((valueStr[0] >= '0' && valueStr[0] <= '9') || valueStr[0] == '-') {
                value = svdb_json::Value::makeNumber(std::stod(valueStr));
            } else {
                value = svdb_json::Value::makeString(valueStr);
            }
            
            setAtPath(root, segments, value, true);
        }
        
        std::string result = svdb_json::Serializer::serialize(root, false);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

char* svdb_json_remove(const char* json_str, const char** paths, int n_paths) {
    if (!json_str || !paths || n_paths < 1) return nullptr;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value root = parser.parse();
        
        for (int i = 0; i < n_paths; i++) {
            std::string pathStr(paths[i]);
            std::vector<svdb_json::PathSegment> segments = svdb_json::parsePath(pathStr);
            removeAtPath(root, segments);
        }
        
        std::string result = svdb_json::Serializer::serialize(root, false);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

char* svdb_json_quote(const char* value) {
    if (!value) return nullptr;
    
    try {
        // Check if already valid JSON
        if (value[0] == '{' || value[0] == '[' || 
            strcmp(value, "null") == 0 || strcmp(value, "true") == 0 ||
            strcmp(value, "false") == 0 ||
            (value[0] >= '0' && value[0] <= '9') || value[0] == '-') {
            svdb_json::Parser parser(value);
            svdb_json::Value v = parser.parse();
            std::string result = svdb_json::Serializer::serialize(v, false);
            char* out = static_cast<char*>(std::malloc(result.size() + 1));
            if (out) {
                std::strcpy(out, result.c_str());
            }
            return out;
        }
        
        // Otherwise, treat as string and quote it
        svdb_json::Value strVal = svdb_json::Value::makeString(value);
        std::string result = svdb_json::Serializer::serialize(strVal, false);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

char* svdb_json_keys(const char* json_str, const char* path) {
    if (!json_str) return nullptr;
    
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value v = parser.parse();
        
        const svdb_json::Value* target = &v;
        if (path && strlen(path) > 0 && strcmp(path, "$") != 0) {
            std::vector<svdb_json::PathSegment> segments = svdb_json::parsePath(path);
            target = svdb_json::getAtPathConst(&v, segments);
            if (!target) return nullptr;
        }
        
        if (!target->isObject()) return nullptr;
        
        svdb_json::Value keys = svdb_json::Value::makeArray();
        for (const auto& kv : target->object) {
            keys.array.push_back(svdb_json::Value::makeString(kv.first));
        }
        
        std::string result = svdb_json::Serializer::serialize(keys, false);
        char* out = static_cast<char*>(std::malloc(result.size() + 1));
        if (out) {
            std::strcpy(out, result.c_str());
        }
        return out;
    } catch (...) {
        return nullptr;
    }
}

int svdb_json_patch(char* dest, size_t dest_size, const char* target, const char* patch) {
    if (!dest || !target || !patch) return -1;
    
    try {
        svdb_json::Parser tparser(target);
        svdb_json::Value tval = tparser.parse();
        
        svdb_json::Parser pparser(patch);
        svdb_json::Value pval = pparser.parse();
        
        // RFC 7396 Merge Patch
        if (!pval.isObject()) {
            std::string result = svdb_json::Serializer::serialize(pval, false);
            if (result.size() >= dest_size) return -1;
            std::strcpy(dest, result.c_str());
            return 0;
        }
        
        if (!tval.isObject()) {
            tval = svdb_json::Value::makeObject();
        }
        
        // Apply patch
        for (const auto& kv : pval.object) {
            if (kv.second.isNull()) {
                tval.object.erase(kv.first);
            } else {
                tval.object[kv.first] = kv.second;
            }
        }
        
        std::string result = svdb_json::Serializer::serialize(tval, false);
        if (result.size() >= dest_size) return -1;
        std::strcpy(dest, result.c_str());
        return 0;
    } catch (...) {
        return -1;
    }
}

void svdb_json_free(char* ptr) {
    if (ptr) {
        std::free(ptr);
    }
}

static char* dup_str(const std::string& s) {
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (p) std::strcpy(p, s.c_str());
    return p;
}

static std::string json_type_name(const svdb_json::Value& v) {
    switch (v.type) {
        case svdb_json::Type::Null:    return "null";
        case svdb_json::Type::Boolean: return "true";  /* SQLite uses "true"/"false" */
        case svdb_json::Type::Number:
            if (v.number == static_cast<int64_t>(v.number)) return "integer";
            return "real";
        case svdb_json::Type::String:  return "text";
        case svdb_json::Type::Array:   return "array";
        case svdb_json::Type::Object:  return "object";
    }
    return "null";
}

static std::string json_atom_str(const svdb_json::Value& v) {
    switch (v.type) {
        case svdb_json::Type::Null:    return "null";
        case svdb_json::Type::Boolean: return v.boolean ? "true" : "false";
        case svdb_json::Type::Number: {
            std::ostringstream oss;
            if (v.number == static_cast<int64_t>(v.number)) {
                oss << static_cast<int64_t>(v.number);
            } else {
                oss << std::setprecision(15) << v.number;
            }
            return oss.str();
        }
        case svdb_json::Type::String:  return v.string;
        default: return "";  /* arrays/objects have no atom */
    }
}

/* json_array_insert(json, path, value) - insert value before element at path */
char* svdb_json_array_insert(const char* json_str, const char* path, const char* value) {
    if (!json_str || !path || !value) return nullptr;
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value root = parser.parse();

        /* Parse path: expect $[N] syntax */
        std::string p(path);
        size_t lb = p.find('[');
        if (lb == std::string::npos) return nullptr;
        size_t rb = p.find(']', lb);
        if (rb == std::string::npos) return nullptr;
        std::string idx_str = p.substr(lb + 1, rb - lb - 1);
        int idx = std::stoi(idx_str);

        /* Navigate to parent */
        std::string parent_path = p.substr(0, lb);
        svdb_json::Value* target = &root;
        if (parent_path != "$" && parent_path.size() > 1) {
            std::vector<svdb_json::PathSegment> segs = svdb_json::parsePath(parent_path);
            target = svdb_json::getAtPath(&root, segs);
            if (!target) return nullptr;
        }

        if (!target->isArray()) return nullptr;
        if (idx < 0) idx = (int)target->array.size() + 1 + idx;
        if (idx < 0) idx = 0;
        if (idx > (int)target->array.size()) idx = (int)target->array.size();

        /* Parse the value to insert */
        svdb_json::Value ins;
        std::string vs(value);
        /* Try to parse as JSON, fallback to string */
        try {
            svdb_json::Parser vp(vs);
            ins = vp.parse();
        } catch (...) {
            ins = svdb_json::Value::makeString(vs);
        }

        target->array.insert(target->array.begin() + idx, ins);

        std::string result = svdb_json::Serializer::serialize(root, false);
        return dup_str(result);
    } catch (...) {
        return nullptr;
    }
}

/* json_each(json) - shallow iterator over array or object top-level elements */
svdb_json_tvf_rows_t* svdb_json_each(const char* json_str) {
    if (!json_str) return nullptr;
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value root = parser.parse();

        svdb_json_tvf_rows_t* result = static_cast<svdb_json_tvf_rows_t*>(
            std::malloc(sizeof(svdb_json_tvf_rows_t)));
        if (!result) return nullptr;

        std::vector<svdb_json_tvf_row_t> rows_vec;
        int64_t id = 1;

        auto make_row = [&](const std::string& key_str, const svdb_json::Value& v,
                             const std::string& fullkey_str, const std::string& path_str) {
            svdb_json_tvf_row_t row;
            row.key     = dup_str(key_str);
            std::string val_s = svdb_json::Serializer::serialize(v, false);
            row.value   = dup_str(val_s);
            row.type    = dup_str(json_type_name(v));
            bool scalar = !v.isArray() && !v.isObject();
            row.atom    = scalar ? dup_str(json_atom_str(v)) : nullptr;
            row.id      = id++;
            row.parent  = -1;
            row.fullkey = dup_str(fullkey_str);
            row.path    = dup_str(path_str);
            rows_vec.push_back(row);
        };

        if (root.isArray()) {
            for (size_t i = 0; i < root.array.size(); ++i) {
                std::string key_s = std::to_string(i);
                std::string fk = "$[" + key_s + "]";
                make_row(key_s, root.array[i], fk, "$");
            }
        } else if (root.isObject()) {
            for (const auto& kv : root.object) {
                std::string fk = "$." + kv.first;
                make_row(kv.first, kv.second, fk, "$");
            }
        }

        result->count = (int)rows_vec.size();
        if (result->count > 0) {
            result->rows = static_cast<svdb_json_tvf_row_t*>(
                std::malloc(result->count * sizeof(svdb_json_tvf_row_t)));
            if (!result->rows) { std::free(result); return nullptr; }
            for (int i = 0; i < result->count; ++i)
                result->rows[i] = rows_vec[i];
        } else {
            result->rows = nullptr;
        }
        return result;
    } catch (...) {
        return nullptr;
    }
}

/* Recursive helper for json_tree */
static void json_tree_recurse(const svdb_json::Value& v,
                               const std::string& fullkey,
                               const std::string& path_str,
                               const std::string& key_str,
                               int64_t parent_id,
                               std::vector<svdb_json_tvf_row_t>& rows_vec,
                               int64_t& id) {
    svdb_json_tvf_row_t row;
    row.key     = dup_str(key_str);
    std::string val_s = svdb_json::Serializer::serialize(v, false);
    row.value   = dup_str(val_s);
    row.type    = dup_str(json_type_name(v));
    bool scalar = !v.isArray() && !v.isObject();
    row.atom    = scalar ? dup_str(json_atom_str(v)) : nullptr;
    row.id      = id;
    row.parent  = parent_id;
    row.fullkey = dup_str(fullkey);
    row.path    = dup_str(path_str);
    int64_t my_id = id++;
    rows_vec.push_back(row);

    if (v.isArray()) {
        for (size_t i = 0; i < v.array.size(); ++i) {
            std::string child_key = std::to_string(i);
            std::string child_fk  = fullkey + "[" + child_key + "]";
            json_tree_recurse(v.array[i], child_fk, fullkey, child_key, my_id, rows_vec, id);
        }
    } else if (v.isObject()) {
        for (const auto& kv : v.object) {
            std::string child_fk = fullkey + "." + kv.first;
            json_tree_recurse(kv.second, child_fk, fullkey, kv.first, my_id, rows_vec, id);
        }
    }
}

/* json_tree(json) - recursive iterator over all JSON nodes */
svdb_json_tvf_rows_t* svdb_json_tree(const char* json_str) {
    if (!json_str) return nullptr;
    try {
        svdb_json::Parser parser(json_str);
        svdb_json::Value root = parser.parse();

        svdb_json_tvf_rows_t* result = static_cast<svdb_json_tvf_rows_t*>(
            std::malloc(sizeof(svdb_json_tvf_rows_t)));
        if (!result) return nullptr;

        std::vector<svdb_json_tvf_row_t> rows_vec;
        int64_t id = 1;
        json_tree_recurse(root, "$", "", "$", -1, rows_vec, id);

        result->count = (int)rows_vec.size();
        if (result->count > 0) {
            result->rows = static_cast<svdb_json_tvf_row_t*>(
                std::malloc(result->count * sizeof(svdb_json_tvf_row_t)));
            if (!result->rows) { std::free(result); return nullptr; }
            for (int i = 0; i < result->count; ++i)
                result->rows[i] = rows_vec[i];
        } else {
            result->rows = nullptr;
        }
        return result;
    } catch (...) {
        return nullptr;
    }
}

void svdb_json_tvf_rows_free(svdb_json_tvf_rows_t* rows) {
    if (!rows) return;
    for (int i = 0; i < rows->count; ++i) {
        svdb_json_tvf_row_t& r = rows->rows[i];
        std::free(r.key);
        std::free(r.value);
        std::free(r.type);
        if (r.atom)    std::free(r.atom);
        std::free(r.fullkey);
        std::free(r.path);
    }
    if (rows->rows) std::free(rows->rows);
    std::free(rows);
}

} // extern "C"
