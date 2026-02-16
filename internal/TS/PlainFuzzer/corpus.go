package PlainFuzzer

import (
	"os"
	"path/filepath"
)

type Corpus struct {
	Path string
}

func NewCorpus(path string) *Corpus {
	return &Corpus{Path: path}
}

func (c *Corpus) Add(sql string) error {
	filename := filepath.Join(c.Path, sanitizeFilename(sql))
	return os.WriteFile(filename, []byte(sql), 0644)
}

func sanitizeFilename(sql string) string {
	result := make([]byte, 0, len(sql))
	for i, r := range sql {
		if i > 50 {
			break
		}
		b := byte(r)
		switch {
		case b >= 'a' && b <= 'z':
			result = append(result, b)
		case b >= 'A' && b <= 'Z':
			result = append(result, b)
		case b >= '0' && b <= '9':
			result = append(result, b)
		case b == ' ' || b == '_':
			result = append(result, '_')
		default:
			result = append(result, '_')
		}
	}
	if len(result) == 0 {
		result = []byte("query")
	}
	return string(result) + ".sql"
}

func (c *Corpus) Load() ([]string, error) {
	entries, err := os.ReadDir(c.Path)
	if err != nil {
		return nil, err
	}

	var queries []string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".sql" {
			data, err := os.ReadFile(filepath.Join(c.Path, entry.Name()))
			if err != nil {
				continue
			}
			queries = append(queries, string(data))
		}
	}
	return queries, nil
}
