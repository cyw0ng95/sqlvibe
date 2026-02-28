package DS

// TableModule provides default (no-op) implementations for optional VTab methods.
// Embed this in your VTabModule implementation to avoid implementing every method.
type TableModule struct{}

// BestIndex provides a default no-op BestIndex implementation.
func (m *TableModule) BestIndex(info *IndexInfo) error { return nil }
