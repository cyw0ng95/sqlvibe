package sqlvibe

// getRowMap retrieves a cleared map[string]interface{} from the pool.
// The caller must call putRowMap when done to return it to the pool.
func getRowMap() map[string]interface{} {
	m := mapPool.Get().(map[string]interface{})
	for k := range m {
		delete(m, k)
	}
	return m
}

// putRowMap returns a map to the pool. The map should not be used after this call.
func putRowMap(m map[string]interface{}) {
	mapPool.Put(m)
}
