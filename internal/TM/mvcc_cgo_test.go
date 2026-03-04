package TM

import (
	"testing"
)

func TestMVCCStoreCGO_Basic(t *testing.T) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Put a value
	commitID := store.Put(key, value)
	if commitID != 1 {
		t.Errorf("Expected commit ID 1, got %d", commitID)
	}

	// Create snapshot and get value
	snap := store.Snapshot()
	defer snap.Free()

	got, found := store.Get(key, snap)
	if !found {
		t.Fatal("Expected to find key")
	}
	if string(got) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(got))
	}
}

func TestMVCCStoreCGO_SnapshotIsolation(t *testing.T) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	key := []byte("test-key")

	// Put initial value
	store.Put(key, []byte("value1"))

	// Create snapshot at commit 1
	snap1 := store.Snapshot()
	defer snap1.Free()

	// Put new value
	store.Put(key, []byte("value2"))

	// Snapshot should still see value1
	got, found := store.Get(key, snap1)
	if !found {
		t.Fatal("Expected to find key in snapshot")
	}
	if string(got) != "value1" {
		t.Errorf("Expected value1 in snapshot, got %s", string(got))
	}

	// Current store should see value2
	snap2 := store.Snapshot()
	defer snap2.Free()
	got, found = store.Get(key, snap2)
	if !found {
		t.Fatal("Expected to find key")
	}
	if string(got) != "value2" {
		t.Errorf("Expected value2, got %s", string(got))
	}
}

func TestMVCCStoreCGO_Delete(t *testing.T) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Put and delete
	store.Put(key, value)
	store.Delete(key)

	// Should not find deleted key
	snap := store.Snapshot()
	defer snap.Free()

	_, found := store.Get(key, snap)
	if found {
		t.Error("Expected deleted key to not be found")
	}
}

func TestMVCCStoreCGO_GC(t *testing.T) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	// Add multiple versions
	for i := 0; i < 10; i++ {
		key := []byte("key")
		value := []byte("value")
		store.Put(key, value)
	}

	// GC should prune old versions
	pruned := store.GC(5)
	if pruned == 0 {
		t.Error("Expected some versions to be pruned")
	}
}

func TestMVCCStoreCGO_MultipleKeys(t *testing.T) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	// Add multiple keys
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
	}

	for i := range keys {
		store.Put(keys[i], values[i])
	}

	// Verify all keys
	snap := store.Snapshot()
	defer snap.Free()

	for i := range keys {
		got, found := store.Get(keys[i], snap)
		if !found {
			t.Errorf("Expected to find key %s", string(keys[i]))
		}
		if string(got) != string(values[i]) {
			t.Errorf("Expected %s, got %s", string(values[i]), string(got))
		}
	}

	if store.KeyCount() != 3 {
		t.Errorf("Expected 3 keys, got %d", store.KeyCount())
	}
}

func TestMVCCStoreCGO_EmptyKey(t *testing.T) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	// Empty key should not crash
	commitID := store.Put([]byte{}, []byte("value"))
	if commitID != 0 {
		t.Error("Expected commit ID 0 for empty key")
	}

	snap := store.Snapshot()
	defer snap.Free()

	_, found := store.Get([]byte{}, snap)
	if found {
		t.Error("Expected empty key to not be found")
	}
}

func TestMVCCStoreCGO_ConcurrentReads(t *testing.T) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Put initial value
	store.Put(key, value)

	// Create multiple snapshots
	snapshots := make([]*SnapshotCGO, 10)
	for i := range snapshots {
		snapshots[i] = store.Snapshot()
	}

	// All snapshots should see the same value
	for i, snap := range snapshots {
		got, found := store.Get(key, snap)
		if !found {
			t.Errorf("Snapshot %d: expected to find key", i)
		}
		if string(got) != string(value) {
			t.Errorf("Snapshot %d: expected %s, got %s", i, string(value), string(got))
		}
	}

	// Free all snapshots
	for _, snap := range snapshots {
		snap.Free()
	}
}

func BenchmarkMVCCStoreCGO_Put(b *testing.B) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	key := []byte("benchmark-key")
	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Put(key, value)
	}
}

func BenchmarkMVCCStoreCGO_Get(b *testing.B) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	key := []byte("benchmark-key")
	value := []byte("benchmark-value")

	store.Put(key, value)
	snap := store.Snapshot()
	defer snap.Free()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Get(key, snap)
	}
}

func BenchmarkMVCCStoreCGO_Snapshot(b *testing.B) {
	store := NewMVCCStoreCGO()
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap := store.Snapshot()
		snap.Free()
	}
}
