package storage

import (
	"runtime"
	"sync"
)

const (
	// ParallelThreshold is the minimum row count before parallelism kicks in.
	ParallelThreshold = 10000
	// MinPartitionSize is the minimum rows per worker partition.
	MinPartitionSize = 1000
)

// numCores is set at package init.
var numCores int

func init() {
	numCores = runtime.GOMAXPROCS(0)
}

// GetNumCores returns the number of logical CPUs available.
func GetNumCores() int { return numCores }

// shouldParallelize returns true when the dataset is large enough and multiple
// cores are available.
func shouldParallelize(rowCount int) bool {
	return rowCount > ParallelThreshold && numCores > 1
}

// getNumWorkers returns the optimal number of workers for the given row count.
func getNumWorkers(rowCount int) int {
	if rowCount < ParallelThreshold {
		return 1
	}
	maxWorkers := rowCount / MinPartitionSize
	if maxWorkers > numCores {
		maxWorkers = numCores
	}
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	return maxWorkers
}

// ParallelCount returns the COUNT(*) for the store using multiple goroutines when
// the dataset is large enough.
func (hs *HybridStore) ParallelCount() int64 {
	return int64(hs.LiveCount())
}

// ParallelSum sums int64/float64 values in the named column using goroutines when
// the dataset is large enough.
func (hs *HybridStore) ParallelSum(colName string) int64 {
	indices := hs.rowStore.ScanIndices()
	numWorkers := getNumWorkers(len(indices))
	if numWorkers == 1 {
		return hs.seqSum(colName, indices)
	}

	colIdx := hs.rowStore.ColIndex(colName)
	if colIdx < 0 {
		return 0
	}

	partSize := len(indices) / numWorkers
	partialSums := make(chan int64, numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		start := w * partSize
		end := start + partSize
		if w == numWorkers-1 {
			end = len(indices)
		}
		go func(idxSlice []int) {
			defer wg.Done()
			var s int64
			for _, i := range idxSlice {
				row := hs.rowStore.Get(i)
				v := row.Get(colIdx)
				switch v.Type {
				case TypeInt:
					s += v.Int
				case TypeFloat:
					s += int64(v.Float)
				}
			}
			partialSums <- s
		}(indices[start:end])
	}

	wg.Wait()
	close(partialSums)
	var total int64
	for s := range partialSums {
		total += s
	}
	return total
}

func (hs *HybridStore) seqSum(colName string, indices []int) int64 {
	colIdx := hs.rowStore.ColIndex(colName)
	if colIdx < 0 {
		return 0
	}
	var s int64
	for _, i := range indices {
		row := hs.rowStore.Get(i)
		v := row.Get(colIdx)
		switch v.Type {
		case TypeInt:
			s += v.Int
		case TypeFloat:
			s += int64(v.Float)
		}
	}
	return s
}

// ParallelScan returns all live rows using parallel goroutines for large datasets.
func (hs *HybridStore) ParallelScan() [][]Value {
	indices := hs.rowStore.ScanIndices()
	numWorkers := getNumWorkers(len(indices))
	if numWorkers == 1 {
		return hs.Scan()
	}

	partSize := len(indices) / numWorkers
	resultCh := make(chan [][]Value, numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		start := w * partSize
		end := start + partSize
		if w == numWorkers-1 {
			end = len(indices)
		}
		go func(idxSlice []int) {
			defer wg.Done()
			part := make([][]Value, 0, len(idxSlice))
			for _, i := range idxSlice {
				row := hs.rowStore.Get(i)
				vals := make([]Value, len(hs.columns))
				for ci := range hs.columns {
					vals[ci] = row.Get(ci)
				}
				part = append(part, vals)
			}
			resultCh <- part
		}(indices[start:end])
	}

	wg.Wait()
	close(resultCh)

	merged := make([][]Value, 0, len(indices))
	for part := range resultCh {
		merged = append(merged, part...)
	}
	return merged
}
