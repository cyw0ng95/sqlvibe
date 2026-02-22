package DS

// Prefetcher wraps the shared prefetchWorkerPool to expose a general-purpose
// async page-prefetch interface.  Callers submit page numbers; the Prefetcher
// fires background reads into the OS page cache so subsequent on-demand reads
// find the data already warm.
//
// The underlying worker pool is shared with BTree.prefetchChildren and is
// initialised lazily on first use (see btree.go: prefetchWorkerPool).
type Prefetcher struct {
	degree int
	pm     *PageManager
}

// NewPrefetcher returns a Prefetcher that uses pm to read pages.
// degree controls the maximum number of in-flight prefetch tasks.  A degree of
// 0 means "use the shared pool capacity" (64 slots).
func NewPrefetcher(pm *PageManager, degree int) *Prefetcher {
	if degree <= 0 {
		degree = 64
	}
	return &Prefetcher{
		degree: degree,
		pm:     pm,
	}
}

// Prefetch asynchronously reads pageNum into the page cache.
// If the shared worker pool is already full, the prefetch is silently skipped
// rather than blocking the caller.
func (p *Prefetcher) Prefetch(pageNum uint32) {
	if pageNum == 0 {
		return
	}
	// Ensure the worker pool is running (shared with prefetchChildren).
	prefetchWorkerPool.once.Do(func() {
		prefetchWorkerPool.tasks = make(chan func(), 64)
		for i := 0; i < 4; i++ {
			go func() {
				for task := range prefetchWorkerPool.tasks {
					task()
				}
			}()
		}
	})

	pm := p.pm
	pn := pageNum
	select {
	case prefetchWorkerPool.tasks <- func() { pm.ReadPage(pn) }: //nolint:errcheck
	default:
		// Pool busy; skip.
	}
}
