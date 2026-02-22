package storage

import "sync"

// WorkerPool manages a fixed pool of goroutines for concurrent task execution.
type WorkerPool struct {
	workers int
	tasks   chan func()
	wg      sync.WaitGroup
}

// NewWorkerPool creates and starts a WorkerPool with the given number of workers.
func NewWorkerPool(workers int) *WorkerPool {
	if workers < 1 {
		workers = 1
	}
	wp := &WorkerPool{
		workers: workers,
		tasks:   make(chan func(), workers*10),
	}
	for i := 0; i < workers; i++ {
		go func() {
			for task := range wp.tasks {
				task()
				wp.wg.Done()
			}
		}()
	}
	return wp
}

// Submit enqueues a task for execution. The caller should call Wait() to wait
// for all submitted tasks to complete.
func (wp *WorkerPool) Submit(task func()) {
	wp.wg.Add(1)
	wp.tasks <- task
}

// Wait blocks until all submitted tasks have completed.
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

// Close shuts down the worker pool. Do not submit tasks after calling Close.
func (wp *WorkerPool) Close() {
	close(wp.tasks)
}
