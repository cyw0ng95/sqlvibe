package QP

import (
	"sync"
	"sync/atomic"
)

// Operator is an abstract query operator node in a DAG.
type Operator interface {
	// Name returns a short human-readable label.
	Name() string
}

// DAGNode is a node in the query execution DAG.
type DAGNode struct {
	ID        int
	Op        Operator
	Inputs    []*DAGNode
	Outputs   []*DAGNode
	mu        sync.Mutex
	done      bool
	scheduled uint32 // atomic: 0 = not yet scheduled, 1 = scheduled
	result    interface{}
	waitCh    chan struct{}
}

// DAG is a directed acyclic graph of query operators.
type DAG struct {
	nodes  []*DAGNode
	nextID int
}

// NewDAG creates an empty DAG.
func NewDAG() *DAG { return &DAG{} }

// AddNode appends an operator node and returns it.
func (d *DAG) AddNode(op Operator) *DAGNode {
	n := &DAGNode{
		ID:     d.nextID,
		Op:     op,
		waitCh: make(chan struct{}),
	}
	d.nextID++
	d.nodes = append(d.nodes, n)
	return n
}

// AddEdge records that src must complete before dst.
func (d *DAG) AddEdge(src, dst *DAGNode) {
	src.Outputs = append(src.Outputs, dst)
	dst.Inputs = append(dst.Inputs, src)
}

// Nodes returns all nodes in the DAG.
func (d *DAG) Nodes() []*DAGNode { return d.nodes }

// isReady returns true when all input nodes have completed.
func (n *DAGNode) isReady() bool {
	for _, in := range n.Inputs {
		in.mu.Lock()
		ready := in.done
		in.mu.Unlock()
		if !ready {
			return false
		}
	}
	return true
}

// markDone marks this node as done and closes the wait channel.
func (n *DAGNode) markDone(result interface{}) {
	n.mu.Lock()
	n.result = result
	n.done = true
	n.mu.Unlock()
	close(n.waitCh)
}

// Result returns the node's result. Blocks until markDone is called.
func (n *DAGNode) Result() interface{} {
	<-n.waitCh
	return n.result
}

// -- Concrete operator types --

// ScanOp is a table scan operator.
type ScanOp struct{ Table string }

func (s ScanOp) Name() string { return "Scan(" + s.Table + ")" }

// FilterOp is a filter/predicate operator.
type FilterOp struct{ Predicate Expr }

func (f FilterOp) Name() string { return "Filter" }

// HashJoinOp is a hash-join operator.
type HashJoinOp struct{ Key string }

func (h HashJoinOp) Name() string { return "HashJoin(" + h.Key + ")" }

// ProjectOp is a projection operator.
type ProjectOp struct{ Columns []string }

func (p ProjectOp) Name() string { return "Project" }

// AggOp is an aggregation operator.
type AggOp struct{ Funcs []string }

func (a AggOp) Name() string { return "Agg" }

// DAGExecutor executes a DAG using a goroutine per ready node.
type DAGExecutor struct {
	dag     *DAG
	cores   int
	handler func(node *DAGNode) interface{}
}

// NewDAGExecutor creates an executor. The handler is called for each node with
// the node itself; it should return the node's result value.
func NewDAGExecutor(dag *DAG, cores int, handler func(*DAGNode) interface{}) *DAGExecutor {
	if cores < 1 {
		cores = 1
	}
	return &DAGExecutor{dag: dag, cores: cores, handler: handler}
}

// Execute runs all nodes in the DAG respecting data-flow dependencies.
// Nodes whose inputs are all done are scheduled immediately; others wait.
func (e *DAGExecutor) Execute() {
	sem := make(chan struct{}, e.cores)
	var wg sync.WaitGroup

	var schedule func(n *DAGNode)
	schedule = func(n *DAGNode) {
		// Wait for all inputs
		for _, in := range n.Inputs {
			<-in.waitCh
		}
		sem <- struct{}{}
		result := e.handler(n)
		n.markDone(result)
		<-sem
		// Schedule outputs that are now ready; use CAS to ensure each node is
		// scheduled at most once even when multiple inputs complete concurrently.
		for _, out := range n.Outputs {
			if out.isReady() && atomic.CompareAndSwapUint32(&out.scheduled, 0, 1) {
				wg.Add(1)
				go func(o *DAGNode) {
					defer wg.Done()
					schedule(o)
				}(out)
			}
		}
	}

	// Start nodes with no inputs
	for _, n := range e.dag.nodes {
		if len(n.Inputs) == 0 {
			atomic.StoreUint32(&n.scheduled, 1)
			wg.Add(1)
			go func(node *DAGNode) {
				defer wg.Done()
				schedule(node)
			}(n)
		}
	}

	wg.Wait()
}
