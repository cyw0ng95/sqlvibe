package CG

import VM "github.com/sqlvibe/sqlvibe/internal/VM"

type Optimizer struct{}

func NewOptimizer() *Optimizer {
	return &Optimizer{}
}

func (o *Optimizer) Optimize(program *VM.Program) *VM.Program {
	return program
}
