package VM

type RegisterAllocator struct {
	regs    []int
	maxReg  int
	nextReg int
	inUse   map[int]bool
}

func NewRegisterAllocator(initialRegs int) *RegisterAllocator {
	return &RegisterAllocator{
		regs:    make([]int, 0),
		maxReg:  initialRegs,
		nextReg: 0,
		inUse:   make(map[int]bool),
	}
}

func (ra *RegisterAllocator) Alloc() int {
	for i := 0; i < ra.maxReg; i++ {
		if !ra.inUse[i] {
			ra.inUse[i] = true
			return i
		}
	}
	ra.inUse[ra.maxReg] = true
	ra.maxReg++
	return ra.maxReg - 1
}

func (ra *RegisterAllocator) AllocMany(count int) []int {
	result := make([]int, count)
	for i := 0; i < count; i++ {
		result[i] = ra.Alloc()
	}
	return result
}

func (ra *RegisterAllocator) Release(reg int) {
	if reg >= 0 && reg <= ra.maxReg {
		ra.inUse[reg] = false
	}
}

func (ra *RegisterAllocator) ReleaseMany(regs []int) {
	for _, r := range regs {
		ra.Release(r)
	}
}

func (ra *RegisterAllocator) Reserve(reg int) {
	if reg >= 0 {
		ra.inUse[reg] = true
		if reg >= ra.maxReg {
			ra.maxReg = reg + 1
		}
	}
}

func (ra *RegisterAllocator) MaxReg() int {
	return ra.maxReg
}

func (ra *RegisterAllocator) Reset() {
	for k := range ra.inUse {
		ra.inUse[k] = false
	}
	ra.nextReg = 0
}
