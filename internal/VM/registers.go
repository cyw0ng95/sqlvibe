package VM

type RegisterAllocator struct {
	maxReg    int
	nextReg   int
	allocated uint64
	largeRegs map[int]bool
}

func NewRegisterAllocator(initialRegs int) *RegisterAllocator {
	if initialRegs <= 0 {
		initialRegs = 16
	}
	return &RegisterAllocator{
		maxReg:    initialRegs,
		nextReg:   0,
		allocated: 0,
		largeRegs: make(map[int]bool),
	}
}

func (ra *RegisterAllocator) Alloc() int {
	if ra.nextReg < 64 {
		for i := ra.nextReg; i < 64; i++ {
			if (ra.allocated & (1 << i)) == 0 {
				ra.allocated |= (1 << i)
				ra.nextReg = i
				return i
			}
		}
	}

	for i := 64; ; i++ {
		if !ra.largeRegs[i] {
			ra.largeRegs[i] = true
			if i >= ra.maxReg {
				ra.maxReg = i + 1
			}
			return i
		}
	}
}

func (ra *RegisterAllocator) AllocMany(count int) []int {
	result := make([]int, count)
	for i := 0; i < count; i++ {
		result[i] = ra.Alloc()
	}
	return result
}

func (ra *RegisterAllocator) Release(reg int) {
	if reg < 0 {
		return
	}
	if reg < 64 {
		ra.allocated &= ^(1 << reg)
		if reg < ra.nextReg {
			ra.nextReg = reg
		}
	} else {
		delete(ra.largeRegs, reg)
	}
}

func (ra *RegisterAllocator) ReleaseMany(regs []int) {
	for _, r := range regs {
		ra.Release(r)
	}
}

func (ra *RegisterAllocator) Reserve(reg int) {
	if reg < 0 {
		return
	}
	if reg < 64 {
		ra.allocated |= (1 << reg)
	} else {
		ra.largeRegs[reg] = true
	}
	if reg >= ra.maxReg {
		ra.maxReg = reg + 1
	}
}

func (ra *RegisterAllocator) MaxReg() int {
	return ra.maxReg
}

func (ra *RegisterAllocator) Reset() {
	ra.allocated = 0
	ra.nextReg = 0
	ra.largeRegs = make(map[int]bool)
}
