package util

import "sync"

// ByteBufferPool is a pool of reusable byte slices.
var ByteBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 4096)
		return &buf
	},
}

// GetByteBuffer retrieves a byte buffer from the pool.
func GetByteBuffer() *[]byte {
	return ByteBufferPool.Get().(*[]byte)
}

// PutByteBuffer returns a byte buffer to the pool.
func PutByteBuffer(buf *[]byte) {
	if buf != nil {
		ByteBufferPool.Put(buf)
	}
}

// InterfaceSlicePool is a pool of reusable []interface{} slices.
var InterfaceSlicePool = sync.Pool{
	New: func() interface{} {
		s := make([]interface{}, 0, 32)
		return &s
	},
}

// GetInterfaceSlice retrieves a []interface{} slice from the pool.
func GetInterfaceSlice() *[]interface{} {
	sp := InterfaceSlicePool.Get().(*[]interface{})
	*sp = (*sp)[:0] // reset length, keep capacity
	return sp
}

// PutInterfaceSlice returns a []interface{} slice to the pool.
func PutInterfaceSlice(s *[]interface{}) {
	if s != nil {
		InterfaceSlicePool.Put(s)
	}
}
