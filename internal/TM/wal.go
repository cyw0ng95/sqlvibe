package TM

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

const (
	WALMagic      = 0x377f0682
	WALFrameSize  = 24
	WALHeaderSize = 32
)

var (
	ErrInvalidWAL = errors.New("invalid WAL file")
	ErrNotWALMode = errors.New("not in WAL mode")
)

type WALHeader struct {
	Magic     uint32
	PageSize  int32
	Sequence  int32
	Salt1     int32
	Salt2     int32
	Checksum1 uint32
	Checksum2 uint32
}

type WALFrame struct {
	PageNumber uint32
	CommitSize uint32
	Checksum1  uint32
	Checksum2  uint32
	PageData   []byte
}

type WAL struct {
	file     *os.File
	pageSize int
	sequence int32
	salt1    int32
	salt2    int32
	mu       sync.Mutex
	closed   bool
}

func OpenWAL(path string, pageSize int) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:     file,
		pageSize: pageSize,
		sequence: 0,
		salt1:    0,
		salt2:    0,
		closed:   false,
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if stat.Size() == 0 {
		if err := wal.writeHeader(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return wal, nil
}

func (wal *WAL) writeHeader() error {
	header := WALHeader{
		Magic:    WALMagic,
		PageSize: int32(wal.pageSize),
		Sequence: wal.sequence,
		Salt1:    wal.salt1,
		Salt2:    wal.salt2,
	}

	data := make([]byte, WALHeaderSize)
	binary.BigEndian.PutUint32(data[0:4], header.Magic)
	binary.BigEndian.PutUint32(data[4:8], uint32(header.PageSize))
	binary.BigEndian.PutUint32(data[8:12], uint32(header.Sequence))
	binary.BigEndian.PutUint32(data[12:16], uint32(header.Salt1))
	binary.BigEndian.PutUint32(data[16:20], uint32(header.Salt2))

	cs1, cs2 := wal.checksum(data[:28])
	binary.BigEndian.PutUint32(data[28:32], cs1)
	binary.BigEndian.PutUint32(data[32:36], cs2)
	_ = cs2

	_, err := wal.file.WriteAt(data, 0)
	return err
}

func (wal *WAL) WriteFrame(pageNum uint32, pageData []byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return errors.New("WAL is closed")
	}

	frame := WALFrame{
		PageNumber: pageNum,
		PageData:   pageData,
	}

	data := make([]byte, WALFrameSize+wal.pageSize)
	binary.BigEndian.PutUint32(data[0:4], frame.PageNumber)
	binary.BigEndian.PutUint32(data[4:8], frame.CommitSize)

	offset := WALFrameSize
	copy(data[offset:offset+wal.pageSize], pageData)
	offset += wal.pageSize

	cs1, cs2 := wal.checksum(data[:offset])
	binary.BigEndian.PutUint32(data[20:24], cs1)
	binary.BigEndian.PutUint32(data[24:28], cs2)
	_ = cs2

	_, err := wal.file.WriteAt(data, int64(WALHeaderSize+wal.sequence*int32(WALFrameSize+wal.pageSize)))
	return err
}

func (wal *WAL) ReadFrame(frameNum int) (*WALFrame, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	offset := int64(WALHeaderSize + frameNum*(WALFrameSize+wal.pageSize))
	data := make([]byte, WALFrameSize+wal.pageSize)

	_, err := wal.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	frame := &WALFrame{
		PageNumber: binary.BigEndian.Uint32(data[0:4]),
		PageData:   make([]byte, wal.pageSize),
	}
	copy(frame.PageData, data[WALFrameSize:WALFrameSize+wal.pageSize])

	return frame, nil
}

func (wal *WAL) Commit() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.sequence++
	return wal.writeHeader()
}

func (wal *WAL) Checkpoint() (int, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	seq := wal.sequence
	wal.sequence = 0
	if err := wal.writeHeader(); err != nil {
		return 0, err
	}

	if err := wal.file.Truncate(int64(WALHeaderSize)); err != nil {
		return 0, err
	}

	return int(seq), nil
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return nil
	}

	wal.closed = true
	return wal.file.Close()
}

func (wal *WAL) Size() int64 {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return int64(WALHeaderSize) + int64(wal.sequence)*int64(WALFrameSize+wal.pageSize)
}

func (wal *WAL) Sequence() int32 {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.sequence
}

// FrameCount returns the number of frames currently stored in the WAL.
func (wal *WAL) FrameCount() int {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return int(wal.sequence)
}

// Recover replays all committed frames in the WAL and returns the number of frames
// that were successfully recovered. It is safe to call Recover on a fresh WAL.
func (wal *WAL) Recover() (int, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	stat, err := wal.file.Stat()
	if err != nil {
		return 0, err
	}

	// Calculate how many complete frames are stored beyond the header.
	frameSize := int64(WALFrameSize + wal.pageSize)
	if frameSize == 0 {
		return 0, nil
	}
	available := stat.Size() - int64(WALHeaderSize)
	if available <= 0 {
		return 0, nil
	}
	count := int(available / frameSize)
	if count == 0 {
		return 0, nil
	}

	// Validate each frame's checksum and count valid ones.
	recovered := 0
	data := make([]byte, frameSize)
	for i := 0; i < count; i++ {
		offset := int64(WALHeaderSize) + int64(i)*frameSize
		n, readErr := wal.file.ReadAt(data, offset)
		if readErr != nil || n < int(frameSize) {
			break
		}
		// Verify stored checksum.
		cs1, _ := wal.checksum(data[:WALFrameSize-8+wal.pageSize])
		stored := binary.BigEndian.Uint32(data[20:24])
		if cs1 != stored {
			break // Stop at first corrupt frame.
		}
		recovered++
	}

	// Sync the sequence counter with the recovered frame count.
	if int32(recovered) > wal.sequence {
		wal.sequence = int32(recovered)
	}
	return recovered, nil
}

func (wal *WAL) checksum(data []byte) (uint32, uint32) {
	var s1, s2 uint32
	for i := 0; i < len(data); i++ {
		s1 = (s1 + uint32(data[i])) % 65521
		s2 = (s2 + s1) % 65521
	}
	return s1, s2
}

type WALReader struct {
	file     *os.File
	pageSize int
	frame    int
	closed   bool
}

func NewWALReader(path string, pageSize int) (*WALReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &WALReader{
		file:     file,
		pageSize: pageSize,
		frame:    0,
		closed:   false,
	}, nil
}

func (r *WALReader) Next() (*WALFrame, error) {
	if r.closed {
		return nil, io.EOF
	}

	offset := int64(WALHeaderSize + r.frame*(WALFrameSize+r.pageSize))
	data := make([]byte, WALFrameSize+r.pageSize)

	n, err := r.file.ReadAt(data, offset)
	if err != nil {
		if err == io.EOF || n == 0 {
			r.closed = true
			return nil, io.EOF
		}
		return nil, err
	}

	frame := &WALFrame{
		PageNumber: binary.BigEndian.Uint32(data[0:4]),
		PageData:   make([]byte, r.pageSize),
	}
	copy(frame.PageData, data[WALFrameSize:WALFrameSize+r.pageSize])

	r.frame++
	return frame, nil
}

func (r *WALReader) Close() error {
	return r.file.Close()
}

func (r *WALReader) Reset() {
	r.frame = 0
	r.closed = false
}
