package DS

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// WalOp is the type of a WAL operation.
type WalOp uint8

const (
	// WalInsert records a new row being appended.
	WalInsert WalOp = 1
	// WalDelete records a row being deleted by its store index.
	WalDelete WalOp = 2
	// WalUpdate records an existing row being replaced.
	WalUpdate WalOp = 3
)

// walEntry is one record in the WAL.
// Fields are exported for JSON marshaling.
type walEntry struct {
	Op     WalOp   `json:"op"`
	Index  int     `json:"idx,omitempty"` // used by WalDelete and WalUpdate
	Values []Value `json:"vals,omitempty"` // used by WalInsert and WalUpdate
}

// WriteAheadLog is an append-only log for SQLVIBE columnar database mutations.
//
// File format (append-only, no seek required):
//   [entry0_len uint32][entry0 JSON][entry1_len uint32][entry1 JSON]...
//
// After a successful Checkpoint the WAL file is truncated to zero and the main
// database file is rewritten with the current store state.
type WriteAheadLog struct {
	mu   sync.Mutex
	path string
	file *os.File
	buf  *bufio.Writer
}

// OpenWAL opens (or creates) a WAL file at path.
// The WAL is append-only; existing entries are not read back here — call
// Replay to load them into a HybridStore.
func OpenWAL(path string) (*WriteAheadLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	return &WriteAheadLog{
		path: path,
		file: f,
		buf:  bufio.NewWriterSize(f, 64*1024),
	}, nil
}

// Close flushes any buffered data and closes the WAL file.
func (w *WriteAheadLog) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// appendEntry serializes entry as a length-prefixed JSON record and appends it
// to the WAL.  The caller must hold w.mu.
func (w *WriteAheadLog) appendEntry(e walEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal WAL entry: %w", err)
	}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := w.buf.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(data); err != nil {
		return err
	}
	return w.buf.Flush()
}

// AppendInsert appends a WalInsert entry for vals.
func (w *WriteAheadLog) AppendInsert(vals []Value) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendEntry(walEntry{Op: WalInsert, Values: vals})
}

// AppendDelete appends a WalDelete entry for the row at idx.
func (w *WriteAheadLog) AppendDelete(idx int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendEntry(walEntry{Op: WalDelete, Index: idx})
}

// AppendUpdate appends a WalUpdate entry that replaces row idx with vals.
func (w *WriteAheadLog) AppendUpdate(idx int, vals []Value) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendEntry(walEntry{Op: WalUpdate, Index: idx, Values: vals})
}

// Replay reads all entries from the WAL file from the beginning and applies
// them to hs.  It is intended to be called on startup to recover from an
// unclean shutdown, before any new entries are appended.
func (w *WriteAheadLog) Replay(hs *HybridStore) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to the start of the file.
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("WAL seek: %w", err)
	}
	r := bufio.NewReader(w.file)

	for {
		// Read 4-byte length prefix.
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("read WAL entry length: %w", err)
		}
		l := int(binary.LittleEndian.Uint32(lenBuf[:]))
		if l == 0 {
			continue
		}

		// Read entry body.
		body := make([]byte, l)
		if _, err := io.ReadFull(r, body); err != nil {
			if err == io.ErrUnexpectedEOF {
				break // Truncated entry at end — safe to ignore.
			}
			return fmt.Errorf("read WAL entry body: %w", err)
		}
		var e walEntry
		if err := json.Unmarshal(body, &e); err != nil {
			// Skip corrupted entries rather than aborting recovery.
			continue
		}

		// Apply the entry to the store.
		switch e.Op {
		case WalInsert:
			hs.Insert(e.Values)
		case WalDelete:
			hs.Delete(e.Index)
		case WalUpdate:
			hs.Update(e.Index, e.Values)
		}
	}

	// Seek back to end so future appends are positioned correctly.
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("WAL seek to end: %w", err)
	}
	return nil
}

// Checkpoint persists the current state of hs to dbPath (SQLVIBE binary
// format) and then truncates the WAL file to zero bytes.  After a successful
// checkpoint, the WAL is empty and the database file is the authoritative
// source of truth.
//
// schema should contain the same column_names / column_types metadata that was
// used when the database file was originally created.
func (w *WriteAheadLog) Checkpoint(hs *HybridStore, dbPath string, schema map[string]interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write the current state of the store atomically (write to tmp, rename).
	tmp := dbPath + ".tmp"
	if err := WriteDatabase(tmp, hs, schema); err != nil {
		return fmt.Errorf("WAL checkpoint write: %w", err)
	}
	if err := os.Rename(tmp, dbPath); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("WAL checkpoint rename: %w", err)
	}

	// Truncate the WAL file to zero.
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("WAL flush before truncate: %w", err)
	}
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("WAL truncate: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("WAL seek after truncate: %w", err)
	}
	w.buf.Reset(w.file)
	return nil
}

// Size returns the current WAL file size in bytes.
func (w *WriteAheadLog) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.buf.Flush(); err != nil {
		return 0, err
	}
	fi, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
