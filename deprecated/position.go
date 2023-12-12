package filechannel

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/arkbriar/filechannel/internal/condvar"
)

func init() {
	// Runtime check. Position should fit in a sector.
	if unsafe.Sizeof(position{}) > 512 {
		panic("position is too large")
	}
}

type pos struct {
	fileIndex uint64
	offset    uint64
}

func (p *pos) LessThan(q pos) bool {
	return p.fileIndex < q.fileIndex || (p.fileIndex == q.fileIndex && p.offset < q.offset)
}

type position struct {
	readFileIndex  uint64
	readOffset     uint64
	writeFileIndex uint64
	writeOffset    uint64
}

// Encode to bytes. Buffer must be at least 512 long.
func (pos *position) Encode(b []byte) {
	b = binary.BigEndian.AppendUint64(b, uint64(time.Now().UnixMilli()))
	b = binary.BigEndian.AppendUint64(b, pos.readFileIndex)
	b = binary.BigEndian.AppendUint64(b, pos.readOffset)
	b = binary.BigEndian.AppendUint64(b, pos.writeFileIndex)
	b = binary.BigEndian.AppendUint64(b, pos.writeOffset)
}

// Decode from bytes. Buffer must be at least 512 long.
func (pos *position) Decode(b []byte) {
	*pos = position{
		readFileIndex:  binary.BigEndian.Uint64(b[8:16]),
		readOffset:     binary.BigEndian.Uint64(b[16:24]),
		writeFileIndex: binary.BigEndian.Uint64(b[24:32]),
		writeOffset:    binary.BigEndian.Uint64(b[32:40]),
	}
}

var (
	ErrPositionFileCorrupted = errors.New("position file corrupted")
	ErrPositionSyncerClosed  = errors.New("position syncer closed")
)

type positionSyncer struct {
	f *os.File

	mu           sync.RWMutex
	gcPathCond   *condvar.Cond
	readPathCond *condvar.Cond
	pos          position
	closed       bool
}

func openPositionSyncer(name string) (*positionSyncer, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	// Load the position from the current file.
	var sector [512]byte
	buf := sector[:]
	n, err := f.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		_ = f.Close()
		return nil, err
	}
	if n < 512 {
		_ = f.Close()
		return nil, ErrPositionFileCorrupted
	}

	s := &positionSyncer{
		f: f,
	}

	if n == len(sector) {
		s.pos.Decode(buf)
	} else {
		err := s.commit(buf)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	// Set up the cond vars.
	s.gcPathCond = condvar.NewCond(s.mu.RLocker())
	s.readPathCond = condvar.NewCond(s.mu.RLocker())

	return s, nil
}

func (s *positionSyncer) commit(buf []byte) error {
	if buf == nil {
		var sector [512]byte
		buf = sector[:]
	}
	s.pos.Encode(buf)
	_, err := s.f.WriteAt(buf, 0)
	if err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *positionSyncer) Close() error {
	err := s.f.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Notify all waiters.
	s.closed = true
	s.gcPathCond.Broadcast()
	s.readPathCond.Broadcast()

	return err
}

// SyncWrite syncs the write position to file.
func (s *positionSyncer) SyncWrite(fileIndex, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if fileIndex > s.pos.writeFileIndex ||
		(fileIndex == s.pos.writeFileIndex && offset > s.pos.writeOffset) {

		s.pos.writeFileIndex = fileIndex
		s.pos.writeOffset = offset

		err := s.commit(nil)
		if err != nil {
			return err
		}

		s.readPathCond.Broadcast()
	}

	return nil
}

// SyncRead syncs the read position to file.
func (s *positionSyncer) SyncRead(fileIndex, offset uint64, commit bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if fileIndex > s.pos.readFileIndex ||
		(fileIndex == s.pos.readFileIndex && offset > s.pos.readOffset) {

		s.pos.readFileIndex = fileIndex
		s.pos.readOffset = offset

		if commit {
			err := s.commit(nil)
			if err != nil {
				return err
			}
		}

		s.gcPathCond.Broadcast()
	}

	return nil
}

// Read reads the current position.
func (s *positionSyncer) Read() position {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pos
}

// WaitForGCBefore waits until the commited read file index is larger than given
// file index or the [context.Context] is cancelled or timeouted. It returns nil
// for the first situation and the result from [context.Context.Err] in the latter.
// If the syncer is closed during the waiting process, it will return
// [ErrPositionSyncerClosed].
func (s *positionSyncer) WaitForGCBefore(ctx context.Context, fileIndex uint64) (uint64, error) {
	cond := s.gcPathCond
	l := cond.L

	l.Lock()
	for {
		if s.closed {
			return 0, ErrPositionSyncerClosed
		}

		if s.pos.readFileIndex > fileIndex {
			l.Unlock()
			return s.pos.readFileIndex, nil
		}

		err := cond.WaitContext(ctx)
		if err != nil {
			return 0, err
		}
	}
}

// WaitForWriteAfter waits until the commited write position is larger than give
// position or the [context.Context] is cancelled or timeouted. It will returns
// the current write position when no error encountered.
// It returns nil for the first situation and the result from [context.Context.Err]
// in the latter. If the syncer is closed during the waiting process, it will return
// [ErrPositionSyncerClosed].
func (s *positionSyncer) WaitForWriteAfter(ctx context.Context,
	fileIndex, offset uint64) (uint64, uint64, error) {
	cond := s.readPathCond
	l := cond.L

	l.Lock()
	for {
		if s.closed {
			return 0, 0, ErrPositionSyncerClosed
		}

		if s.pos.writeFileIndex > fileIndex ||
			(s.pos.writeFileIndex == fileIndex && s.pos.writeOffset > offset) {
			l.Unlock()
			return s.pos.writeFileIndex, s.pos.writeOffset, nil
		}

		err := cond.WaitContext(ctx)
		if err != nil {
			return 0, 0, err
		}
	}
}
