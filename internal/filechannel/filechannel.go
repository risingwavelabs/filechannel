// Copyright 2023-2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filechannel

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/ironpark/skiplist"
	"github.com/klauspost/compress/snappy"

	"github.com/risingwavelabs/filechannel/internal/condvar"
	"github.com/risingwavelabs/filechannel/internal/fs"
	"github.com/risingwavelabs/filechannel/internal/utils"
)

var (
	ErrChecksumMismatch   = errors.New("channel corrupted: checksum mismatch")
	ErrChannelClosed      = errors.New("channel closed")
	ErrNotEnoughMessages  = errors.New("not enough messages")
	ErrNotEnoughReadToAck = errors.New("not enough read to ack")
)

type SegmentFileState int

const (
	Plain SegmentFileState = iota
	Compressing
	Compressed
)

const MessageHeaderBinarySize = 8

type MessageHeader struct {
	Length   uint32
	Checksum uint32
}

func (h *MessageHeader) Encode(b []byte) {
	if len(b) < MessageHeaderBinarySize {
		panic("buffer size not large enough")
	}
	binary.BigEndian.PutUint32(b[:4], h.Length)
	binary.BigEndian.PutUint32(b[4:8], h.Checksum)
}

func (h *MessageHeader) Decode(b []byte) {
	if len(b) < MessageHeaderBinarySize {
		panic("buffer size not large enough")
	}
	h.Length = binary.BigEndian.Uint32(b[:4])
	h.Checksum = binary.BigEndian.Uint32(b[4:8])
}

const SegmentHeaderBinarySize = 64

type PlainSegmentHeader struct {
	SegmentID   uint32
	BeginOffset uint64
}

func (h *PlainSegmentHeader) Decode(b []byte) {
	if len(b) < SegmentHeaderBinarySize {
		panic("buffer size not large enough")
	}
	h.SegmentID = binary.BigEndian.Uint32(b[:4])
	h.BeginOffset = binary.BigEndian.Uint64(b[4:12])
}

func (h *PlainSegmentHeader) Encode(b []byte) {
	if len(b) < SegmentHeaderBinarySize {
		panic("buffer size not large enough")
	}
	binary.BigEndian.PutUint32(b[:4], h.SegmentID)
	binary.BigEndian.PutUint64(b[4:12], h.BeginOffset)
}

type CompressionMethod byte

const (
	Snappy CompressionMethod = iota
	Gzip
)

type CompressedSegmentHeader struct {
	SegmentID         uint32
	BeginOffset       uint64
	EndOffset         uint64
	CompressionMethod CompressionMethod
}

func (h *CompressedSegmentHeader) Decode(b []byte) {
	if len(b) < SegmentHeaderBinarySize {
		panic("buffer size not large enough")
	}

	h.SegmentID = binary.BigEndian.Uint32(b[:4])
	h.BeginOffset = binary.BigEndian.Uint64(b[4:12])
	h.EndOffset = binary.BigEndian.Uint64(b[12:20])
	h.CompressionMethod = CompressionMethod(b[20])
}

func (h *CompressedSegmentHeader) Encode(b []byte) {
	if len(b) < SegmentHeaderBinarySize {
		panic("buffer size not large enough")
	}
	binary.BigEndian.PutUint32(b[:4], h.SegmentID)
	binary.BigEndian.PutUint64(b[4:12], h.BeginOffset)
	binary.BigEndian.PutUint64(b[12:20], h.EndOffset)
	b[20] = byte(h.CompressionMethod)
}

const (
	IteratorBufferLimit = 1 << 20
)

type Iterator struct {
	segmentManager *SegmentManager
	position       *Position

	writeOffset  uint64 // local copy of write offset
	offset       uint64
	segmentIndex uint32
	f            *fs.SequentialFile
	r            io.Reader
	headerBuf    [MessageHeaderBinarySize]byte

	autoAck         bool
	readerIndex     uint32
	pendingAckCount int
	pendingAck      skiplist.SkipList[uint32, int]

	lastErr error
	buf     *bytes.Buffer
}

func (it *Iterator) Offset() uint64 {
	return it.offset
}

func (it *Iterator) updateOffset(offset uint64) error {
	if it.offset == math.MaxUint64 {
		it.offset = offset
	} else if it.offset != offset {
		return errors.New("broken of continuity")
	}
	return nil
}

func (it *Iterator) openCompressedFile() error {
	file := it.segmentManager.SegmentFile(it.segmentIndex, Compressed)
	f, err := fs.OpenSequentialFile(file)
	if err != nil {
		return err
	}
	var buf [SegmentHeaderBinarySize]byte

	if _, err = io.ReadFull(f, buf[:]); err != nil {
		err1 := f.Close()
		return errors.Join(err, err1)
	}

	header := &CompressedSegmentHeader{}
	header.Decode(buf[:])
	err = it.updateOffset(header.BeginOffset)
	if err != nil {
		err1 := f.Close()
		return errors.Join(err, err1)
	}

	var r io.Reader
	switch header.CompressionMethod {
	case Snappy:
		r = snappy.NewReader(f)
	case Gzip:
		r, err = gzip.NewReader(f)
		if err != nil {
			err1 := f.Close()
			return errors.Join(err, err1)
		}
	default:
		err1 := f.Close()
		return errors.Join(errors.New("unsupported compression method"), err1)
	}
	it.f, it.r = f, r
	return nil
}

func (it *Iterator) openPlainFile() error {
	file := it.segmentManager.SegmentFile(it.segmentIndex, Plain)
	f, err := fs.OpenSequentialFile(file)
	if err != nil {
		return err
	}
	var buf [SegmentHeaderBinarySize]byte
	if _, err = io.ReadFull(f, buf[:]); err != nil {
		err1 := f.Close()
		return errors.Join(err, err1)
	}

	header := &PlainSegmentHeader{}
	header.Decode(buf[:])
	err = it.updateOffset(header.BeginOffset)
	if err != nil {
		err1 := f.Close()
		return errors.Join(err, err1)
	}

	it.f, it.r = f, f
	return nil
}

func (it *Iterator) openFile() error {
	err := it.openCompressedFile()
	if os.IsNotExist(err) {
		return it.openPlainFile()
	}
	return err
}

func (it *Iterator) closeFile() error {
	if it.f != nil {
		err := it.f.Close()
		it.f, it.r = nil, nil
		return err
	}
	return nil
}

func (it *Iterator) ensure() error {
	if it.f == nil {
		return it.openFile()
	}
	return nil
}

// ReadNext reads the next message from the reader. If any error occurs, it
// returns immediately. If the message isn't fully read, it returns io.ErrUnexpectedEOF.
// If the checksum doesn't match, it returns ErrChecksumMismatch.
// It will return io.EOF if and only if no bytes were read.
func ReadNext(r io.Reader, w io.Writer, hBuf []byte) error {
	_, err := io.ReadFull(r, hBuf[:])
	if err != nil {
		return err
	}
	h := &MessageHeader{}
	h.Decode(hBuf[:])

	// Grow the buffer to avoid multiple allocations.
	if buf, ok := w.(*bytes.Buffer); ok {
		buf.Grow(int(h.Length))
		start := buf.Len()
		n, err := io.CopyN(buf, r, int64(h.Length))
		if err != nil {
			if errors.Is(err, io.EOF) && n < int64(h.Length) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if crc32.ChecksumIEEE(buf.Bytes()[start:]) != h.Checksum {
			return ErrChecksumMismatch
		}
	} else {
		cw := utils.NewChecksumWriter(w)
		n, err := io.CopyN(cw, r, int64(h.Length))
		if err != nil {
			if errors.Is(err, io.EOF) && n < int64(h.Length) {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		if cw.Checksum() != h.Checksum {
			return ErrChecksumMismatch
		}
	}

	return nil
}

func (it *Iterator) readNext() ([]byte, error) {
	err := ReadNext(it.r, it.buf, it.headerBuf[:])
	if err != nil {
		return nil, err
	}

	it.pendingAckCount++
	it.pendingAck.Set(it.segmentIndex, getOrDefault(it.pendingAck, it.segmentIndex, 0)+1)
	it.offset += uint64(MessageHeaderBinarySize) + uint64(it.buf.Len())
	msg := it.buf.Bytes()

	// If buffer size exceeds the limit, allocate a new one. The current one
	// will be returned to the caller.
	if it.buf.Cap() > IteratorBufferLimit {
		it.buf = bytes.NewBuffer(make([]byte, 0, IteratorBufferLimit))
	} else {
		it.buf.Reset()
	}

	return msg, nil
}

func (it *Iterator) Ack(n int) error {
	if it.pendingAckCount < n {
		return ErrNotEnoughReadToAck
	}
	it.pendingAckCount -= n

	f := it.pendingAck.Front()

	for n > 0 {
		if n >= f.Value {
			n -= f.Value
			it.pendingAck.Remove(f.Key())
			f = it.pendingAck.Front()
		} else {
			f.Value -= n
			n = 0
		}
	}

	curSegmentIndex := it.segmentIndex
	if f != nil {
		curSegmentIndex = f.Key()
	}

	if curSegmentIndex > it.readerIndex {
		it.readerIndex, _ = it.segmentManager.AdvanceReader(it.readerIndex, curSegmentIndex-it.readerIndex)
	}

	return nil
}

func (it *Iterator) waitForData(ctx context.Context) error {
	if it.offset >= it.writeOffset {
		writeOffset, err := it.position.Wait(ctx, func(writeOffset uint64) bool {
			return it.offset < writeOffset
		})
		if err != nil {
			return err
		}
		it.writeOffset = writeOffset
	}
	return nil
}

func (it *Iterator) TryNext() ([]byte, error) {
	// nolint: staticcheck
	return it.tryNext(nil)
}

func (it *Iterator) Next(ctx context.Context) (b []byte, err error) {
	return it.tryNext(ctx)
}

func (it *Iterator) tryNext(ctx context.Context) (b []byte, err error) {
	defer func() {
		//goland:noinspection GoDirectComparisonOfErrors
		if err != nil && err != ErrNotEnoughMessages {
			if ctx != nil && err == ctx.Err() {
				return
			}
			it.lastErr = err
		}
	}()
	if it.lastErr != nil {
		return nil, it.lastErr
	}

	// Initialize the offset.
	if it.offset == math.MaxUint64 {
		if err = it.ensure(); err != nil {
			return nil, err
		}
	}

	if ctx == nil {
		if it.offset >= it.writeOffset {
			it.writeOffset = it.position.Get()
			if it.offset >= it.writeOffset {
				return nil, ErrNotEnoughMessages
			}
		}
	} else {
		if err := it.waitForData(ctx); err != nil {
			return nil, err
		}
	}

	loopCount := 0
	for {
		if err = it.ensure(); err != nil {
			return nil, err
		}

		// If loop count > 1, that means there were some empty segment files skipped.
		// That is technically possible but should not happen in real.
		if loopCount > 1 {
			panic("unexpected loop count")
		}

		b, err = it.readNext()

		// Handle end of file: move to next segment.
		if err == io.EOF {
			if err := it.closeFile(); err != nil {
				return nil, err
			}
			it.segmentIndex++
			loopCount++
			continue
		}

		if err == nil && it.autoAck {
			_ = it.Ack(1)
		}

		return b, err
	}
}

func (it *Iterator) Close() error {
	it.segmentManager.CloseReader(it.readerIndex)
	it.lastErr = errors.New("closed")
	return it.closeFile()
}

func NewIterator(manager *SegmentManager, position *Position, autoAck bool) *Iterator {
	reader := manager.NewReader()
	return &Iterator{
		segmentManager: manager,
		position:       position,
		offset:         math.MaxUint64,
		segmentIndex:   reader,
		readerIndex:    reader,
		buf:            bytes.NewBuffer(make([]byte, 0, 4096)),
		autoAck:        autoAck,
		pendingAck:     skiplist.New[uint32, int](cmp.Compare[uint32]),
	}
}

type Position struct {
	mu     sync.RWMutex
	cond   *condvar.Cond
	offset uint64
	closed bool
}

func (p *Position) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	p.cond.Broadcast()
}

func (p *Position) Update(offset uint64) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset > p.offset {
		p.offset = offset
		defer p.cond.Broadcast()
	}
	return p.offset
}

func (p *Position) Get() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.offset
}

func (p *Position) Wait(ctx context.Context, cond func(uint64) bool) (uint64, error) {
	p.mu.RLock()
	for !p.closed && !cond(p.offset) {
		err := p.cond.WaitContext(ctx)
		if err != nil {
			return 0, err
		}
	}
	defer p.mu.RUnlock()

	if cond(p.offset) {
		return p.offset, nil
	} else { // p.closed == true
		return 0, ErrChannelClosed
	}
}

func NewPosition(offset uint64) *Position {
	pos := &Position{offset: offset}
	pos.cond = condvar.NewCond(pos.mu.RLocker())
	return pos
}

type SegmentManager struct {
	dir string

	beginIndex uint32

	writeMu sync.RWMutex
	index   uint32

	readMu       sync.RWMutex
	readCond     *condvar.Cond
	pinFreq      skiplist.SkipList[uint32, int]
	readFreq     skiplist.SkipList[uint32, int]
	maxReadIndex int64
	watermark    uint32
}

func (sm *SegmentManager) GetBeginIndex() uint32 {
	return sm.beginIndex
}

func (sm *SegmentManager) SetBeginIndex(index uint32) {
	sm.beginIndex = index
}

func (sm *SegmentManager) NewReader() uint32 {
	sm.readMu.Lock()
	defer sm.readMu.Unlock()

	beginIndex := sm.watermark
	sm.readFreq.Set(beginIndex, getOrDefault(sm.readFreq, beginIndex, 0)+1)
	return beginIndex
}

func (sm *SegmentManager) Pin(index uint32) bool {
	sm.readMu.Lock()
	defer sm.readMu.Unlock()

	if index >= sm.watermark {
		sm.pinFreq.Set(index, getOrDefault(sm.pinFreq, index, 0)+1)
		return true
	}

	return false
}

func (sm *SegmentManager) Unpin(index uint32) {
	sm.readMu.Lock()
	defer sm.readMu.Unlock()

	v, ok := sm.pinFreq.GetValue(index)
	if !ok {
		panic("unpin non-existing segment")
	}
	isFront := sm.pinFreq.Front().Key() == index

	if v == 1 {
		sm.pinFreq.Remove(index)
	} else {
		sm.pinFreq.Set(index, v-1)
	}

	if isFront && v == 1 {
		sm.updateWatermark()
	}
}

func (sm *SegmentManager) updateWatermark() {
	prevWatermark := sm.watermark

	// Constraint: watermark <= min(readFreq.Front(), pinFreq.Front()) <= maxReadIndex + 1
	readFreqEmpty := sm.readFreq.Len() == 0
	pinFreqEmpty := sm.pinFreq.Len() == 0
	if readFreqEmpty && pinFreqEmpty {
		sm.watermark = uint32(sm.maxReadIndex + 1)
	} else if readFreqEmpty {
		sm.watermark = min(sm.pinFreq.Front().Key(), uint32(sm.maxReadIndex+1))
	} else if pinFreqEmpty {
		sm.watermark = sm.readFreq.Front().Key()
	} else {
		sm.watermark = min(sm.pinFreq.Front().Key(), sm.readFreq.Front().Key())
	}

	if prevWatermark != sm.watermark {
		sm.readCond.Broadcast()
	}
}

func (sm *SegmentManager) AdvanceReader(prev uint32, delta uint32) (uint32, uint32) {
	sm.readMu.Lock()
	defer sm.readMu.Unlock()

	v, ok := sm.readFreq.GetValue(prev)
	if !ok {
		panic("advancing a non-existing reader")
	}
	isFront := sm.readFreq.Front().Key() == prev

	if v == 1 {
		sm.readFreq.Remove(prev)
	} else {
		sm.readFreq.Set(prev, v-1)
	}

	next := prev + delta
	sm.maxReadIndex = max(int64(next-1), sm.maxReadIndex)
	sm.readFreq.Set(next, getOrDefault(sm.readFreq, next, 0)+1)

	// If the minimum reader index is deleted, there's a chance to
	// advance the watermark.
	if isFront && v == 1 {
		sm.updateWatermark()
	}

	return next, sm.watermark
}

func (sm *SegmentManager) CloseReader(cur uint32) uint32 {
	sm.readMu.Lock()
	defer sm.readMu.Unlock()

	v, ok := sm.readFreq.GetValue(cur)
	if !ok {
		panic("closing a non-existing reader")
	}
	isFront := sm.readFreq.Front().Key() == cur

	if v == 1 {
		sm.readFreq.Remove(cur)
	} else {
		sm.readFreq.Set(cur, v-1)
	}

	// If the minimum reader index is deleted, there's a chance to
	// advance the watermark.
	if isFront && v == 1 {
		sm.updateWatermark()
	}

	return sm.watermark
}

func (sm *SegmentManager) segmentFile(index uint32, state SegmentFileState) string {
	switch state {
	case Plain:
		return fmt.Sprintf("segment.%d", index)
	case Compressing:
		return fmt.Sprintf("segment.%d.z.ing", index)
	case Compressed:
		return fmt.Sprintf("segment.%d.z", index)
	}
	panic("invalid state: " + strconv.Itoa(int(state)))
}

func (sm *SegmentManager) SegmentFile(index uint32, state SegmentFileState) string {
	return path.Join(sm.dir, sm.segmentFile(index, state))
}

var segmentFilePattern = regexp.MustCompile(`^segment\.([0-9]+)(\.\S+)?$`)

func ParseSegmentIndexAndState(file string) (uint32, SegmentFileState, error) {
	matches := segmentFilePattern.FindStringSubmatch(path.Base(file))
	if len(matches) == 0 {
		return 0, 0, errors.New("unrecognized segment")
	}
	if len(matches) >= 2 {
		idx, err := strconv.ParseUint(matches[1], 10, 32)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to parse segment index: %w", err)
		}

		switch matches[2] {
		case "":
			return uint32(idx), Plain, nil
		case ".z":
			return uint32(idx), Compressed, nil
		case ".z.ing":
			return uint32(idx), Compressing, nil
		default:
			return 0, 0, errors.New("unrecognized file state")
		}
	}
	panic("unreachable")
}

func (sm *SegmentManager) CurrentSegmentWatermark() uint32 {
	sm.readMu.RLock()
	defer sm.readMu.RUnlock()

	return sm.watermark
}

func (sm *SegmentManager) WaitUntilWatermarkAbove(ctx context.Context, index uint32) (uint32, error) {
	sm.readMu.RLock()

	for sm.watermark <= index {
		err := sm.readCond.WaitContext(ctx)
		if err != nil {
			return 0, err
		}
	}

	defer sm.readMu.RUnlock()
	return sm.watermark, nil
}

func (sm *SegmentManager) CurrentSegmentIndex() uint32 {
	sm.writeMu.RLock()
	defer sm.writeMu.RUnlock()

	return sm.index
}

func (sm *SegmentManager) IncSegmentIndex() uint32 {
	sm.writeMu.Lock()
	defer sm.writeMu.Unlock()

	sm.index++
	return sm.index
}

func NewSegmentManager(dir string) *SegmentManager {
	sm := &SegmentManager{
		dir:          dir,
		readFreq:     skiplist.New[uint32, int](cmp.Compare[uint32]),
		pinFreq:      skiplist.New[uint32, int](cmp.Compare[uint32]),
		maxReadIndex: -1,
	}
	cond := condvar.NewCond(sm.readMu.RLocker())
	sm.readCond = cond
	return sm
}

type FileChannel struct {
	dir            string
	segmentManager *SegmentManager
	position       *Position
	fileLock       *flock.Flock

	flushInterval   time.Duration
	rotateThreshold uint64

	lastErr  error
	closed   bool
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bgWg     sync.WaitGroup

	mu            sync.Mutex
	beginOffset   uint64
	currentOffset uint64
	f             *fs.AppendableFile

	compressionMethod CompressionMethod
}

var (
	DefaultFlushInterval   = 100 * time.Microsecond // 100us
	DefaultRotateThreshold = uint64(512 << 20)      // 512 MB
)

type Option func(*FileChannel)

func FlushInterval(d time.Duration) Option {
	if d < time.Microsecond {
		panic("flush interval is too small")
	}
	return func(channel *FileChannel) {
		channel.flushInterval = d
	}
}

func RotateThreshold(n uint64) Option {
	return func(channel *FileChannel) {
		channel.rotateThreshold = n
	}
}

// WithCompressionMethod sets the compression method for the file channel.
func WithCompressionMethod(method CompressionMethod) Option {
	return func(channel *FileChannel) {
		channel.compressionMethod = method
	}
}

func NewFileChannel(dir string, opts ...Option) *FileChannel {
	ctx, cancel := context.WithCancel(context.Background())
	fc := &FileChannel{
		dir:               dir,
		segmentManager:    NewSegmentManager(dir),
		flushInterval:     DefaultFlushInterval,
		rotateThreshold:   DefaultRotateThreshold,
		bgCtx:             ctx,
		bgCancel:          cancel,
		compressionMethod: Snappy,
	}

	for _, opt := range opts {
		opt(fc)
	}

	return fc
}

func OpenFileChannel(dir string, opts ...Option) (*FileChannel, error) {
	fc := NewFileChannel(dir, opts...)
	err := fc.Open()
	if err != nil {
		return nil, err
	}
	return fc, nil
}

func readCompressedSegmentHeader(file string) (CompressedSegmentHeader, error) {
	f, err := fs.OpenSequentialFile(file)
	if err != nil {
		return CompressedSegmentHeader{}, err
	}
	defer f.Close()

	var hBuf [SegmentHeaderBinarySize]byte
	_, err = io.ReadFull(f, hBuf[:])
	if err != nil {
		return CompressedSegmentHeader{}, err
	}

	h := CompressedSegmentHeader{}
	h.Decode(hBuf[:])

	return h, nil
}

func repairPlainSegment(file string) (PlainSegmentHeader, uint64, error) {
	f, err := fs.OpenSequentialFile(file)
	if err != nil {
		return PlainSegmentHeader{}, 0, err
	}
	defer f.Close()

	var hBuf [SegmentHeaderBinarySize]byte
	_, err = io.ReadFull(f, hBuf[:])
	if err != nil {
		return PlainSegmentHeader{}, 0, err
	}

	h := PlainSegmentHeader{}
	h.Decode(hBuf[:])

	lastReadCount := uint64(0)
	discard := utils.NewCountWriter(io.Discard)
	var mHeaderBuf [MessageHeaderBinarySize]byte
	msgCnt := 0
	for {
		err = ReadNext(f, discard, mHeaderBuf[:])
		if err != nil {
			break
		}
		msgCnt++
		lastReadCount = discard.Count()
	}

	lastOffset := lastReadCount + uint64(msgCnt*MessageHeaderBinarySize)
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrChecksumMismatch) {
		// Truncate to the last offset.
		err := os.Truncate(file, int64(lastOffset)+SegmentHeaderBinarySize)
		if err != nil {
			return PlainSegmentHeader{}, 0, fmt.Errorf("failed to truncate: %w", err)
		}
	} else if err != io.EOF {
		// Unrecoverable.
		return PlainSegmentHeader{}, 0, err
	}

	return h, lastOffset + h.BeginOffset, nil
}

func (fc *FileChannel) probeWritingFileAndInit(lastIndex uint32, states []SegmentFileState) error {
	fc.segmentManager.index = lastIndex

	if slices.Contains(states, Compressed) {
		h, err := readCompressedSegmentHeader(fc.segmentManager.SegmentFile(lastIndex, Compressed))
		if err != nil {
			return err
		}
		fc.position = NewPosition(h.EndOffset)
		fc.currentOffset = h.EndOffset
		fc.beginOffset = h.EndOffset

		fc.segmentManager.IncSegmentIndex()

		return fc.createSegmentFile()
	} else {
		h, endOffset, err := repairPlainSegment(fc.segmentManager.SegmentFile(lastIndex, Plain))
		if err != nil {
			return err
		}
		fc.position = NewPosition(endOffset)
		fc.currentOffset = endOffset
		fc.beginOffset = h.BeginOffset

		f, err := fs.OpenAppendableFile(fc.segmentManager.SegmentFile(lastIndex, Plain), 0644)
		if err != nil {
			return err
		}
		fc.f = f

		return nil
	}
}

func (fc *FileChannel) tryLock() error {
	if fc.fileLock != nil {
		return errors.New("already opened")
	}
	fileLock := flock.New(path.Join(fc.dir, "lock"))
	locked, err := fileLock.TryLock()
	if err != nil {
		return fmt.Errorf("failed to lock: %w", err)
	}
	if !locked {
		return errors.New("failed to locked, other process is using the channel")
	}
	fc.fileLock = fileLock
	return nil
}

func (fc *FileChannel) unlock() error {
	return fc.fileLock.Unlock()
}

func (fc *FileChannel) Open() error {
	if err := fc.tryLock(); err != nil {
		return err
	}

	d, err := os.Open(fc.dir)
	if err != nil {
		return err
	}
	defer d.Close()

	entries, err := d.ReadDir(-1)
	if err != nil {
		return err
	}

	segmentFiles := make(map[uint32][]SegmentFileState)
	lowestIndex, highestIndex := uint32(math.MaxUint32), uint32(0)
	for _, et := range entries {
		if et.IsDir() {
			continue
		}
		index, state, err := ParseSegmentIndexAndState(et.Name())
		if err == nil {
			// Remove compressing files.
			if state == Compressing {
				err := forceRemoveFile(path.Join(fc.dir, et.Name()))
				if err != nil {
					return err
				}
				continue
			}
			segmentFiles[index] = append(segmentFiles[index], state)
			if index < lowestIndex {
				lowestIndex = index
			}
			if index > highestIndex {
				highestIndex = index
			}
		}
	}

	if len(segmentFiles) > 0 && len(segmentFiles) != int(highestIndex-lowestIndex+1) {
		return errors.New("file channel corrupted: segments missing")
	}

	if len(segmentFiles) == 0 {
		fc.position = NewPosition(0)
		if err := fc.createSegmentFile(); err != nil {
			return err
		}
		fc.segmentManager.beginIndex = 0
		fc.segmentManager.watermark = 0
	} else {
		if err := fc.probeWritingFileAndInit(highestIndex, segmentFiles[highestIndex]); err != nil {
			return err
		}
		fc.segmentManager.beginIndex = lowestIndex
		fc.segmentManager.watermark = lowestIndex
	}

	toCompress := make([]uint32, 0, 4)
	for index, states := range segmentFiles {
		slices.Sort(states)
		switch {
		case slices.Equal(states, []SegmentFileState{Plain, Compressed}):
			_ = os.Remove(fc.segmentManager.SegmentFile(index, Plain))
		case slices.Equal(states, []SegmentFileState{Plain}):
			if index != fc.segmentManager.CurrentSegmentIndex() {
				toCompress = append(toCompress, index)
			}
		case slices.Equal(states, []SegmentFileState{Compressed}):
		default:
			return fmt.Errorf("unrecognized states: %+v", states)
		}
	}

	// Start background tasks.
	fc.bgWg.Add(1)
	go func() {
		defer fc.bgWg.Done()
		fc.flushAndNotifyLoop(fc.bgCtx)
	}()
	for _, idx := range toCompress {
		fc.bgWg.Add(1)
		cIdx := idx
		go func() {
			defer fc.bgWg.Done()
			err = fc.compress(fc.bgCtx, cIdx)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "failed to compress segment %d: %v\n", cIdx, err)
			}
		}()
	}
	fc.bgWg.Add(1)
	go func() {
		defer fc.bgWg.Done()
		fc.gcLoop(fc.bgCtx)
	}()

	return nil
}

func (fc *FileChannel) compressInner(index uint32, from, to string, method CompressionMethod) error {
	stats, err := os.Stat(from)
	if err != nil {
		return fmt.Errorf("failed to get stats of plain segment file: %w", err)
	}

	f, err := fs.OpenSequentialFile(from)
	if err != nil {
		return fmt.Errorf("failed to open plain segment file: %w", err)
	}
	defer f.Close()

	// Truncate before compressing.
	wf, err := fs.OpenFile(to, os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open compressing segment file: %w", err)
	}
	defer wf.Close()

	var buf [SegmentHeaderBinarySize]byte
	// Read plain file header.
	plainHeader := PlainSegmentHeader{}
	if _, err := io.ReadFull(f, buf[:]); err != nil {
		return fmt.Errorf("failed to read plain segment header: %w", err)
	}
	plainHeader.Decode(buf[:])

	// Write compressed file header.
	compressedHeader := CompressedSegmentHeader{
		SegmentID:         index,
		BeginOffset:       plainHeader.BeginOffset,
		EndOffset:         plainHeader.BeginOffset + uint64(stats.Size()-SegmentHeaderBinarySize),
		CompressionMethod: method,
	}
	compressedHeader.Encode(buf[:])
	_, err = wf.Write(buf[:])
	if err != nil {
		return fmt.Errorf("failed to write compressed segment header: %w", err)
	}

	// Copy.
	var w io.WriteCloser
	switch method {
	case Snappy:
		w = snappy.NewBufferedWriter(wf)
	case Gzip:
		w = gzip.NewWriter(wf)
	default:
		panic("unsupported compression method")
	}

	_, err = io.Copy(w, f)
	if err != nil {
		return fmt.Errorf("failed to compress: %w", err)
	}
	err = w.Close()
	if err != nil {
		return fmt.Errorf("failed to compress: %w", err)
	}

	return nil
}

func (fc *FileChannel) compress(ctx context.Context, index uint32) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if !fc.segmentManager.Pin(index) {
		return fmt.Errorf("failed to pin segment index %d", index)
	}
	defer fc.segmentManager.Unpin(index)

	plainFile := fc.segmentManager.SegmentFile(index, Plain)
	compressingFile := fc.segmentManager.SegmentFile(index, Compressing)

	// Compress.
	err := fc.compressInner(index, plainFile, compressingFile, fc.compressionMethod)
	if err != nil {
		return fmt.Errorf("failed to compress: %w", err)
	}

	// Rename and delete the original file.
	err = os.Rename(compressingFile, fc.segmentManager.SegmentFile(index, Compressed))
	if err != nil {
		return fmt.Errorf("failed to rename: %w", err)
	}

	_ = os.Remove(plainFile)
	return nil
}

func (fc *FileChannel) Close() error {
	if fc.fileLock == nil {
		return errors.New("not opened")
	}

	_ = fc.flushAndNotify()
	fc.position.Close()

	fc.closed = true
	fc.bgCancel()
	fc.bgWg.Wait()
	err := fc.f.Close()
	fc.f = nil

	_ = fc.unlock()
	fc.fileLock = nil

	return err
}

func forceRemoveFile(f string) error {
	err := os.Remove(f)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (fc *FileChannel) gcForIndex(index uint32) error {
	err1 := forceRemoveFile(fc.segmentManager.SegmentFile(index, Plain))
	err2 := forceRemoveFile(fc.segmentManager.SegmentFile(index, Compressed))
	return errors.Join(err1, err2)
}

func (fc *FileChannel) gcOnce(watermark uint32) {
	beginIndex := fc.segmentManager.GetBeginIndex()
	for index := beginIndex; index < watermark; index++ {
		_ = fc.gcForIndex(index)
	}
	fc.segmentManager.SetBeginIndex(watermark)
}

func (fc *FileChannel) gcLoop(ctx context.Context) {
	watermark := fc.segmentManager.CurrentSegmentWatermark()
	fc.gcOnce(watermark)

	for {
		nextWatermark, err := fc.segmentManager.WaitUntilWatermarkAbove(ctx, watermark)
		if err != nil {
			return
		}
		fc.gcOnce(nextWatermark)
		watermark = nextWatermark
	}
}

func (fc *FileChannel) flushAndNotifyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(fc.flushInterval):
			// FIXME: log the error
			_ = fc.flushAndNotifyWithLock()
		}
	}
}

func (fc *FileChannel) flushAndNotifyWithLock() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	return fc.flushAndNotify()
}

func (fc *FileChannel) flushAndNotify() error {
	err := fc.f.Flush()
	if err != nil {
		return err
	}
	fc.position.Update(fc.currentOffset)

	return nil
}

func (fc *FileChannel) needsRotate() bool {
	return fc.currentOffset-fc.beginOffset >= fc.rotateThreshold
}

func (fc *FileChannel) createSegmentFile() error {
	file := fc.segmentManager.SegmentFile(fc.segmentManager.CurrentSegmentIndex(), Plain)
	f, err := fs.OpenAppendableFile(file, 0644)
	if err != nil {
		return err
	}

	// Write header.
	var hBuf [SegmentHeaderBinarySize]byte
	h := PlainSegmentHeader{
		SegmentID:   fc.segmentManager.CurrentSegmentIndex(),
		BeginOffset: fc.currentOffset,
	}
	h.Encode(hBuf[:])
	_, err = f.Write(hBuf[:])
	if err != nil {
		err1 := f.Close()
		return errors.Join(err, err1)
	}

	// Flush to persist.
	if err := f.Flush(); err != nil {
		err1 := f.Close()
		return errors.Join(err, err1)
	}

	fc.beginOffset = fc.currentOffset
	fc.f = f
	return nil
}

func (fc *FileChannel) rotate() error {
	err := fc.flushAndNotify()
	if err != nil {
		return err
	}

	err = fc.f.Close()
	if err != nil {
		return err
	}

	curIndex := fc.segmentManager.CurrentSegmentIndex()

	// Seal the current one, and start compressing the previous one.
	// This is an optimization to save CPU cost when throughput of reading
	// and writing are almost equal so that when rotation happens, there
	// won't be compression on the file that reader is reading, and it provides
	// an opportunity for the GC removing a file before compressed.
	if curIndex > 0 {
		fc.bgWg.Add(1)
		go func() {
			defer fc.bgWg.Done()
			err = fc.compress(fc.bgCtx, curIndex-1)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "failed to compress segment %d: %v\n", curIndex-1, err)
			}
		}()
	}

	fc.segmentManager.IncSegmentIndex()
	return fc.createSegmentFile()
}

func (fc *FileChannel) Write(p []byte) (err error) {
	defer func() {
		if err != nil {
			fc.lastErr = err
		}
	}()

	if fc.lastErr != nil {
		return fc.lastErr
	}

	if fc.closed {
		return errors.New("closed")
	}

	// Construct header.
	header := MessageHeader{
		Length:   uint32(len(p)),
		Checksum: crc32.ChecksumIEEE(p),
	}
	var headerBuf [MessageHeaderBinarySize]byte
	header.Encode(headerBuf[:])

	fc.mu.Lock()
	defer fc.mu.Unlock()

	// If the segment size exceeds the threshold, seal and rotate to the next file.
	if fc.needsRotate() {
		if err := fc.rotate(); err != nil {
			return err
		}
	}

	// Write header + payload.
	_, err = fc.f.Write(headerBuf[:])
	if err != nil {
		return err
	}
	_, err = fc.f.Write(p)
	if err != nil {
		return err
	}

	fc.currentOffset += uint64(MessageHeaderBinarySize + len(p))

	return nil
}

func (fc *FileChannel) Flush() error {
	return fc.flushAndNotifyWithLock()
}

func (fc *FileChannel) Iterator() *Iterator {
	if fc.closed {
		panic("file channel closed")
	}
	return NewIterator(fc.segmentManager, fc.position, true)
}

func (fc *FileChannel) IteratorAcknowledgable() *Iterator {
	if fc.closed {
		panic("file channel closed")
	}
	return NewIterator(fc.segmentManager, fc.position, false)
}

func (fc *FileChannel) WriteOffset() uint64 {
	return fc.currentOffset
}

func (fc *FileChannel) FlushOffset() uint64 {
	return fc.position.Get()
}

func (fc *FileChannel) DiskUsage() (uint64, error) {
	var total uint64
	err := filepath.Walk(fc.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !segmentFilePattern.MatchString(info.Name()) {
			return nil
		}
		total += uint64(info.Size())
		return nil
	})
	return total, err
}
