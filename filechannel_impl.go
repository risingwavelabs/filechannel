package filechannel

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arkbriar/filechannel/internal/fs"
)

var ErrAckNotSupported = errors.New("ack not supported")

// Relative path to the position file.
const relatviePositionFile = "position"

var DefaultFileChannelOptions = FileChannelOptions{
	RollingFileSizeThreshold: 16 << 20, // 16 megabytes
	EnableCompression:        true,
	SyncInterval:             time.Second,
	SenderBufferSize:         128,
	FlushInterval:            10 * time.Millisecond,
	EnableAcknowledgable:     false,
}

// FileChannelOptions are the options of the file channel.
type FileChannelOptions struct {
	// RollingFileSizeThreshold is the size threshold for the writing file
	// to be sealed and rolled. This is the file size before any compression.
	RollingFileSizeThreshold int64

	// EnableCompression enables compression while writing the files. It
	// also indicates that reading from files should apply a reverse
	// decompression. For now, the snappy stream encoding/decoding is
	// only allowed and implemented compression/decompression.
	EnableCompression bool

	// SyncInterval is the interval of sync operations that help persist
	// the position and data to the underlying materials. Once a sync
	// happens, the readers on the related files should be able to read
	// the data that guaranteed to exist.
	SyncInterval time.Duration

	// FlushInterval is the interval of flush operations that help flush
	// the buffered data to the OS. It will notify the readers like sync does.
	// However, there's a very low chance that when a power outage happens and
	// data in OS get lost, and position recorded will be inconsistent with
	// the file.
	// Set it to negative to disable the feature.
	FlushInterval time.Duration

	// SenderBufferSize is the buffer size shared by all senders. If it is
	// exceeded, then the next Sends will block.
	SenderBufferSize uint

	// EnableAcknowledgable enables the async acknowledgable receiver. If
	// set to false, the receiver returned will not be aknowledgable.
	EnableAcknowledgable bool
}

// FileChannelOption defines the option when opening a file channel.
type FileChannelOption func(*FileChannelOptions)

// Compiler fence.
var _ FileChannel[any] = &fileChannel[any, noOpSerializer[any]]{}
var _ FanOutFileChannel[any] = &fileChannel[any, noOpSerializer[any]]{}

// fileChannel is the implementation of the [FileChannel] interface.
// It also implements the [FanOutFileChannel] if possible.
type fileChannel[T any, S Serializer[T]] struct {
	opts FileChannelOptions

	dir string

	serializer S

	ctx    context.Context
	signal context.CancelFunc
	wg     sync.WaitGroup

	positionSyncer *positionSyncer

	writeChan         chan *T
	writeClosed       chan struct{}
	writingFileBuffer *bytes.Buffer
	writingFileIndex  uint64
	writingFileOffset uint64
	writingFile       *fs.AppendableFile

	ackCommitTimer    *time.Timer
	readMu            sync.Mutex
	nextFanOutIndex   atomic.Uint64
	fanOutPositionMap map[uint64]pos
	rx                Receiver[T]
	minPos            pos
}

// OpenFileChannel opens or creates a [FileChannel] stored in the
// specified dir.
func OpenFileChannel[T any, S Serializer[T]](dir string, serializer S, opts ...FileChannelOption) (FileChannel[T], error) {
	fc := &fileChannel[T, S]{
		opts:              DefaultFileChannelOptions,
		dir:               dir,
		serializer:        serializer,
		nextFanOutIndex:   atomic.Uint64{},
		fanOutPositionMap: make(map[uint64]pos),
	}

	for _, opt := range opts {
		opt(&fc.opts)
	}

	// Close in case of failures when opening.
	err := fc.open()
	if err != nil {
		_ = fc.Close()
		return nil, err
	}

	return fc, nil
}

func (f *fileChannel[T, S]) path(sub string) string {
	return path.Join(f.dir, sub)
}

func (f *fileChannel[T, S]) filename(index uint64) string {
	return f.path(fmt.Sprintf("%x", index))
}

func (f *fileChannel[T, S]) open() error {
	f.ctx, f.signal = context.WithCancel(context.Background())

	// Open the position file and syncer.
	var err error
	f.positionSyncer, err = openPositionSyncer(f.path(relatviePositionFile))
	if err != nil {
		return err
	}

	f.ackCommitTimer = time.NewTimer(time.Second)

	f.writeChan = make(chan *T, f.opts.SenderBufferSize)
	f.writeClosed = make(chan struct{})

	// Setup the writing file.
	curPos := f.positionSyncer.Read()
	f.writingFileBuffer = bytes.NewBuffer(make([]byte, 0, 4096))
	f.writingFileIndex = curPos.writeFileIndex
	f.writingFileOffset = curPos.writeOffset

	// Set the initial reading position.
	f.fanOutPositionMap[0] = pos{
		fileIndex: curPos.readFileIndex,
		offset:    curPos.readOffset,
	}
	f.minPos = pos{
		fileIndex: curPos.readFileIndex,
		offset:    curPos.readOffset,
	}
	f.rx = &sharedFileChannelFanOutReceiver[T, S]{
		rx: f.allocateFanOutRx(0),
	}

	// FIXME: explore and truncate the current writing file before opening the appendable file.

	f.writingFile, err = fs.OpenAppendableFile(f.filename(f.writingFileIndex), os.ModePerm, f.opts.EnableCompression)
	if err != nil {
		return err
	}

	// Start background write and GC.
	f.startBackground(f.write)
	f.startBackground(f.gc)

	return nil
}

// Close implements [FileChannel].
func (f *fileChannel[T, S]) Close() error {
	// Close the write chan and wait until the write consumes
	// all buffering messagse and exits.
	if f.writeChan != nil {
		close(f.writeChan)
		<-f.writeClosed
	}

	// Signal background goroutines.
	f.signal()

	// Wait before all background goroutines exit.
	f.wg.Wait()

	errs := make([]error, 0)

	if f.positionSyncer != nil {
		err := f.positionSyncer.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (f *fileChannel[T, S]) flush(sync bool) (uint64, uint64, error) {
	var err error
	if sync {
		err = f.writingFile.Sync()
	} else {
		err = f.writingFile.Flush()
	}
	if err != nil {
		return 0, 0, err
	}

	return f.writingFileIndex, f.writingFileOffset, nil
}

func (f *fileChannel[T, S]) startBackground(fn func(ctx context.Context)) {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		fn(f.ctx)
	}()
}

func (f *fileChannel[T, S]) writeBytes(b []byte) (uint64, error) {
	// Header:
	// size (uint32) | crc32 (uint32)
	// Payload:
	// (bytes)

	const headerSize = 8
	checksum := crc32.ChecksumIEEE(b)

	var header [8]byte
	binary.BigEndian.PutUint32(header[0:], uint32(len(b)))
	binary.BigEndian.PutUint32(header[4:], checksum)

	_, err := f.writingFile.Write(header[:])
	if err != nil {
		return 0, err
	}
	_, err = f.writingFile.Write(b)
	if err != nil {
		return 0, err
	}

	f.writingFileOffset += uint64(len(b) + headerSize)
	return f.writingFileOffset, nil
}

func (f *fileChannel[T, S]) writeOnce(m *T) (uint64, error) {
	buf := f.writingFileBuffer
	buf.Reset()

	_, err := f.serializer.Write(buf, m)
	if err != nil {
		return 0, err
	}
	return f.writeBytes(buf.Bytes())
}

func (f *fileChannel[T, S]) roll() error {
	wf := f.writingFile
	err := wf.Sync()
	if err != nil {
		return err
	}

	nwf, err := fs.OpenAppendableFile(f.filename(f.writingFileIndex+1),
		os.ModePerm, f.opts.EnableCompression)
	if err != nil {
		return err
	}

	// Now everything's ready. Defer the close of the previous file.
	defer wf.Close()

	f.writingFileIndex++
	f.writingFileOffset = 0
	f.writingFile = nwf

	return nil
}

func (f *fileChannel[T, S]) write(ctx context.Context) {
	var flushTimer *time.Timer
	if f.opts.FlushInterval > 0 {
		flushTimer = time.NewTimer(f.opts.FlushInterval)
	} else {
		flushTimer = time.NewTimer(math.MaxInt64)
	}
	syncTimer := time.NewTimer(f.opts.SyncInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-f.writeChan:
			if !ok {
				close(f.writeClosed)
				return
			}
			curOffset, err := f.writeOnce(msg)
			if err != nil {
				panic("write failed: " + err.Error())
			}

			// Seal and roll if exceeds limit.
			if curOffset >= uint64(f.opts.RollingFileSizeThreshold) {
				if err := f.roll(); err != nil {
					panic("roll failed: " + err.Error())
				}
			}
		case <-flushTimer.C:
			fileIndex, offset, err := f.flush(false)
			if err != nil {
				panic("flush failed: " + err.Error())
			}
			err = f.positionSyncer.SyncWrite(fileIndex, offset)
			if err != nil {
				panic("sync write failed: " + err.Error())
			}
			flushTimer.Reset(f.opts.FlushInterval)
		case <-syncTimer.C:
			fileIndex, offset, err := f.flush(true)
			if err != nil {
				panic("sync failed: " + err.Error())
			}
			err = f.positionSyncer.SyncWrite(fileIndex, offset)
			if err != nil {
				panic("sync write failed: " + err.Error())
			}
			syncTimer.Reset(f.opts.SyncInterval)
		}
	}
}

func (f *fileChannel[T, S]) gc(ctx context.Context) {
	pos := f.positionSyncer.Read()
	before := pos.readFileIndex

	for {
		cur, err := f.positionSyncer.WaitForGCBefore(ctx, before)

		// Exit on error.
		if err != nil {
			return
		}

		// Files between [before,cur) can be purged.
		for i := before; i < cur; i++ {
			_ = os.Remove(f.filename(i))
			// TODO: log the GC events.
		}

		// Set the before margin to cur + 1
		before = cur + 1
	}
}

func (f *fileChannel[T, S]) updateMinPos() (uint64, uint64, bool) {
	minPos := f.fanOutPositionMap[0]

	for _, v := range f.fanOutPositionMap {
		if v.LessThan(minPos) {
			minPos = v
		}
	}

	changed := minPos == f.minPos

	return minPos.fileIndex, minPos.offset, changed
}

func (f *fileChannel[T, S]) updateAck(fid uint64, fileIndex uint64, offset uint64) (uint64, uint64, bool) {
	f.readMu.Lock()
	defer f.readMu.Unlock()

	f.fanOutPositionMap[fid] = pos{
		fileIndex: fileIndex,
		offset:    offset,
	}

	return f.updateMinPos()
}

func (f *fileChannel[T, S]) notifyRead(fi, off uint64) error {
	commit := false
	select {
	case <-f.ackCommitTimer.C:
		commit = false
		f.ackCommitTimer.Reset(time.Second)
	default:
	}

	return f.positionSyncer.SyncRead(fi, off, commit)
}

func (f *fileChannel[T, S]) ack(fid uint64, fileIndex uint64, offset uint64) error {
	fi, off, ok := f.updateAck(fid, fileIndex, offset)
	if ok {
		return f.notifyRead(fi, off)
	}
	return nil
}

func (f *fileChannel[T, S]) allocateFanOutRx(fid uint64) *fileChannelFanOutReceiver[T, S] {
	f.readMu.Lock()
	defer f.readMu.Unlock()

	p := f.fanOutPositionMap[0]
	f.fanOutPositionMap[fid] = p

	return &fileChannelFanOutReceiver[T, S]{
		fid: fid,
		fc:  f,
		posArr: []pos{
			{
				fileIndex: p.fileIndex,
				offset:    p.offset,
			},
		},
		buf: bytes.NewBuffer(nil),
	}
}

func (f *fileChannel[T, S]) releaseFanOutRx(fid uint64) error {
	if fid == 0 {
		return nil
	}

	f.readMu.Lock()
	delete(f.fanOutPositionMap, fid)
	fi, off, changed := f.updateMinPos()
	f.readMu.Unlock()

	if changed {
		return f.notifyRead(fi, off)
	} else {
		return nil
	}
}

// Sender implements [FileChannel].
func (f *fileChannel[T, S]) Sender() Sender[T] {
	return &fileChannelSender[T]{ch: f.writeChan}
}

// Receiver implements [FileChannel].
func (f *fileChannel[T, S]) Receiver() Receiver[T] {
	return f.rx
}

// Acknowledgable implements [FileChannel].
func (f *fileChannel[T, S]) Acknowledgable() bool {
	return f.opts.EnableAcknowledgable
}

// FanOutReceiver implements [FanOutFileChannel].
func (f *fileChannel[T, S]) FanOutReceiver() FanOutReceiver[T] {
	return f.allocateFanOutRx(f.nextFanOutIndex.Add(1))
}

// Compiler fence.
var _ Sender[any] = &fileChannelSender[any]{}

// fileChannelSender is the implementation of the [Sender] interface.
type fileChannelSender[T any] struct {
	ch chan *T
}

// Send implements the [Sender] interface.
func (s *fileChannelSender[T]) Send(ctx context.Context, obj *T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.ch <- obj:
		return nil
	}
}

// Compiler fence.
var _ FanOutReceiver[any] = &fileChannelFanOutReceiver[any, noOpSerializer[any]]{}

// fileChannelFanOutReceiver is the implementation of the [FanOutReceiver] interface.
type fileChannelFanOutReceiver[T any, S Serializer[T]] struct {
	fid     uint64
	fc      *fileChannel[T, S]
	posArr  []pos
	f       *fs.SequentialFile
	lastErr error
	buf     *bytes.Buffer

	wroteFileIndex uint64
	wroteOffset    uint64
}

func (r *fileChannelFanOutReceiver[T, S]) nextOffset() (uint64, uint64) {
	n := len(r.posArr)
	return r.posArr[n-1].fileIndex, r.posArr[n-1].offset
}

func (r *fileChannelFanOutReceiver[T, S]) openFile() error {
	if r.f == nil {
		fi, off := r.nextOffset()
		f, err := fs.OpenSequentialFile(r.fc.filename(fi), r.fc.opts.EnableCompression)
		if err != nil {
			return err
		}
		_, err = io.CopyN(io.Discard, f, int64(off))
		if err != nil {
			_ = f.Close()
			return err
		}
		r.f = f
	}
	return nil
}

func (r *fileChannelFanOutReceiver[T, S]) wait(ctx context.Context) error {
	fi, off := r.nextOffset()
	if fi < r.wroteFileIndex || (fi == r.wroteFileIndex && off < r.wroteOffset) {
		return nil
	}

	wfi, woff, err := r.fc.positionSyncer.WaitForWriteAfter(ctx, fi, off)
	if err != nil {
		return err
	}
	r.wroteFileIndex, r.wroteOffset = wfi, woff
	return nil
}

func (r *fileChannelFanOutReceiver[T, S]) readNext(ctx context.Context, obj *T) error {
	fi, off := r.nextOffset()

	var header [8]byte
	_, err := r.f.Read(header[:])
	if err != nil {
		return err
	}

	n := binary.BigEndian.Uint32(header[:4])
	checksum := binary.BigEndian.Uint32(header[4:])
	r.buf.Reset()
	r.buf.Grow(int(n))

	_, err = io.CopyN(r.buf, r.f, int64(n))
	if err != nil {
		return err
	}

	if crc32.ChecksumIEEE(r.buf.Bytes()) != checksum {
		return fmt.Errorf("checksum mismatch, file index %d, offset %d", fi, off)
	}

	_, err = r.fc.serializer.Read(r.buf, obj, int(n))
	if err != nil {
		return err
	}

	// FIXME: next offset should depend on read.
	r.posArr = append(r.posArr, pos{
		fileIndex: fi,
		offset:    off + uint64(n) + 8,
	})

	return nil
}

func (r *fileChannelFanOutReceiver[T, S]) skip(n int) (uint64, uint64) {
	if len(r.posArr) <= n {
		r.posArr = r.posArr[len(r.posArr)-1:]
	} else {
		r.posArr = r.posArr[n:]
	}
	return r.posArr[0].fileIndex, r.posArr[0].offset
}

// Receive implements the [Receiver] interface.
func (r *fileChannelFanOutReceiver[T, S]) Receive(ctx context.Context, obj *T) (err error) {
	if r.lastErr != nil {
		return r.lastErr
	}

	defer func() {
		if err != ctx.Err() {
			r.lastErr = err
		}
	}()

	for {
		err = r.openFile()
		if err != nil {
			return err
		}

		err = r.readNext(ctx, obj)
		if err != nil {
			if err == io.EOF {
				err := r.f.Close()
				if err != nil {
					return err
				}
				r.f = nil
				continue
			} else {
				return err
			}
		}

		if !r.fc.opts.EnableAcknowledgable {
			return r.ack(ctx, 1)
		}

		return nil
	}
}

func (r *fileChannelFanOutReceiver[T, S]) ack(ctx context.Context, msgN int) error {
	fi, off := r.skip(msgN)
	return r.fc.ack(r.fid, fi, off)
}

// Ack implements the [Acknowledgable] interface.
func (r *fileChannelFanOutReceiver[T, S]) Ack(ctx context.Context, msgN int) error {
	if r.fc.opts.EnableAcknowledgable {
		return r.ack(ctx, msgN)
	} else {
		return ErrAckNotSupported
	}
}

// Close implements the [FanOutReceiver] interface.
func (r *fileChannelFanOutReceiver[T, S]) Close() error {
	return r.fc.releaseFanOutRx(r.fid)
}

// Shared receiver with a mutex.
type sharedFileChannelFanOutReceiver[T any, S Serializer[T]] struct {
	mu sync.Mutex
	rx *fileChannelFanOutReceiver[T, S]
}

// Receive implements the [Receiver] interface.
func (r *sharedFileChannelFanOutReceiver[T, S]) Receive(ctx context.Context, obj *T) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rx.Receive(ctx, obj)
}

// Close implements the [Receiver] interface.
func (r *sharedFileChannelFanOutReceiver[T, S]) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rx.Close()
}

// Ack implements the [Receiver] interface.
func (r *sharedFileChannelFanOutReceiver[T, S]) Ack(ctx context.Context, msgN int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rx.Ack(ctx, msgN)
}
