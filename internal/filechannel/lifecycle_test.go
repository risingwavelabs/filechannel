// Copyright 2023 RisingWave Labs
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
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelLifecycle(t *testing.T) {
	fc := NewFileChannel(t.TempDir(), FlushInterval(time.Hour))
	require.ErrorIs(t, fc.Write(nil), ErrChannelClosed)
	require.ErrorIs(t, fc.Flush(), ErrChannelClosed)
	require.Zero(t, fc.FlushOffset())
	require.Panics(t, func() { fc.Iterator() })
	require.NoError(t, fc.Open())
	require.ErrorIs(t, fc.Open(), errAlreadyOpened)
	require.NoError(t, fc.Write([]byte("last buffered message")))
	wantOffset := fc.WriteOffset()
	require.NoError(t, fc.Close())
	require.Equal(t, wantOffset, fc.FlushOffset())
	require.NoError(t, fc.Close())
	require.ErrorIs(t, fc.Open(), ErrChannelClosed)
	require.ErrorIs(t, fc.Write(nil), ErrChannelClosed)
	require.ErrorIs(t, fc.Flush(), ErrChannelClosed)
	require.Panics(t, func() { fc.IteratorAcknowledgable() })

	reopened, err := OpenFileChannel(fc.dir)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	require.Equal(t, wantOffset, reopened.WriteOffset())
}

func TestCloseConcurrentWithWritesAndFlushes(t *testing.T) {
	for attempt := 0; attempt < 30; attempt++ {
		fc, err := OpenFileChannel(t.TempDir(), FlushInterval(time.Microsecond), RotateThreshold(128))
		require.NoError(t, err)
		require.NoError(t, fc.Write([]byte("buffered")))
		start := make(chan struct{})
		errs := make(chan error, 6)
		var wg sync.WaitGroup
		for worker := 0; worker < 6; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 50; i++ {
					var err error
					switch worker {
					case 0, 1:
						err = fc.Write([]byte("concurrent message"))
					case 2, 3:
						err = fc.Flush()
					default:
						err = fc.Close()
					}
					if err != nil {
						if !errors.Is(err, ErrChannelClosed) {
							errs <- err
						}
						return
					}
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		require.Equal(t, fc.WriteOffset(), fc.FlushOffset())
		reopened, err := OpenFileChannel(fc.dir)
		require.NoError(t, err)
		require.Equal(t, fc.WriteOffset(), reopened.WriteOffset())
		require.NoError(t, reopened.Close())
	}
}

func TestClosingRejectsOperationsAndWaitsForWorkers(t *testing.T) {
	fc, err := OpenFileChannel(t.TempDir(), FlushInterval(time.Hour))
	require.NoError(t, err)
	// Keep a worker alive to observe the closing state and overlapping Close calls.
	fc.bgWg.Add(1)
	defer func() { _ = fc.Close() }()
	var release sync.Once
	defer release.Do(fc.bgWg.Done)
	closed := make(chan error, 2)
	go func() { closed <- fc.Close() }()
	select {
	case <-fc.bgCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel workers")
	}
	go func() { closed <- fc.Close() }()
	require.ErrorIs(t, fc.Write(nil), ErrChannelClosed)
	require.ErrorIs(t, fc.Flush(), ErrChannelClosed)
	require.ErrorIs(t, fc.Open(), ErrChannelClosed)
	require.Panics(t, func() { fc.Iterator() })
	other, err := OpenFileChannel(fc.dir)
	require.Error(t, err)
	require.Nil(t, other)
	select {
	case <-closed:
		t.Fatal("Close returned before its worker finished")
	default:
	}
	release.Do(fc.bgWg.Done)
	for i := 0; i < 2; i++ {
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not finish after workers stopped")
		}
	}
}

func TestRotationFailureCanCloseAndReopen(t *testing.T) {
	fc, err := OpenFileChannel(t.TempDir(), RotateThreshold(1), FlushInterval(time.Microsecond))
	require.NoError(t, err)
	defer func() { _ = fc.Close() }()
	payload := []byte("persisted before rotation")
	require.NoError(t, fc.Write(payload))
	// A directory at the replacement path forces creation to fail on every OS.
	blockedPath := fc.segmentManager.SegmentFile(1, Plain)
	require.NoError(t, os.Mkdir(blockedPath, 0755))
	rotationErr := fc.Write([]byte("not written"))
	require.Error(t, rotationErr)
	require.ErrorIs(t, fc.Write(nil), rotationErr)
	require.ErrorIs(t, fc.Flush(), rotationErr)
	require.ErrorIs(t, fc.Close(), rotationErr)
	require.ErrorIs(t, fc.Close(), rotationErr)
	require.Nil(t, fc.f)
	require.Nil(t, fc.fileLock)
	require.NoError(t, os.Remove(blockedPath))

	reopened, err := OpenFileChannel(fc.dir)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	it := reopened.Iterator()
	defer func() { _ = it.Close() }()
	got, err := it.TryNext()
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoError(t, reopened.Write([]byte("after recovery")))
}

type failingSegmentWriter struct {
	segmentWriter
	writeErr error
	flushErr error
	closeErr error
	closes   int
}

func (w *failingSegmentWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		n, _ := w.segmentWriter.Write(p[:1])
		return n, w.writeErr
	}
	return w.segmentWriter.Write(p)
}

func (w *failingSegmentWriter) Flush() error {
	if w.flushErr != nil {
		return w.flushErr
	}
	return w.segmentWriter.Flush()
}

func (w *failingSegmentWriter) Close() error {
	w.closes++
	return errors.Join(w.segmentWriter.Close(), w.closeErr)
}

func TestReceiversDrainAfterFlushFailure(t *testing.T) {
	fc, err := OpenFileChannel(t.TempDir(), FlushInterval(time.Hour))
	require.NoError(t, err)
	defer func() { _ = fc.Close() }()
	require.NoError(t, fc.Write([]byte("published")))
	require.NoError(t, fc.Flush())
	require.NoError(t, fc.Write([]byte("unpublished")))
	flushErr := errors.New("injected flush failure")
	fc.mu.Lock()
	fc.f = &failingSegmentWriter{segmentWriter: fc.f, flushErr: flushErr}
	fc.mu.Unlock()
	require.ErrorIs(t, fc.Flush(), flushErr)

	for _, it := range []*Iterator{fc.Iterator(), fc.IteratorAcknowledgable()} {
		defer func() { _ = it.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		data, err := it.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte("published"), data)
		if !it.autoAck {
			require.NoError(t, it.Ack(1))
		}
		_, err = it.Next(ctx)
		require.ErrorIs(t, err, ErrChannelClosed)
	}
	require.ErrorIs(t, fc.Write(nil), flushErr)
	require.ErrorIs(t, fc.Flush(), flushErr)
}

func TestWriterFailuresAreStickyAndCleanupIsComplete(t *testing.T) {
	for _, operation := range []string{"write", "flush", "background flush", "close flush", "rotation close"} {
		t.Run(operation, func(t *testing.T) {
			interval := time.Hour
			if operation == "background flush" {
				interval = time.Microsecond
			}
			fc, err := OpenFileChannel(t.TempDir(), FlushInterval(interval), RotateThreshold(1))
			require.NoError(t, err)
			defer func() { _ = fc.Close() }()
			it := fc.Iterator()
			defer func() { _ = it.Close() }()
			ioErr := errors.New("injected I/O failure")
			cleanupErr := errors.New("injected close failure")
			writer := &failingSegmentWriter{closeErr: cleanupErr}
			if operation == "background flush" {
				writer.flushErr = ioErr
			}
			fc.mu.Lock()
			writer.segmentWriter = fc.f
			fc.f = writer
			fc.mu.Unlock()
			switch operation {
			case "write":
				writer.writeErr = ioErr
				err = fc.Write([]byte("partial message"))
			case "flush", "close flush":
				require.NoError(t, fc.Write([]byte("unpublished message")))
				fc.mu.Lock()
				writer.flushErr = ioErr
				fc.mu.Unlock()
				if operation == "flush" {
					err = fc.Flush()
				} else {
					err = fc.Close()
				}
				require.Zero(t, fc.FlushOffset())
			case "background flush":
				select {
				case <-fc.bgCtx.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("background flush failure did not stop workers")
				}
				err = fc.Flush()
				require.Zero(t, fc.FlushOffset())
			case "rotation close":
				require.NoError(t, fc.Write([]byte("rotate")))
				writer.closeErr = errors.Join(ioErr, cleanupErr)
				err = fc.Write([]byte("not written"))
			}
			require.ErrorIs(t, err, ioErr)
			if operation != "close flush" {
				require.ErrorIs(t, fc.Write(nil), ioErr)
				require.ErrorIs(t, fc.Flush(), ioErr)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = fc.position.Wait(ctx, func(uint64) bool { return false })
			require.ErrorIs(t, err, ErrChannelClosed)
			for i := 0; i < 2; i++ {
				err = fc.Close()
				require.ErrorIs(t, err, ioErr)
				require.ErrorIs(t, err, cleanupErr)
			}
			require.Equal(t, 1, writer.closes)
			require.Nil(t, fc.f)
			require.Nil(t, fc.fileLock)
			reopened, err := OpenFileChannel(fc.dir)
			require.NoError(t, err)
			require.NoError(t, reopened.Close())
		})
	}
}

func TestOpenFailureCleansUpWriterAndLock(t *testing.T) {
	dir := t.TempDir()
	// Both names parse as segment zero. Validation fails after opening its writer.
	for _, name := range []string{"segment.0", "segment.00"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), make([]byte, SegmentHeaderBinarySize), 0644))
	}
	fc := NewFileChannel(dir)
	openErr := fc.Open()
	require.Error(t, openErr)
	require.Nil(t, fc.f)
	require.Nil(t, fc.fileLock)
	require.Nil(t, fc.position)
	require.Panics(t, func() { fc.Iterator() })
	require.Panics(t, func() { fc.IteratorAcknowledgable() })
	require.ErrorIs(t, fc.Write(nil), openErr)
	require.ErrorIs(t, fc.Flush(), openErr)
	require.ErrorIs(t, fc.Close(), openErr)
	require.NoError(t, os.Remove(filepath.Join(dir, "segment.00")))
	reopened, err := OpenFileChannel(dir)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}
