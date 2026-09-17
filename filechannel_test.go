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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/risingwavelabs/filechannel/internal/utils"
)

type testingT interface {
	assert.TestingT
	FailNow()
}

func mkdirTemp(t testingT) string {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "filechannel_test")
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return tmpDir
}

func TestRepeatedClose(t *testing.T) {
	for _, failRotation := range []bool{false, true} {
		t.Run(fmt.Sprintf("rotationFailure=%t", failRotation), func(t *testing.T) {
			dir := t.TempDir()
			fc, err := OpenFileChannel(dir, RotateThreshold(1))
			if !assert.NoError(t, err) {
				return
			}
			defer func() { _ = fc.Close() }()
			tx := fc.Tx()
			assert.NoError(t, tx.Send(context.Background(), []byte("buffered")))
			var writeErr error
			if failRotation {
				assert.NoError(t, os.Mkdir(dir+"/segment.1", 0755))
				writeErr = tx.Send(context.Background(), []byte("not written"))
				assert.Error(t, writeErr)
			}
			assert.NoError(t, tx.Close())
			for i := 0; i < 2; i++ {
				if failRotation {
					assert.ErrorIs(t, fc.Close(), writeErr)
				} else {
					assert.NoError(t, fc.Close())
				}
			}
			assert.NotZero(t, fc.FlushOffset())
		})
	}
}

// closeWaitLocker reports when a closer releases the mutex in Cond.Wait.
type closeWaitLocker struct {
	sync.Locker
	waiting chan struct{}
}

func (l closeWaitLocker) Unlock() {
	l.Locker.Unlock()
	l.waiting <- struct{}{}
}

func TestConcurrentCloseWaitsForSender(t *testing.T) {
	for _, failRotation := range []bool{false, true} {
		t.Run(fmt.Sprintf("rotationFailure=%t", failRotation), func(t *testing.T) {
			dir := t.TempDir()
			fc, err := openFileChannel(dir, RotateThreshold(1))
			require.NoError(t, err)
			tx := fc.Tx()
			var wg sync.WaitGroup
			defer func() {
				_ = tx.Close()
				// Unblock all closers even if a regression makes the test fail.
				fc.wRefCond.Broadcast()
				wg.Wait()
				_ = fc.Close()
			}()

			var writeErr error
			if failRotation {
				require.NoError(t, tx.Send(context.Background(), []byte("buffered")))
				require.NoError(t, os.Mkdir(filepath.Join(dir, "segment.1"), 0755))
				writeErr = tx.Send(context.Background(), []byte("not written"))
				require.Error(t, writeErr)
			}

			waiting := make(chan struct{}, 2)
			fc.wRefCond = sync.NewCond(closeWaitLocker{&fc.wRefLock, waiting})
			closed := make(chan error, 2)
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					closed <- fc.Close()
				}()
			}
			for i := 0; i < 2; i++ {
				select {
				case <-waiting:
				case <-time.After(5 * time.Second):
					t.Fatal("Close did not wait for the sender")
				}
			}
			require.NoError(t, tx.Close())
			for i := 0; i < 2; i++ {
				select {
				case err := <-closed:
					if failRotation {
						require.ErrorIs(t, err, writeErr)
					} else {
						require.NoError(t, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Close stayed blocked after the last sender closed")
				}
			}
		})
	}
}

func TestReceiversDrainAfterRotationFailure(t *testing.T) {
	dir := t.TempDir()
	fc, err := OpenAckFileChannel(dir, RotateThreshold(1), FlushInterval(time.Hour))
	require.NoError(t, err)
	defer func() { _ = fc.Close() }()
	tx := fc.Tx()
	defer func() { _ = tx.Close() }()
	require.NoError(t, tx.Send(context.Background(), []byte("published")))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "segment.1"), 0755))
	writeErr := tx.Send(context.Background(), []byte("not written"))
	require.Error(t, writeErr)

	for _, rx := range []Receiver{fc.Rx(), fc.RxAck()} {
		defer func() { _ = rx.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		data, err := rx.Recv(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte("published"), data)
		if ackRx, ok := rx.(AckReceiver); ok {
			require.NoError(t, ackRx.Ack(1))
		}
		_, err = rx.Recv(ctx)
		require.ErrorIs(t, err, ErrChannelClosed)
	}
	require.ErrorIs(t, tx.Send(context.Background(), nil), writeErr)
}

func TestFileChannel(t *testing.T) {
	tmpDir := mkdirTemp(t)
	defer os.RemoveAll(tmpDir)

	fch, err := OpenFileChannel(tmpDir)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer fch.Close()

	msg := []byte("Hello world!")

	tx := fch.Tx()
	defer tx.Close()
	err = tx.Send(context.Background(), msg)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	rx := fch.Rx()
	defer rx.Close()
	p, err := rx.Recv(context.Background())
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.Equal(t, msg, p) {
		t.FailNow()
	}
}

func printMemoryStats(title string) {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)

	fmt.Println(title)
	fmt.Printf(`Alloc: %s
TotalAlloc: %s
Sys: %s
Lookups: %d
Mallocs: %d
Frees: %d
HeapAlloc: %s
HeapSys: %s
HeapIdle: %s
HeapInuse: %s
HeapReleased: %s
HeapObjects: %d
`,
		utils.ByteCountIEC(memStats.Alloc),
		utils.ByteCountIEC(memStats.TotalAlloc),
		utils.ByteCountIEC(memStats.Sys),
		memStats.Lookups,
		memStats.Mallocs,
		memStats.Frees,
		utils.ByteCountIEC(memStats.HeapAlloc),
		utils.ByteCountIEC(memStats.HeapSys),
		utils.ByteCountIEC(memStats.HeapIdle),
		utils.ByteCountIEC(memStats.HeapInuse),
		utils.ByteCountIEC(memStats.HeapReleased),
		memStats.HeapObjects,
	)
	fmt.Println()
}

func TestFileChannel_MemoryConsumption(t *testing.T) {
	tmpDir := mkdirTemp(t)
	defer printMemoryStats("=========== AFTER ALL ===========")
	defer os.RemoveAll(tmpDir)

	printMemoryStats("=========== BEFORE ALL ===========")

	fch, err := OpenFileChannel(tmpDir)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer fch.Close()

	printMemoryStats("=========== AFTER OPEN ===========")

	msg := []byte("Hello world!")

	const iterateCount = 2 << 20
	tx := fch.Tx()
	defer tx.Close()
	for i := 0; i < iterateCount; i++ {
		err = tx.Send(context.Background(), msg)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}

	printMemoryStats("=========== AFTER SENDING ===========")

	rx := fch.Rx()
	defer rx.Close()
	for i := 0; i < iterateCount; i++ {
		p, err := rx.Recv(context.Background())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		if !assert.Equal(t, msg, p) {
			t.FailNow()
		}
	}

	printMemoryStats("=========== AFTER RECEIVING ===========")
}
