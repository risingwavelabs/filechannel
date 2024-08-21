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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/risingwavelabs/filechannel/internal/utils"
)

var testDir = path.Join(os.TempDir(), "file_channel")

var magicBytes = []byte{
	0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8,
}

func magicPayload(n int) []byte {
	if n < len(magicBytes) {
		return magicBytes[:n]
	} else {
		r := bytes.Repeat(magicBytes, (n+len(magicBytes)-1)/len(magicBytes))
		return r[:n]
	}
}

type failNow interface {
	assert.TestingT

	FailNow()
}

func setup(t failNow, name string, opts ...Option) *FileChannel {
	targetDir := path.Join(testDir, name)

	// Remove the directory.
	err := os.RemoveAll(targetDir)
	if !os.IsNotExist(err) && !assert.NoError(t, err, "failed to remove directory") {
		t.FailNow()
	}

	// Create the directory.
	err = os.MkdirAll(targetDir, os.ModePerm)
	if !assert.NoError(t, err, "failed to create directory") {
		t.FailNow()
	}

	// Open file channel and return.
	fc, err := OpenFileChannel(targetDir, opts...)
	if !assert.NoError(t, err, "failed to open file channel") {
		t.FailNow()
	}

	return fc
}

func teardown(t failNow, fc *FileChannel, ignoreClosed bool) {
	targetDir := fc.dir

	// Close the file channel.
	err := fc.Close()
	if !ignoreClosed || errors.Is(err, ErrChannelClosed) {
		assert.NoError(t, err, "failed to close file channel")
	}

	// Remove the directory.
	err = os.RemoveAll(targetDir)
	if !assert.NoError(t, err, "failed to remove directory") {
		t.FailNow()
	}
}

func writeAll(t failNow, fc *FileChannel, data func(int) []byte, size int) {
	for i := 0; i < size; i++ {
		err := fc.Write(data(i))
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}
	err := fc.Flush()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
}

func writeBytesList(t failNow, fc *FileChannel, bytesList [][]byte) {
	writeAll(t, fc, func(i int) []byte {
		return bytesList[i]
	}, len(bytesList))
}

func writeStringList(t failNow, fc *FileChannel, strs []string) {
	writeAll(t, fc, func(i int) []byte {
		return []byte(strs[i])
	}, len(strs))
}

func readAll(t failNow, it *Iterator, data func(int) []byte, size int, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for i := 0; i < size; i++ {
		p, err := it.Next(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		if !assert.Equal(t, data(i), p) {
			t.FailNow()
		}
	}

	_, err := it.TryNext()
	if !assert.ErrorIs(t, err, ErrNotEnoughMessages) {
		t.FailNow()
	}
}

func readBytesList(t failNow, it *Iterator, bytesList [][]byte, timeout time.Duration) {
	readAll(t, it, func(i int) []byte {
		return bytesList[i]
	}, len(bytesList), timeout)
}

// nolint:unused
func readStringList(t failNow, it *Iterator, strs []string, timeout time.Duration) {
	readAll(t, it, func(i int) []byte {
		return []byte(strs[i])
	}, len(strs), timeout)
}

func TestParseSegmentIndexAndState(t *testing.T) {
	testcases := map[string]struct {
		file  string
		index uint32
		state SegmentFileState
		err   error
	}{
		"plain": {
			file:  "segment.1",
			index: 1,
			state: Plain,
		},
		"compressing": {
			file:  "segment.2.z.ing",
			index: 2,
			state: Compressing,
		},
		"compressed": {
			file:  "segment.3.z",
			index: 3,
			state: Compressed,
		},
		"unknown": {
			file: "unknown",
			err:  errors.New("unrecognized segment"),
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			index, state, err := ParseSegmentIndexAndState(tc.file)
			if err != nil {
				if tc.err == nil {
					t.Fatalf("unexpected error: %s", err)
				} else if tc.err.Error() != err.Error() {
					t.Fatalf("expect error: %s, real error: %s", tc.err, err)
				}
			} else {
				if tc.err != nil {
					t.Fatalf("expect error: %s", tc.err)
				}
				if tc.index != index {
					t.Fatalf("expect value: %d, real value: %d", tc.index, index)
				}
				if tc.state != state {
					t.Fatalf("expect value: %d, real value: %d", tc.state, state)
				}
			}
		})
	}
}

func TestFileChannel_Simple(t *testing.T) {
	fc := setup(t, "test_file_channel_simple")
	defer teardown(t, fc, true)

	messages := [][]byte{
		{0x1},
		{0x1, 0x2},
		{0x1, 0x2, 0x3},
		{0x1, 0x2, 0x3, 0x4},
		{0x1, 0x2, 0x3, 0x4, 0x5},
	}

	writeBytesList(t, fc, messages)

	it := fc.Iterator()
	readBytesList(t, it, messages, time.Second)

	assert.NoError(t, it.Close())
}

func TestFileChannel_ReadCompressed(t *testing.T) {
	fc := setup(t, "test_file_channel_read_compressed", RotateThreshold(1<<20))
	defer teardown(t, fc, true)

	const payloadSize, totalSize = 128, 10 << 20
	msgNum := totalSize / payloadSize
	payload := magicPayload(payloadSize)

	writeAll(t, fc, func(_ int) []byte { return payload }, msgNum)

	// Wait until the first segment is compressed.
	checkFileChannelDir(t, fc.dir, func(entries []os.DirEntry) bool {
		return slices.ContainsFunc(entries, func(entry os.DirEntry) bool {
			return entry.Name() == "segment.0.z"
		})
	}, 10*time.Second)

	it := fc.Iterator()
	readAll(t, it, func(_ int) []byte { return payload }, msgNum, 10*time.Second)

	assert.NoError(t, it.Close())
}

func TestFileChannel_ReadCompressed_Gzip(t *testing.T) {
	fc := setup(t, "test_file_channel_read_compressed_gzip", RotateThreshold(1<<20), WithCompressionMethod(Gzip))
	defer teardown(t, fc, true)

	const payloadSize, totalSize = 128, 10 << 20
	msgNum := totalSize / payloadSize
	payload := magicPayload(payloadSize)

	writeAll(t, fc, func(_ int) []byte { return payload }, msgNum)

	// Wait until the first segment is compressed.
	checkFileChannelDir(t, fc.dir, func(entries []os.DirEntry) bool {
		return slices.ContainsFunc(entries, func(entry os.DirEntry) bool {
			return entry.Name() == "segment.0.z"
		})
	}, 10*time.Second)

	it := fc.Iterator()
	readAll(t, it, func(_ int) []byte { return payload }, msgNum, 10*time.Second)

	assert.NoError(t, it.Close())
}

func TestFileChannel_ReadCompressed_HoldDeleted(t *testing.T) {
	fc := setup(t, "test_file_channel_read_compressed", RotateThreshold(1<<20))
	defer teardown(t, fc, true)

	itCreatedBeforeCompression := fc.Iterator()

	const payloadSize, totalSize = 128, 10 << 20
	msgNum := totalSize / payloadSize
	payload := magicPayload(payloadSize)

	writeAll(t, fc, func(_ int) []byte { return payload }, msgNum)

	// Wait until the first segment is compressed.
	checkFileChannelDir(t, fc.dir, func(entries []os.DirEntry) bool {
		return slices.ContainsFunc(entries, func(entry os.DirEntry) bool {
			return entry.Name() == "segment.0.z"
		})
	}, 10*time.Second)

	itCreatedAfterCompression := fc.Iterator()

	readAll(t, itCreatedBeforeCompression, func(_ int) []byte { return payload }, msgNum, 10*time.Second)
	readAll(t, itCreatedAfterCompression, func(_ int) []byte { return payload }, msgNum, 10*time.Second)

	assert.NoError(t, itCreatedBeforeCompression.Close())
	assert.NoError(t, itCreatedAfterCompression.Close())
}

func testFileChannelWithRandomStrings(t *testing.T, rand *rand.Rand, minLen, maxLen, size int, parallelism int, opts ...Option) {
	fc := setup(t, fmt.Sprintf("file_channel_benchmark_random_%d_%d_%d", minLen, maxLen, size), opts...)
	defer teardown(t, fc, false)

	strList := make([]string, size)
	for i := 0; i < size; i++ {
		strList[i] = utils.RandomString(rand, rand.Intn(maxLen-minLen)+minLen)
	}

	writeStringList(t, fc, strList)

	// Read them all.
	wg := &sync.WaitGroup{}
	for p := 0; p < parallelism; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			it := fc.Iterator()
			for i := 0; i < size; i++ {
				b, err := it.Next(context.Background())
				if !assert.NoError(t, err) {
					return
				}
				if !assert.Equal(t, strList[i], string(b)) {
					return
				}
			}

			// Next read should time out.
			timeoutNext := func(it *Iterator, t time.Duration) ([]byte, error) {
				ctx, cancel := context.WithTimeout(context.Background(), t)
				defer cancel()
				return it.Next(ctx)
			}
			_, err := timeoutNext(it, time.Millisecond)
			if !assert.ErrorIs(t, err, context.DeadlineExceeded) {
				return
			}
		}()
	}

	wg.Wait()
}

func TestFileChannelWithRandomStrings(t *testing.T) {
	testFileChannelWithRandomStrings(t, rand.New(rand.NewSource(1)), 0, 512, 1<<15, 1)
}

func TestFileChannelWithRandomStringsAndSmallRotationThreshold(t *testing.T) {
	testFileChannelWithRandomStrings(t, rand.New(rand.NewSource(1)),
		0, 512, 1<<20, 1, RotateThreshold(10<<20))
}

func TestFileChannelWithRandomStrings_Parallel_2(t *testing.T) {
	testFileChannelWithRandomStrings(t, rand.New(rand.NewSource(2)), 0, 128, 1<<15, 4)
}

func TestFileChannelWithRandomStrings_Parallel_4(t *testing.T) {
	testFileChannelWithRandomStrings(t, rand.New(rand.NewSource(3)), 0, 64, 1<<15, 4)
}

func listDir(dir string) ([]os.DirEntry, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return f.ReadDir(-1)
}

func checkFileChannelDir(t *testing.T, targetDir string, prediction func([]os.DirEntry) bool, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	for {
		dirEntries, err := listDir(targetDir)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		if prediction(dirEntries) {
			break
		}
		entryNames := make([]string, 0, len(dirEntries))
		for _, et := range dirEntries {
			entryNames = append(entryNames, et.Name())
		}
		select {
		case <-timer.C:
			t.Fatalf("Check failed. Unexpected files found:\n%s",
				strings.Join(entryNames, " "))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestFileChannel_GC(t *testing.T) {
	fc := setup(t, "test_file_channel_gc", RotateThreshold(1<<20))
	defer teardown(t, fc, true)

	const payloadSize, totalSize = 128, 10 << 20
	msgNum := totalSize / payloadSize
	payload := magicPayload(payloadSize)

	writeAll(t, fc, func(_ int) []byte { return payload }, msgNum)

	// Read them all.
	it := fc.Iterator()
	readAll(t, it, func(_ int) []byte { return payload }, msgNum, 10*time.Second)
	assert.NoError(t, it.Close())

	// Check the directory for at most 2 seconds. There should be only two files and one
	// of them is the lock file.
	checkFileChannelDir(t, fc.dir, func(entries []os.DirEntry) bool {
		return len(entries) == 2 && slices.ContainsFunc(entries, func(entry os.DirEntry) bool {
			return entry.Name() == "lock"
		})
	}, 5*time.Second)
}

func TestFileChannel_GCWithoutAck(t *testing.T) {
	fc := setup(t, "test_file_channel_gc_without_ack", RotateThreshold(1<<20))
	defer teardown(t, fc, true)

	// Write 10 MB data.
	const payloadSize = 128
	msgNum := (10 << 20) / payloadSize
	payload := magicPayload(payloadSize)

	writeAll(t, fc, func(_ int) []byte { return payload }, msgNum)

	// Read them all.
	it := fc.IteratorAcknowledgable()
	readAll(t, it, func(_ int) []byte { return payload }, msgNum, 10*time.Second)
	assert.NoError(t, it.Close())

	// Check the directory for at most 2 seconds. The first segment file should still exist.
	checkFileChannelDir(t, fc.dir, func(entries []os.DirEntry) bool {
		return slices.ContainsFunc(entries, func(entry os.DirEntry) bool {
			return entry.Name() == "segment.0" || entry.Name() == "segment.0.z"
		})
	}, 2*time.Second)
}

func testFileChannelRecoveryWriteAndReadWithoutAck(t *testing.T, fc *FileChannel, payload []byte, size int) {
	writeAll(t, fc, func(_ int) []byte { return payload }, size)

	it := fc.IteratorAcknowledgable()
	readAll(t, it, func(_ int) []byte { return payload }, size, 10*time.Second)
	assert.NoError(t, it.Close())
}

func testFileChannelRecoveryReadAll(t *testing.T, fc *FileChannel, payload []byte, size int) {
	it := fc.Iterator()
	readAll(t, it, func(_ int) []byte { return payload }, size, 10*time.Second)
	assert.NoError(t, it.Close())
}

func TestFileChannel_Recovery(t *testing.T) {
	fc := setup(t, "test_file_channel_recovery", RotateThreshold(1<<20))
	defer func() {
		teardown(t, fc, false)
	}()

	const payloadSize, totalSize = 128, 10 << 20
	msgNum := totalSize / payloadSize
	payload := magicPayload(payloadSize)

	// Write and read without ack (to reserve data).
	testFileChannelRecoveryWriteAndReadWithoutAck(t, fc, payload, msgNum)

	// Close the original channel.
	if !assert.NoError(t, fc.Close()) {
		t.FailNow()
	}

	var err error
	fc, err = OpenFileChannel(fc.dir, RotateThreshold(1<<20))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	// Read all.
	testFileChannelRecoveryReadAll(t, fc, payload, msgNum)
}
