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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	// Write some data.
	for _, msg := range messages {
		err := fc.Write(msg)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}

	// Create an iterator and then close it.
	it := fc.Iterator()
	if !assert.NoError(t, fc.Close()) {
		t.FailNow()
	}

	// Read them all.
	i := 0
	for i < len(messages) {
		msg, err := it.Next(context.Background())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.Equal(t, msg, messages[i])
		i++
	}

	_, err := it.Next(context.Background())
	assert.Error(t, err, ErrChannelClosed)
}

func TestFileChannel_ReadCompressed(t *testing.T) {
	fc := setup(t, "test_file_channel_read_compressed", RotateThreshold(1<<20))
	defer teardown(t, fc, true)

	// Write 10 MB data.
	const payloadSize = 128
	msgNum := (10 << 20) / payloadSize
	payload := magicPayload(128)

	for i := 0; i < msgNum; i++ {
		err := fc.Write(payload)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}

	// Read them all. Close channel to make sure the heading segments were
	// all compressed.
	it := fc.Iterator()
	if !assert.NoError(t, fc.Close()) {
		t.FailNow()
	}
	for i := 0; i < msgNum; i++ {
		msg, err := it.Next(context.Background())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		if !assert.Equal(t, payload, msg) {
			t.FailNow()
		}
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(rand *rand.Rand, n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func testFileChannelWithRandomStrings(t *testing.T, rand *rand.Rand, minLen, maxLen, size int, parallelism int) {
	fc := setup(t, fmt.Sprintf("file_channel_benchmark_random_%d_%d_%d", minLen, maxLen, size))
	defer teardown(t, fc, false)

	strList := make([]string, size)
	totalStrLen := 0
	for i := 0; i < size; i++ {
		strList[i] = RandStringRunes(rand, rand.Intn(maxLen-minLen)+minLen)
		totalStrLen += len(strList[i])
	}
	fmt.Printf("Total string length: %d\n", totalStrLen)

	// Write them all.
	for _, s := range strList {
		err := fc.Write([]byte(s))
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}
	if !assert.NoError(t, fc.Flush()) {
		t.FailNow()
	}

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
	testFileChannelWithRandomStrings(t, rand.New(rand.NewSource(10)), 0, 512, 1<<20, 1)
}

func TestFileChannelWithRandomStrings_Parallel(t *testing.T) {
	testFileChannelWithRandomStrings(t, rand.New(rand.NewSource(10)), 0, 512, 1<<20, 4)
}

func benchmarkFileChannelWrite(b *testing.B, size int) {
	fc := setup(b, fmt.Sprintf("file_channel_benchmark_write_%d", size))
	defer teardown(b, fc, false)

	// Run benchmark.
	b.ResetTimer()
	payload := make([]byte, size)
	for n := 0; n < b.N; n++ {
		err := fc.Write(payload)
		if !assert.NoError(b, err) {
			b.FailNow()
		}
	}
	b.StopTimer()
}

func benchmarkFileChannelRead(b *testing.B, size int) {
	fc := setup(b, fmt.Sprintf("file_channel_benchmark_read_%d", size))
	defer teardown(b, fc, false)

	// Setup data.
	payload := magicPayload(size)
	for i := 0; i < b.N; i++ {
		err := fc.Write(payload)
		if !assert.NoError(b, err) {
			b.FailNow()
		}
	}
	if !assert.NoError(b, fc.Flush()) {
		b.FailNow()
	}

	// Create an iterator.
	it := fc.Iterator()

	// Run benchmark.
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := it.Next(context.Background())

		if !assert.NoError(b, err) {
			b.FailNow()
		}
	}
	b.StopTimer()

	// Close it.
	err := it.Close()
	assert.NoError(b, err)
}

func BenchmarkFileChannel_Write_16(b *testing.B) {
	benchmarkFileChannelWrite(b, 16)
}

func BenchmarkFileChannel_Write_64(b *testing.B) {
	benchmarkFileChannelWrite(b, 64)
}

func BenchmarkFileChannel_Write_512(b *testing.B) {
	benchmarkFileChannelWrite(b, 512)
}

func BenchmarkFileChannel_Read_16(b *testing.B) {
	benchmarkFileChannelRead(b, 16)
}

func BenchmarkFileChannel_Read_64(b *testing.B) {
	benchmarkFileChannelRead(b, 64)
}

func BenchmarkFileChannel_Read_512(b *testing.B) {
	benchmarkFileChannelRead(b, 512)
}
