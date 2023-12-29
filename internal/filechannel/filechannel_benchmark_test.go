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
	"testing"

	"github.com/stretchr/testify/assert"
)

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
