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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkFileChannelWrite(b *testing.B) {
	tmpDir := mkdirTemp(b)
	defer os.RemoveAll(tmpDir)

	fch, err := OpenFileChannel(tmpDir)
	if !assert.NoError(b, err) {
		b.FailNow()
	}
	defer fch.Close()

	msg := []byte("Hello world! Salut le Monde! Hallo Welt!")

	tx := fch.Tx()
	defer tx.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = tx.Send(context.Background(), msg)
		if !assert.NoError(b, err) {
			b.FailNow()
		}
	}
}

func BenchmarkFileChannelRead(b *testing.B) {
	tmpDir := mkdirTemp(b)
	defer os.RemoveAll(tmpDir)

	fch, err := OpenFileChannel(tmpDir)
	if !assert.NoError(b, err) {
		b.FailNow()
	}
	defer fch.Close()

	msg := []byte("Hello world! Salut le Monde! Hallo Welt!")

	tx := fch.Tx()
	defer tx.Close()

	for i := 0; i < b.N; i++ {
		err = tx.Send(context.Background(), msg)
		if !assert.NoError(b, err) {
			b.FailNow()
		}
	}

	b.ResetTimer()
	rx := fch.Rx()
	defer rx.Close()
	for i := 0; i < b.N; i++ {
		_, err := rx.Recv(context.Background())
		if !assert.NoError(b, err) {
			b.FailNow()
		}
	}
}
