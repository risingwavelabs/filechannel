// Copyright 2024 RisingWave Labs
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
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileChannel_Stats(t *testing.T) {
	tmpDir := mkdirTemp(t)
	defer os.RemoveAll(tmpDir)

	fch, err := OpenFileChannel(tmpDir)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer fch.Close()

	// Assert disk usage == 64 (header size).
	usage, err := fch.DiskUsage()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	assert.Equal(t, uint64(64), usage)

	msg := []byte("Hello world!")

	tx := fch.Tx()
	defer tx.Close()

	// Assert sender offset == 0.
	assert.Equal(t, uint64(0), tx.WriteOffset())

	err = tx.Send(context.Background(), msg)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	// Assert sender offset != 0.
	assert.NotEqual(t, uint64(0), tx.WriteOffset())

	rx := fch.Rx()
	defer rx.Close()

	// Assert reader offset == math.MaxUint64.
	assert.Equal(t, uint64(math.MaxUint64), rx.ReadOffset())

	p, err := rx.Recv(context.Background())
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.Equal(t, msg, p) {
		t.FailNow()
	}

	// Assert reader offset != 0.
	assert.NotEqual(t, uint64(0), rx.ReadOffset())

	// Assert reader offset == sender offset.
	assert.Equal(t, tx.WriteOffset(), rx.ReadOffset())

	// Recv happened means the file channel has flushed.
	// Now we can examine the disk usage and flush offset.

	// Assert flush offset != 0.
	assert.NotEqual(t, uint64(0), fch.FlushOffset())

	// Assert disk usage > 64.
	usage, err = fch.DiskUsage()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	assert.NotEqual(t, uint64(64), usage)
}
