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
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

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
