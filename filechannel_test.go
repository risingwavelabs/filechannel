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

func mkdirTemp(t *testing.T) string {
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
