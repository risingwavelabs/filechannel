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

package utils

import "io"

type CountWriter struct {
	n uint64
	w io.Writer
}

func (w *CountWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.n += uint64(n)
	return
}

func (w *CountWriter) Count() uint64 {
	return w.n
}

func NewCountWriter(w io.Writer) *CountWriter {
	return &CountWriter{
		w: w,
	}
}
