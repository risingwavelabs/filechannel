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

package condvar

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Cond is a conditional variable implementation that uses channels for
// notifications. Only supports .Broadcast() method, however supports
// timeout based Wait() calls unlike regular sync.Cond.
type Cond struct {
	L sync.Locker
	n unsafe.Pointer
}

// NewCond creates a new [Cond] with a [sync.Locker].
func NewCond(l sync.Locker) *Cond {
	c := &Cond{L: l}
	n := make(chan struct{})
	c.n = unsafe.Pointer(&n)
	return c
}

func (c *Cond) notifyChan() <-chan struct{} {
	ptr := atomic.LoadPointer(&c.n)
	return *((*chan struct{})(ptr))
}

// Wait behaves just like [sync.Cond.Wait] and will return unless awaken
// by Broadcast.
func (c *Cond) Wait() {
	n := c.notifyChan()
	c.L.Unlock()
	<-n
	c.L.Lock()
}

// WaitTimer waits for Broadcast calls. When it waits successfully, it
// behaves the same as Wait and returns nil. However, when the timer expired
// it won't lock again but returns [context.DeadlineExceeded].
func (c *Cond) WaitTimer(t *time.Timer) error {
	n := c.notifyChan()
	c.L.Unlock()
	select {
	case <-n:
	case <-t.C:
		return context.DeadlineExceeded
	}
	c.L.Lock()
	return nil
}

// WaitContext waits for Broadcast calls. When it waits successfully, it
// behaves the same as Wait and returns nil. However, when the context
// is done, it won't lock again but returns the error from ctx.Err().
func (c *Cond) WaitContext(ctx context.Context) error {
	n := c.notifyChan()
	c.L.Unlock()
	select {
	case <-n:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.L.Lock()
	return nil
}

// Broadcast call notifies everyone that something has changed.
func (c *Cond) Broadcast() {
	n := make(chan struct{})
	ptrOld := atomic.SwapPointer(&c.n, unsafe.Pointer(&n))
	close(*(*chan struct{})(ptrOld))
}
