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
	"sync"

	"github.com/arkbriar/filechannel/internal/filechannel"
)

// Sender sends bytes to file channel.
type Sender interface {
	// Send bytes to file channel. Data will be finally persistent on disk.
	Send(context.Context, []byte) error
}

// Receiver receives bytes from file channel in the sending order.
type Receiver interface {
	// Recv bytes from file channel.
	Recv(context.Context) ([]byte, error)

	// Close closes the reader.
	Close() error
}

// AckReceiver receives bytes like Receiver. However, it doesn't
// consume the data until a manual Ack is invoked. Consuming a data
// means telling the file channel that the data can be purged.
type AckReceiver interface {
	Receiver

	// Ack consumes the front unacknowledged messages.
	Ack(n int) error
}

// FileChannel is the interface for a file-based persistent channel.
type FileChannel interface {
	// Tx creates a Sender. Sender is thread safe.
	// It's possible to have multiple senders at the same time.
	Tx() Sender

	// Rx creates a Receiver. Be careful that Receiver is non-thread safe.
	// However, it's possible to have multiple receivers at the same time.
	// The first message received by each receiver is undetermined and
	// leaved to implementation.
	Rx() Receiver

	// Close the channel.
	Close() error
}

// AckFileChannel is the interface for a file-based persistent channel
// that supports asynchronous ack of received messages.
type AckFileChannel interface {
	FileChannel

	// RxAck creates a AckReceiver. AckReceiver behaves the same as
	// Receiver from [FileChannel.Rx] except the ack. Like Receiver,
	// there also can be multiple AckReceiver at the same time.
	RxAck() AckReceiver
}

// Option to create a FileChannel.
type Option = filechannel.Option

// Default option values and options.
var (
	DefaultRotateThreshold = filechannel.DefaultRotateThreshold
	DefaultFlushInterval   = filechannel.DefaultFlushInterval
	RotateThreshold        = filechannel.RotateThreshold
	FlushInterval          = filechannel.FlushInterval
)

// Compiler fence.
var _ AckFileChannel = &fileChannel{}

type fileChannel struct {
	wLock sync.Mutex
	inner *filechannel.FileChannel
}

func (f *fileChannel) Close() error {
	return f.inner.Close()
}

func (f *fileChannel) Tx() Sender {
	return &fileChannelSender{
		lock:  &f.wLock,
		inner: f.inner,
	}
}

func (f *fileChannel) Rx() Receiver {
	return &fileChannelReceiver{f.inner.Iterator()}
}

func (f *fileChannel) RxAck() AckReceiver {
	return &fileChannelAckReceiver{
		fileChannelReceiver{f.inner.IteratorAcknowledgable()},
	}
}

func openFileChannel(dir string, opts ...Option) (*fileChannel, error) {
	inner, err := filechannel.OpenFileChannel(dir, opts...)
	if err != nil {
		return nil, err
	}
	return &fileChannel{inner: inner}, nil
}

// OpenFileChannel opens a new FileChannel.
func OpenFileChannel(dir string, opts ...Option) (FileChannel, error) {
	return openFileChannel(dir, opts...)
}

// OpenAckFileChannel opens a new AckFileChannel.
func OpenAckFileChannel(dir string, opts ...Option) (AckFileChannel, error) {
	return openFileChannel(dir, opts...)
}

// Compiler fence.
var _ Sender = &fileChannelSender{}

type fileChannelSender struct {
	lock  sync.Locker
	inner *filechannel.FileChannel
}

func (s *fileChannelSender) Send(ctx context.Context, p []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	return s.inner.Write(p)
}

// Compiler fence.
var _ Receiver = &fileChannelReceiver{}

type fileChannelReceiver struct {
	inner *filechannel.Iterator
}

func (r *fileChannelReceiver) Close() error {
	return r.inner.Close()
}

func (r *fileChannelReceiver) Recv(ctx context.Context) ([]byte, error) {
	return r.inner.Next(ctx)
}

// Compiler fence.
var _ AckReceiver = &fileChannelAckReceiver{}

type fileChannelAckReceiver struct {
	fileChannelReceiver
}

func (r *fileChannelAckReceiver) Ack(n int) error {
	return r.inner.Ack(n)
}
