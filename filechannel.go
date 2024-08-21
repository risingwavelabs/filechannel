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

	"github.com/risingwavelabs/filechannel/internal/filechannel"
)

// Errors.
var (
	ErrChecksumMismatch   = filechannel.ErrChecksumMismatch
	ErrChannelClosed      = filechannel.ErrChannelClosed
	ErrNotEnoughMessages  = filechannel.ErrNotEnoughMessages
	ErrNotEnoughReadToAck = filechannel.ErrNotEnoughReadToAck
)

// Sender sends bytes to file channel.
type Sender interface {
	SenderStats

	// Send bytes to file channel. Data will be finally persistent on disk.
	Send(context.Context, []byte) error

	// Close closes a sender.
	Close() error
}

// Receiver receives bytes from file channel in the sending order.
type Receiver interface {
	ReceiverStats

	// Recv bytes from file channel.
	Recv(context.Context) ([]byte, error)

	// TryRecv tries to receive bytes from file channel without blocking.
	// If there is no data available, it will return nil, ErrNotEnoughMessages.
	TryRecv() ([]byte, error)

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
	Stats

	// Tx creates a Sender. Sender is thread safe.
	// It's possible to have multiple senders at the same time.
	Tx() Sender

	// Rx creates a Receiver. Be careful that Receiver is non-thread safe.
	// However, it's possible to have multiple receivers at the same time.
	// The first message received by each receiver is undetermined and
	// leaved to implementation.
	Rx() Receiver

	// Close the channel. Unclosed senders will block the method.
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
var _ Stats = &fileChannel{}

type fileChannel struct {
	wRefLock sync.Mutex
	wRefCond *sync.Cond
	wRefCnt  int
	wLock    sync.Mutex
	inner    *filechannel.FileChannel
}

func (f *fileChannel) FlushOffset() uint64 {
	return f.inner.FlushOffset()
}

func (f *fileChannel) DiskUsage() (uint64, error) {
	return f.inner.DiskUsage()
}

func (f *fileChannel) Close() error {
	f.wRefLock.Lock()
	defer f.wRefLock.Unlock()
	for f.wRefCnt != 0 {
		f.wRefCond.Wait()
	}

	err := f.inner.Close()
	f.inner = nil
	return err
}

func (f *fileChannel) writeOffset() uint64 {
	return f.inner.WriteOffset()
}

func (f *fileChannel) send(bytes []byte) error {
	f.wLock.Lock()
	defer f.wLock.Unlock()

	return f.inner.Write(bytes)
}

// nolint: unused
func (f *fileChannel) flush() error {
	f.wLock.Lock()
	defer f.wLock.Unlock()

	return f.inner.Flush()
}

func (f *fileChannel) Tx() Sender {
	f.wRefLock.Lock()
	defer f.wRefLock.Unlock()
	f.wRefCnt++

	return &fileChannelSender{
		inner: f,
	}
}

func (f *fileChannel) closeTx() {
	f.wRefLock.Lock()
	defer f.wRefLock.Unlock()

	f.wRefCnt--
	f.wRefCond.Signal()
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
	f := &fileChannel{inner: inner}
	f.wRefCond = sync.NewCond(&f.wRefLock)
	return f, nil
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
var _ SenderStats = &fileChannelSender{}

type fileChannelSender struct {
	inner *fileChannel
}

func (s *fileChannelSender) WriteOffset() uint64 {
	return s.inner.writeOffset()
}

func (s *fileChannelSender) Close() error {
	if s.inner != nil {
		s.inner.closeTx()
		s.inner = nil
	}
	return nil
}

func (s *fileChannelSender) Send(ctx context.Context, p []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return s.inner.send(p)
}

// Compiler fence.
var _ Receiver = &fileChannelReceiver{}
var _ ReceiverStats = &fileChannelReceiver{}

type fileChannelReceiver struct {
	inner *filechannel.Iterator
}

func (r *fileChannelReceiver) TryRecv() ([]byte, error) {
	// nolint:staticcheck
	return r.inner.Next(nil)
}

func (r *fileChannelReceiver) ReadOffset() uint64 {
	return r.inner.Offset()
}

func (r *fileChannelReceiver) Close() error {
	return r.inner.Close()
}

func (r *fileChannelReceiver) Recv(ctx context.Context) ([]byte, error) {
	return r.inner.Next(ctx)
}

// Compiler fence.
var _ AckReceiver = &fileChannelAckReceiver{}
var _ ReceiverStats = &fileChannelAckReceiver{}

type fileChannelAckReceiver struct {
	fileChannelReceiver
}

func (r *fileChannelAckReceiver) Ack(n int) error {
	return r.inner.Ack(n)
}
