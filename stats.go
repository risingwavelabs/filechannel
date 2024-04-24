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

// ReceiverStats is the interface for getting the stats of a receiver.
type ReceiverStats interface {
	// ReadOffset returns the offset of the last read message.
	// Note that the offset is local to the receiver. It will only change
	// when the receiver reads a message.
	//
	// The initial offset is math.MaxUint64 to indicate that no message has
	// been read.
	ReadOffset() uint64
}

// SenderStats is the interface for getting the stats of a sender.
type SenderStats interface {
	// WriteOffset returns the offset of the last written message.
	// Note that the offset is not the byte offset in the file. It's the
	// offset of the message in the channel. The offset will change no matter
	// a message is written by the sender, or the other senders of the same
	// channel.
	WriteOffset() uint64
}

// Stats is the interface for getting the stats of a file channel.
type Stats interface {
	// DiskUsage returns the disk usage of the file channel.
	// Note that calling DiskUsage() is an expensive operation.
	DiskUsage() (uint64, error)

	// FlushOffset returns the offset of the last flushed message.
	// Messages with offset less than the flush offset are guaranteed to be
	// seen by the readers.
	FlushOffset() uint64
}
