package filechannel

import (
	"context"
)

// FileChannel is the interface defines how a file channel behaves.
// It should support multiple senders and multiple receivers working
// concurrently.
type FileChannel[T any] interface {
	// Sender creates a [Sender] and allows sending messages through it.
	// Messages successfully sent should be persisted to disk or other
	// persistent materials. The sender is free to apply some batching
	// or buffering techniques to improve the performance as well as
	// reduce the impact on the underlying disk or systems. But it must
	// at least ensure the integrity of the messages persisted.
	Sender() Sender[T]

	// Receiver creates a [Receiver] that allows receiving messages from
	// it. Multiple receivers should be able work concurrently and
	// consume from the same channel, i.e., each message must be consumed
	// exactly once. The receiving process should be blocked while there's
	// nothing to consume in the queue. However, it should allow quitting
	// when the Context is timeouted or cancelled.
	Receiver() Receiver[T]

	// Acknowledgable tells the caller that the file channel is acknowledgable,
	// which means that the messages received from receivers should be acked
	// manually so as to tell the file channel to remove it from the channel.
	// Otherwise, the message can be consumed agained when the file channel
	// is reopened afterwards.
	Acknowledgable() bool

	// Close closes the file channel and release any resources attached. Sending
	// to a closed file channel shall panic and receiving from a closed file
	// channel still works and will receive empty messages when the file channel
	// is fully consumed.
	Close() error
}

// FanOutFileChannel is the interface defines how a fan-out file channel
// behaves. It supports creating fan-out receivers besides the functionality
// provided in [FileChannel].
type FanOutFileChannel[T any] interface {
	FileChannel[T]

	// FanOutReceiver creates a [FanOutReceiver] that allows receiving messages
	// from the file channel. Multiple receivers can consume from different
	// positions of the same channel simultaneously. The messages that have
	// been consumed by all the fan-out receivers could be purged from the
	// file channel depending on the implementation. The starting position
	// of the fan-out receiver is decided by the implementation.
	//
	// A [FanOutReceiver] must be closed to release the position occupied and
	// let the file channel purge those have been consumed.
	FanOutReceiver() FanOutReceiver[T]
}

// Sender is the interface defines how a sender works. For more
// requirements on the Sender, see the [FileChannel.Sender] method
// in [FileChannel] instead.
type Sender[T any] interface {
	// Send a message to the file channel. The error will be returned
	// either when serialization fails or writing fails. If [context.Context]
	// is done during the process, the error from [context.Context.Err] will
	// be returned. The Sender can ignore the Context depending on
	// the implementation. But it should prevent incompleted intermediate
	// result from being persisted.
	Send(context.Context, *T) error
}

// Receiver is the interface defines how a receiver works. For more
// requirements on the Receiver, see the Receiver method in [FileChannel]
// instead.
type Receiver[T any] interface {
	// Receive a message from the file channel. The error will be
	// returned either when reading fails or deserialization fails.
	// If [context.Context] is done during the process, the error from
	// [context.Context.Err] will be returned.
	Receive(context.Context, *T) error
}

// FanOutReceiver is the interface defines how a fan-out receiver works.
// For more requirements on the FanOutReceiver, see the
// [FanOutFileChannel.FanOutReceiver] method in [FanOutFileChannel] instead.
type FanOutReceiver[T any] interface {
	Receiver[T]

	// Close releases the occupied resources such as position in the file
	// channel to release any constraints. It returns an error when failure
	// happens. Recommendation:
	//
	//   func ConsumeForever(c FanOutFileChannel[Object]) {
	//       rx := c.FanOutReceiver()
	//       defer rx.Close()
	//
	//       for {
	//           obj, err := rx.Receive(context.Background())
	//           if err != nil {
	//               fmt.Println(err)
	//               return
	//           }
	//           Handle(obj)
	//       }
	//   }
	Close() error
}

// Acknowledgable is the interface defines how ack works.
type Acknowledgable interface {
	// Ack acks with the underlying system that the first received
	// N messages could be treated as consumed.
	Ack(context.Context, int) error
}

// AcknowledgableReceiver is the interface defines how an acknowledgable
// file channel receiver works.
type AcknowledgableReceiver[T any] interface {
	Receiver[T]

	Acknowledgable
}

// AcknowledgableFanOutReceiver is the interface defines how an acknowledgable
// file channel fan-out receiver works.
type AcknowledgableFanOutReceiver[T any] interface {
	FanOutReceiver[T]

	Acknowledgable
}
