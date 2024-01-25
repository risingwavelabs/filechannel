# Design of File Channel

In general, file channel is a file based persistent channel.

## Interface

```go
package filechannel

import "context"

type Sender interface {
	Send(context.Context, []byte) error
	Close() error
}

type Receiver interface {
	Recv(context.Context) ([]byte, error)
	Close() error
}

type FileChannel interface {
	Tx() Sender
	Rx() Receiver
	Close() error
}
```

Developer can create `Sender` and `Receiver` from a `FileChannel` to send and receive messages in bytes through the 
channel. The `Send` and `Recv` methods both accept a `Context` to help on the cancellation. Once `Context` is done,
the methods will return immediately with the error from the `Context`. The operations shall be transactional: either 
succeed and update the channel, or fail and leave the channel untouched.

## On-disk Data Structure

### Message and Channel

Structure of a message:

```plain
|---- Length ----|---- CRC32 ----|---- Payload ----|
|---- uint32 ----|---- uint32 ---|---- []byte  ----|
|----   4    ----|----   4   ----|---- length  ----|
```

Structure of a channel:

```plain
| message 1 | message 2 |   ...   | message N |
```

## Operations

The following design doesn't take concurrency into consideration.

### Send (Write)

Sending a message to channel simply appends it and its header to the disk. Since the writing process must be 
transactional, `Context` won't be effective anymore once it starts to append. Furthermore, sending fails on any failure 
returned by the IO operations and will prevent any further sending from happening. Thus, IO error such as disk full 
basically locks a channel and make it read-only. 

### Recv (Read)

Similar to sending, receiving from a channel reads message frame from disk. It validates the checksum before returning 
the raw message to the invoker. IO errors and validation errors indicates the channel might be broken. When implementing
a `Receiver`, no further `Recv` can succeed after a preceding error.

`Recv` must wait until there is message left to read. That means there should be some sort of synchronization between 
sending and receiving so that a waiting reader can be notified when a message is written. Such synchronization can be 
easily implemented with a conditional variable in other languages. However, the Go's `sync.Cond` doesn't provide a way 
to interact with `Context`. Luckily there's a [workaround](internal/condvar/condvar.go).

## Concurrency

Limited by the data structure, there's no way except locking to achieve concurrency on a single `Sender` and `Receiver`.

## Persistency & Crash consistency

Data must be written to the OS before a `Send` operation finishes. However, there's no guarantee that the OS will 
persist to the underlying disk immediately. That basically means

- During process restarts, there won't be any data loss.
- During machine crashes, there might be data loss at the end of the channel.

File channel should be able to handle both cases. It's easy to read through the channel and find the right margin where 
the next message is broken, or there's no message anymore. A file channel should trim the broken messages. 

## Optimization

### File Segments

While a large single file works, it's non-trivial to perform GC and partial compression on it. Segmenting the whole 
channel into files is a viable solution.

```plain
segment.0    # read-only, offset [0, 1000)
segment.1    # read-only, offset [1000, 2001)
segment.2    # read-only, offset [2001, 3003)
segment.3    # read-write, offset [3003,)
```

The last file is the one where new data appended. Preceding files are all read-only. Sequence of files in order formed 
the whole space of the channel. Therefore, GC could be segment based. Once a segment is entirely within the GC interval,
then the GC worker could remove it.

Each file should have a header recording the following information:

```plain
|----   uint32   ----|----    uint64    ----|
|---- segment id ----|---- begin offset ----|
```

The ending offset isn't record in order to keep the file append-only. Once a segment is decided to be cut, the writing 
file is sealed and closed. A new file with greater segment id will be created.

#### Compression on Segments

This is a follow-up optimization on the segment files. Once a file is sealed to be read-only, we could compress it to 
save disk space. The process could be as following:

1. For each read-only segment file,
2. Check if it's still valid. If true, then
3. Compress it with a temporary name.
4. When finishes, check again if it's still valid.
5. If true, hold the read lock and rename the file atomically.
    1. After that, release the lock and delete the original file.
    2. Deletion shouldn't affect the fds that opened on that file if we open it with carefully chosen flags. (Linux/macOS/Windows)
6. Otherwise, delete the temporary file.

The compressed file has the same name as the uncompressed ones, except an additional suffix `.z`.

Compressed file could have a different file header to contain more information:

```plain
|----    uint32  ----|----    uint64    ----|----    uint64  ----|----        uint8       ----|
|---- segment id ----|---- begin offset ----|---- end offset ----|---- compression method ----|
```

A reader that opens a segment should try the compressed file first and then the original one.

### Buffering

IO operation per write is known as inefficient due to system calls, especially when the data being written is small.
A common solution to this problem is buffering in the user space. However, buffering breaks the premise that once a 
message is sent, it could be read from the file. To address the problem, we should either synchronize read and write 
on the buffer carefully, or delay the read until the buffer is flushed. We chose the latter. There will be a background
thread flushes the data periodically and notifies the reader. 

Buffering for reading is the same and simpler. It can be done by leveraging the builtin buffered reader.

### Fan-out Receiver

Fan-out receiver is quite straight-forward. However, the first message to receive is undetermined and depends on 
implementation. Fan-out receivers won't affect each other in any circumstances.

### File Descriptor per Reader/Writer

Like the problem in buffering, a single file descriptor for both read and write will require careful management on it.
For example, offset to read and write must be maintained separately, and closing must wait until all readers and writers
exit. Also, buffering is hard because it requires a complex way to do cache eviction. With one file descriptor per 
reader and writer, it will be much simpler as we delegate almost all things to the OS, and it already handles them well.

### Acknowledgable Receiver

Sometimes, it requires the channel to prevent the messages from being deleted to perform some sort of retrying during
restarts. For example, a server would like to send all local messages to remote, and it should guarantee all messages 
are delivered in order. If a delivery fails, it can retry on it. But since the message is read from the channel, it 
could be deleted from the disk in theory. However, if the process crashes before succeeding in delivery, the message 
could be lost.

In such cases, we would like to manually acknowledge that the message is successfully consumed so that it's deletable.

The interface looks like

```go
package filechannel

import "context"

type Receiver interface {
	Recv(context.Context) ([]byte, error)
	Close() error
}

type AckReceiver interface {
	Receiver

	Ack(n int) error
}
```

`Ack` simply notifies the underlying the file channel that the message is fully consumed. However, when to actually 
delete it is left to implementation. 

## Trade-offs

The file channel current only guarantees:

1. Messages are received in the same order of sending.
2. Messages can be received in a bound period after the sending, but there's no promise of how long.
3. Messages will be finally persistent.
4. Messages unread by any receiver (if any) won't be GC-ed.
5. Messages unread by some receiver won't be GC-ed.

It doesn't guarantee that

1. A message will be persistent immediately once the sending returns.
2. A message will be persistent during system crashes (e.g., power loss).
3. A message read in the previous process can be still found in the channel.
4. A message read in the previous process will be skipped.
5. A message read by a fan-out receiver can be read by another fan-out receiver because of GC.
6. A message is GCed once it's read by all receivers.

## Unresolved Problems

1. Support receiving from a specific point to avoid redundant messages upon restart.