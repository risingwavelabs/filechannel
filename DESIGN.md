# Design

## Interface

File channel is a file based persistent channel for serializable messages. The interface looks like:

```go
type Sender[T any] interface {
	Send(context.Context, *T) error
}

type Receiver[T any] interface {
	Recv(context.Context, *T) error
}

type FileChannel[T any] interface {
	Tx() Sender[T]
	Rx() Receiver[T]
}
```

Before optimization, let's consider it as a SPSC (single producer single consumer) queue.


## On-disk Data Structure

Message:

```plain
|---- Length ----|---- CRC32 ----|---- Payload ----|
|---- uint32 ----|---- uint32 ---|---- []byte  ----|
|----   4    ----|----   4   ----|---- length  ----|
```

Overview:

```plain
| message 1 | message 2 |   ...   | message N |
```

## Sychronization between Read and Write

Offset is the global offset in the on-disk data. The type is `uint64`. 

```plain
read offset |                                               | write offset
            |                                               |
   GC-able  | message R | message R +1 |   ...   | message W|
```

Valid offset must be either
- zero,
- at the beginning of a message,
- at the end of a message.

Read operations must
- proceed immediately when `read offset < write offset`,
- wait when `read offset >= write offset`.

## Persistency during Restarts

Properties must hold:
- Write to the tail.
- Read from the head.

Read offset and write offset should be recorded somewhere on the disk. Let's say a separate file named `offset`.

Before read / write operations return:
- In memory (user space) data must be flushed to OS.
- Offsets must be written into the `offset` file.

Restart process:
1. Load read and write offsets.
2. Open the file and seek the offsets.
3. Run checks.
4. Construct other memory structures.

## Crash consistency

Crash consistency requires transactional operations like databases. A `fsync` must be invoded before the `Send` or `Recv` operation return. It will greatly downgrade the overall performance since the maximum IOPS of the underlying disk will be the bottleneck.

Crash consistency should be able to be disabled to suit the cases where we favor performance.

## Fan-out Receivers

```go
type FanOutReceiver[T any] interface {
    Receiver[T]
    
    ID() uint64
    Close() error
}

type FanOutFileChannel[T any] interface {
    Tx() Sender[T]
    Rx() FanOutReceiver[T]
}
```

Each fan-out receiver holds a read offset. GC-able regions are defined as the regions with offset less equal than the minimum of all the fan-out receivers' read offsets.

In terms of locks, It requires an overall synchronization to achieve the effect above. Besides, it's easy to extend a SPSC channel to one supports fan-out receivers as long as it provides control over the `read offset`.


```go
type spscChannel interface {
    SetReadOffset(uint64)
}

type fanoutChannel struct {
    mu          sync.Mutex
    readOffsets []uint64
    inner       spscChannel
}

// SetReadOffset is a callback function when a fan-out receiver advances.
func (c *fanoutChannel) SetReadOffset(i int, offset uint64) {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    // assert offset >= c.readOffsets[i]
    c.readOffsets[i] = offset
    
    minReadOffset := slices.Min(c.readOffsets)
    
    c.inner.SetReadOffset(minReadOffset)
}
```


## Optimization

### Acknowledgable

Acknowledgable means that one can peek the head (or even the elements after the head as long as they exist) without modifying the `read offset`. Meanwhile, the file channel will provide an `Ack` method in order to advance the `read offset` manually.

```go
type Acknowledgable interface {
    UnackCount() uint
    
    Ack(uint) error
}
```

It's also feasible when file channel supports control over the `read offset`. The receiver should maintain a local buffer to record the unacked offsets and set the underlying `read offset` when `Ack` is invoked.

### File as Segment

While a large single file works, it's non-trivial to perform GC and partial compression on it. Segmenting the whole channel into files is a viable solution. 

```plain
segment.0    # read-only, offset [0, 1000)
segment.1    # read-only, offset [1000, 2001)
segment.2    # read-only, offset [2001, 3003)
segment.3    # read-write, offset [3003,)
```

The last file is the one where new data appended. Preceding files are all read-only. Sequence of files in order formed the whole space of the channel. Therefore, GC could be segment (file) based. Once a segment is entirely within the GC interval, then the GC worker could remove it.

Each file should have a header recording the following information:

```plain
|----   uint32   ----|----    uint64    ----|
|---- segment id ----|---- begin offset ----|
```

The endding offset isn't record in order to keep the file append-only. Once a segment is decided to be cut, the writing file is sealed and closed. A new file with greater segment id is created.

Synchronization on the writing file is simply done in the following way:
- An fd is opened for write only to append the data.
- Extra fd will be opened on the writing file for read only. Then closing the fd for write won't affect reads.
- Synchronization of offsets will be down with lock and conditional variable. Simple and neat.


#### Compression on Segments

This is a follow-up optimization on the segment files. Once a file is sealed to be read-only, we could compress it to save disk space. The process could be as following:
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


### MPMC

Builtin `chan` and mutex could be a great helper to achieve MPMC.

For concurrent sending, we could just use a structure like the following:

```go
type message[T any] struct {
    ack chan error
    data *T
}

type Sender[T any] struct {
    ack chan error

    // shared channel from file channel
    ch chan message[T]
}

func (s *Sender[T]) Send(ctx context.Context, obj *T) error {
    s.ch <- message{ack: s.ack, data: obj}
    return <-s.ack
}
```

For concurrent reading, just use a Mutex to guard the receiver.
