package filechannel

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"
)

func TestRegressionUnexpectedOverwritten(t *testing.T) {
	tmpDir, err := os.MkdirTemp("/tmp", "file_channel_regression")
	if err != nil {
		t.Fatalf("failed to create temp dir: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	// Open a file channel that doesn't flush automatically. The default file write buffer
	// is 4096 bytes. When the first 4096 bytes are written, they will be flushed to the disk
	// but left the other bytes in the buffer. In the previous implementation, the file channel
	// wrongly recovers and leaves a corrupted channel.
	fc, err := OpenFileChannel(tmpDir, FlushInterval(24*time.Hour))
	if err != nil {
		t.Fatalf("failed to open file channel: %s", err)
	}

	// Write 4096 bytes, and it will be broken since there's a 8 bytes message header and exceeds the
	// write buffer. Then the write buffer will be flushed to the disk, and left 8 trailing bytes in it.
	err = fc.Write(magicPayload(4096))
	if err != nil {
		t.Fatalf("failed to write payload: %s", err)
	}

	// NOTE: don't close. Otherwise, there will be a flush. There is to simulate a sudden crash.
	err = fc.unlock()
	if err != nil {
		t.Fatalf("failed to unlock file channel: %s", err)
	}

	stat, err := os.Stat(path.Join(tmpDir, fmt.Sprintf("segment.%d", 0)))
	if err != nil {
		t.Fatalf("failed to check file stat: %s", err)
	}

	if stat.Size() != 4096+64 {
		t.Fatalf("segment file size isn't as expected, size now is %d", stat.Size())
	}

	// Now opens it again, the file size should have been correctly truncated.
	func() {
		fc, err := OpenFileChannel(tmpDir, FlushInterval(24*time.Hour))
		if err != nil {
			t.Fatalf("failed to open file channel: %s", err)
		}
		defer fc.Close()

		stat, err := os.Stat(path.Join(tmpDir, fmt.Sprintf("segment.%d", 0)))
		if err != nil {
			t.Fatalf("failed to check file stat: %s", err)
		}

		if stat.Size() != SegmentHeaderBinarySize {
			t.Fatalf("segment file size wasn't correctly truncated, size now is %d", stat.Size())
		}
	}()

	// Keep fc alive.
	_ = fc.WriteOffset()
}
