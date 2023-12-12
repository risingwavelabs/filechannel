package fs

import (
	"bufio"
	"errors"
	"io"
	"os"
)

var _ io.WriteCloser = &AppendableFile{}

// AppendableFile is a wrapper of the [os.File] to provide appendable
// only file access.
type AppendableFile struct {
	written int
	f       *os.File
	w       *bufio.Writer
}

// OpenAppendableFile opens an appendable file.
func OpenAppendableFile(name string, perm os.FileMode) (*AppendableFile, error) {
	f, err := OpenFile(name, os.O_CREATE|os.O_APPEND, perm)
	if err != nil {
		return nil, err
	}

	return &AppendableFile{
		f: f,
		w: bufio.NewWriter(f),
	}, nil
}

// Written returns the number of bytes written into the file. It counts the
// bytes that have been processed by Write and doesn't reflect the bytes that
// have been on disk.
func (f *AppendableFile) Written() int {
	return f.written
}

// Write implements io.Writer.
func (f *AppendableFile) Write(b []byte) (int, error) {
	n, err := f.w.Write(b)
	f.written += n
	return n, err
}

// Flush flushes the buffered data into OS.
func (f *AppendableFile) Flush() error {
	return f.w.Flush()
}

// Sync syncs the changes to the underlying disk. It will
// flush first and then sync.
func (f *AppendableFile) Sync() error {
	err := f.Flush()
	if err != nil {
		return err
	}
	return f.f.Sync()
}

// Close implements io.Closer.
func (f *AppendableFile) Close() error {
	err1 := f.Flush()
	err2 := f.f.Close()
	f.w, f.f = nil, nil
	return errors.Join(err1, err2)
}
