package fs

import (
	"bufio"
	"errors"
	"io"
	"os"
)

var _ io.ReadCloser = &SequentialFile{}

// SequentialFile is a wrapper of the [os.File] to provide sequential
// read only file access.
type SequentialFile struct {
	read int64
	f    *os.File
	r    *bufio.Reader
}

// OpenSequentialFile opens a sequential file.
func OpenSequentialFile(name string) (*SequentialFile, error) {
	f, err := OpenFile(name, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &SequentialFile{
		read: 0,
		f:    f,
		r:    bufio.NewReader(f),
	}, nil
}

// Read implements io.Reader.
func (f *SequentialFile) Read(p []byte) (n int, err error) {
	n, err = f.r.Read(p)
	f.read += int64(n)
	return
}

// Close implements io.Closer.
func (f *SequentialFile) Close() error {
	f.r = nil
	err := f.f.Close()
	f.f = nil
	return err
}

var _ io.WriteCloser = &AppendableFile{}

// AppendableFile is a wrapper of the [os.File] to provide appendable
// only file access.
type AppendableFile struct {
	written int64
	f       *os.File
	w       *bufio.Writer
}

// OpenAppendableFile opens an appendable file.
func OpenAppendableFile(name string, perm os.FileMode) (*AppendableFile, error) {
	f, err := OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, perm)
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
func (f *AppendableFile) Written() int64 {
	return f.written
}

// Write implements io.Writer.
func (f *AppendableFile) Write(b []byte) (int, error) {
	n, err := f.w.Write(b)
	f.written += int64(n)
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

// ReadFrom implements io.ReaderFrom.
func (f *AppendableFile) ReadFrom(r io.Reader) (n int64, err error) {
	err = f.Flush()
	if err != nil {
		return 0, err
	}

	n, err = f.f.ReadFrom(r)
	f.written += n
	return
}

// Close implements io.Closer.
func (f *AppendableFile) Close() error {
	err1 := f.Flush()
	err2 := f.f.Close()
	f.w, f.f = nil, nil
	return errors.Join(err1, err2)
}
