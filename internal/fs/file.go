package fs

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/klauspost/compress/snappy"
)

// Compiler fence.
var _ io.ReadCloser = &SequentialFile{}

// SequentialFile is a wrapper of the [os.File] to provide sequential
// read-only file access.
type SequentialFile struct {
	f *os.File
	r io.Reader
}

// OpenSequentialFile opens the file in the specified name in sequential
// read-only mode. If compressed is true, then open the file with a snappy
// decoder.
func OpenSequentialFile(name string, compressed bool) (*SequentialFile, error) {
	f, err := Open(name)
	if err != nil {
		return nil, err
	}
	var r io.Reader
	r = bufio.NewReader(f)
	if compressed {
		r = snappy.NewReader(r)
	}
	return &SequentialFile{f: f, r: r}, nil
}

// Read implements [io.Reader].
func (f *SequentialFile) Read(p []byte) (int, error) {
	return f.r.Read(p)
}

// Close implements [io.Closer].
func (f *SequentialFile) Close() error {
	return f.f.Close()
}

var _ io.WriteCloser = &AppendableFile{}

// AppendableFile is a wrapper of the [os.File] to provide appendable
// only file access.
type AppendableFile struct {
	f     *os.File
	w     io.Writer
	flush func() error
}

func OpenAppendableFile(name string, perm os.FileMode, compressed bool) (*AppendableFile, error) {
	f, err := OpenFile(name, os.O_CREATE|os.O_APPEND, perm)
	if err != nil {
		return nil, err
	}
	var w io.Writer
	var flush func() error
	if compressed {
		wr := snappy.NewBufferedWriter(f)
		w, flush = wr, wr.Flush
	} else {
		wr := bufio.NewWriter(w)
		w, flush = wr, wr.Flush
	}
	return &AppendableFile{
		f:     f,
		w:     w,
		flush: flush,
	}, nil
}

// Write implements io.Writer.
func (f *AppendableFile) Write(b []byte) (int, error) {
	return f.w.Write(b)
}

// Flush flushes the buffered data into OS.
func (f *AppendableFile) Flush() error {
	return f.flush()
}

// Sync syncs the changes to the underlying disk. It will
// flush first and then sync.
func (f *AppendableFile) Sync() error {
	err := f.flush()
	if err != nil {
		return err
	}
	return f.f.Sync()
}

// Close implements io.Closer.
func (f *AppendableFile) Close() error {
	err1 := f.flush()
	err2 := f.f.Close()
	return errors.Join(err1, err2)
}
