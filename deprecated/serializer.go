package filechannel

import (
	"bytes"
	"io"
)

// Serializable is a generic interface that defines the common methods used
// to serialize object into bytes and deserialize object from bytes.
type Serializer[T any] interface {
	// Write serializes the object into bytes and writes to the writer.
	Write(io.Writer, *T) (int, error)

	// Read reads from the bytes and deserializes the object into the object.
	// The third parameter specifies the number of bytes to read if it is
	// provided as a positive number.
	Read(io.Reader, *T, int) (int, error)
}

// No-op serializer for all generic types. Used for tests and validations only.
type noOpSerializer[T any] struct{}

// Write implements Serializer for noOpSerializer.
func (s noOpSerializer[T]) Write(_ io.Writer, _ *T) (int, error) {
	return 0, nil
}

// Read implements Serializer for noOpSerializer.
func (s noOpSerializer[T]) Read(r io.Reader, _ *T, n int) (int, error) {
	if n > 0 {
		nr, err := io.CopyN(io.Discard, r, int64(n))
		return int(nr), err
	}
	return 0, nil
}

// CopySerializer implements a naive serializer for types that are
// identical to []byte.
type CopySerializer[T ~[]byte] struct {
	ReadIntoIfPossible bool
}

// Write implements Serializer for CopySerializer.
func (s *CopySerializer[T]) Write(w io.Writer, b *T) (int, error) {
	return w.Write([]byte(*b))
}

// Read implements Serializer for CopySerializer.
func (s *CopySerializer[T]) Read(r io.Reader, b *T, size int) (int, error) {
	if size < 0 {
		buf := bytes.NewBuffer(nil)
		n, err := io.Copy(buf, r)
		if n > 0 {
			if s.ReadIntoIfPossible && b != nil && len(*b) >= buf.Len() {
				_ = copy(*b, buf.Bytes())
			} else {
				*b = buf.Bytes()
			}
		}
		return int(n), err
	} else {
		var in []byte
		if !s.ReadIntoIfPossible || b == nil || len(*b) < size {
			in = T(make([]byte, size))
			*b = in
		} else {
			in = (*b)[:size]
		}
		return r.Read(in)
	}
}
