package utils

import "io"

type CountWriter struct {
	n uint64
	w io.Writer
}

func (w *CountWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.n += uint64(n)
	return
}

func (w *CountWriter) Count() uint64 {
	return w.n
}

func NewCountWriter(w io.Writer) *CountWriter {
	return &CountWriter{
		w: w,
	}
}
