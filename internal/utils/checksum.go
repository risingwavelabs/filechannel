package utils

import (
	"hash/crc32"
	"io"
)

type ChecksumWriter struct {
	checksum uint32
	w        io.Writer
}

func (w *ChecksumWriter) Checksum() uint32 {
	return w.checksum
}

func (w *ChecksumWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.checksum = crc32.Update(w.checksum, crc32.IEEETable, p[:n])
	return
}

func NewChecksumWriter(w io.Writer) *ChecksumWriter {
	return &ChecksumWriter{w: w}
}
