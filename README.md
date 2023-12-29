# File-based Persistent Channel

## Example

```go
package example

import (
	"context"

	"github.com/risingwavelabs/filechannel"
)

func Example(dir string) error {
	fch, err := filechannel.OpenFileChannel(dir)
	if err != nil {
		return err
	}
	defer fch.Close()

	msg := []byte("Hello world!")

	tx := fch.Tx()
	err = tx.Send(context.Background(), msg)
	if err != nil {
		return err
	}

	rx := fch.Rx()
	defer rx.Close()
	p, err := rx.Recv(context.Background())
	if err != nil {
		return err
	}

	return nil
}

```

## Benchmarks

Check benchmarks here [internal/filechannel/filechannel_benchmark_test.go](internal/filechannel/filechannel_benchmark_test.go).

```text
Hardware: Macbook Pro
OS: macOS
CPU: M1 Pro
GOMAXPROCS: 4

pkg: github.com/risingwavelabs/filechannel/internal/filechannel
BenchmarkFileChannel_Write_16
BenchmarkFileChannel_Write_16-4           	18254582	       63.36 ns/op	       8 B/op	       1 allocs/op
BenchmarkFileChannel_Write_64
BenchmarkFileChannel_Write_64-4           	 8664692	       140.5 ns/op	       8 B/op	       1 allocs/op
BenchmarkFileChannel_Write_512
BenchmarkFileChannel_Write_512-4          	 1427677	       817.6 ns/op	       9 B/op	       1 allocs/op
BenchmarkFileChannel_Read_16
BenchmarkFileChannel_Read_16-4            	 9050712	       132.7 ns/op	      56 B/op	       2 allocs/op
BenchmarkFileChannel_Read_64
BenchmarkFileChannel_Read_64-4            	 8260588	       146.0 ns/op	      56 B/op	       2 allocs/op
BenchmarkFileChannel_Read_512
BenchmarkFileChannel_Read_512-4           	 3973660	       303.7 ns/op	      56 B/op	       2 allocs/op
BenchmarkFileChannel_ReadWrite_16
BenchmarkFileChannel_ReadWrite_16-4       	 6157968	       197.0 ns/op	      64 B/op	       3 allocs/op
BenchmarkFileChannel_ReadWrite_64
BenchmarkFileChannel_ReadWrite_64-4       	 4252632	       281.8 ns/op	      64 B/op	       3 allocs/op
BenchmarkFileChannel_ReadWrite_512
BenchmarkFileChannel_ReadWrite_512-4      	  902235	        1116 ns/op	      65 B/op	       3 allocs/op
BenchmarkFileChannel_ParallelRead_16
BenchmarkFileChannel_ParallelRead_16-4    	29948120	       38.91 ns/op	      56 B/op	       2 allocs/op
BenchmarkFileChannel_ParallelRead_64
BenchmarkFileChannel_ParallelRead_64-4    	26518456	       42.85 ns/op	      56 B/op	       2 allocs/op
BenchmarkFileChannel_ParallelRead_512
BenchmarkFileChannel_ParallelRead_512-4   	27467155	       42.09 ns/op	      56 B/op	       2 allocs/op
```

## Memory Consumption

From test case [`TestFileChannel_MemoryConsumption`](https://github.com/risingwavelabs/filechannel/blob/02461081d05b48825484e0c9593dd982328bc92d/filechannel_test.go#L100).

```text
=========== BEFORE ALL ===========
Alloc: 240.3 KiB
TotalAlloc: 240.3 KiB
Sys: 7.4 MiB
Lookups: 0
Mallocs: 1024
Frees: 122
HeapAlloc: 240.3 KiB
HeapSys: 3.7 MiB
HeapIdle: 3.0 MiB
HeapInuse: 632.0 KiB
HeapReleased: 3.0 MiB
HeapObjects: 902

=========== AFTER OPEN ===========
Alloc: 249.5 KiB
TotalAlloc: 249.5 KiB
Sys: 7.4 MiB
Lookups: 0
Mallocs: 1101
Frees: 130
HeapAlloc: 249.5 KiB
HeapSys: 3.7 MiB
HeapIdle: 3.0 MiB
HeapInuse: 656.0 KiB
HeapReleased: 3.0 MiB
HeapObjects: 971

=========== AFTER SENDING ===========
Alloc: 2.5 MiB
TotalAlloc: 16.6 MiB
Sys: 14.3 MiB
Lookups: 0
Mallocs: 2104024
Frees: 1953265
HeapAlloc: 2.5 MiB
HeapSys: 7.6 MiB
HeapIdle: 4.5 MiB
HeapInuse: 3.0 MiB
HeapReleased: 2.9 MiB
HeapObjects: 150759

=========== AFTER RECEIVING ===========
Alloc: 2.5 MiB
TotalAlloc: 226.0 MiB
Sys: 14.3 MiB
Lookups: 0
Mallocs: 10514798
Frees: 10421026
HeapAlloc: 2.5 MiB
HeapSys: 7.6 MiB
HeapIdle: 4.5 MiB
HeapInuse: 3.0 MiB
HeapReleased: 2.9 MiB
HeapObjects: 93772

=========== AFTER ALL ===========
Alloc: 2.5 MiB
TotalAlloc: 226.0 MiB
Sys: 14.3 MiB
Lookups: 0
Mallocs: 10514861
Frees: 10421037
HeapAlloc: 2.5 MiB
HeapSys: 7.5 MiB
HeapIdle: 4.5 MiB
HeapInuse: 3.1 MiB
HeapReleased: 2.9 MiB
HeapObjects: 93824
```