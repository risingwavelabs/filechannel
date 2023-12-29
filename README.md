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