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