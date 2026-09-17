// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filechannel

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReceiveOwnsResult(t *testing.T) {
	for _, manualAck := range []bool{false, true} {
		for _, method := range []string{"Recv", "TryRecv", "Iterator"} {
			t.Run(fmt.Sprintf("%s/manualAck=%t", method, manualAck), func(t *testing.T) {
				fc, err := openFileChannel(t.TempDir(), FlushInterval(time.Hour))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, fc.Close()) })
				tx := fc.Tx()
				t.Cleanup(func() { require.NoError(t, tx.Close()) })
				var rx Receiver
				if manualAck {
					rx = fc.RxAck()
				} else {
					rx = fc.Rx()
				}
				t.Cleanup(func() { require.NoError(t, rx.Close()) })
				messages := [][]byte{[]byte("first"), []byte("other"), {}, bytes.Repeat([]byte("x"), (1<<20)+1), []byte("last")}
				for _, msg := range messages {
					require.NoError(t, tx.Send(context.Background(), msg))
				}
				require.NoError(t, fc.flush())
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var received [][]byte
				if method == "Iterator" {
					for msg, err := range NewIteratorForReceiver(ctx, rx) {
						require.NoError(t, err)
						received = append(received, msg)
						if len(received) == len(messages) {
							break
						}
					}
				} else {
					for range messages {
						var msg []byte
						if method == "Recv" {
							msg, err = rx.Recv(ctx)
						} else {
							msg, err = rx.TryRecv()
						}
						require.NoError(t, err)
						received = append(received, msg)
					}
				}
				if manualAck {
					require.NoError(t, rx.(AckReceiver).Ack(len(messages)))
				}
				for i, want := range messages {
					require.Equal(t, string(want), string(received[i]))
				}
				received[0][0] = 'F'
				require.Equal(t, "other", string(received[1]))
				msg, err := rx.TryRecv()
				require.ErrorIs(t, err, ErrNotEnoughMessages)
				require.Nil(t, msg)
			})
		}
	}
}

func TestBorrowedReceive(t *testing.T) {
	for _, manualAck := range []bool{false, true} {
		t.Run(fmt.Sprintf("manualAck=%t", manualAck), func(t *testing.T) {
			fc, err := openFileChannel(t.TempDir(), FlushInterval(time.Hour))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, fc.Close()) })
			tx := fc.Tx()
			t.Cleanup(func() { require.NoError(t, tx.Close()) })
			var rx Receiver
			if manualAck {
				rx = fc.RxAck()
			} else {
				rx = fc.Rx()
			}
			t.Cleanup(func() { require.NoError(t, rx.Close()) })
			borrowed := rx.(BorrowingReceiver)
			for _, msg := range []string{"owned", "first", "other", "final"} {
				require.NoError(t, tx.Send(context.Background(), []byte(msg)))
			}
			require.NoError(t, fc.flush())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			owned, err := rx.Recv(ctx)
			require.NoError(t, err)
			first, err := borrowed.RecvBorrowed(ctx)
			require.NoError(t, err)
			require.Equal(t, "first", string(first))
			if manualAck {
				require.NoError(t, rx.(AckReceiver).Ack(2))
				require.Equal(t, "first", string(first))
			}
			// Keep only the address to verify buffer reuse without reading an expired slice.
			address := &first[0]
			other, err := borrowed.TryRecvBorrowed()
			require.NoError(t, err)
			require.Equal(t, "other", string(other))
			require.True(t, address == &other[0], "borrowed receives should reuse the buffer")
			last, err := rx.TryRecv()
			require.NoError(t, err)
			require.Equal(t, "final", string(last))
			require.Equal(t, "owned", string(owned))
			if manualAck {
				require.NoError(t, rx.(AckReceiver).Ack(2))
			}
			msg, err := borrowed.TryRecvBorrowed()
			require.ErrorIs(t, err, ErrNotEnoughMessages)
			require.Nil(t, msg)
			canceled, cancelNow := context.WithCancel(context.Background())
			cancelNow()
			msg, err = borrowed.RecvBorrowed(canceled)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, msg)
		})
	}
}
