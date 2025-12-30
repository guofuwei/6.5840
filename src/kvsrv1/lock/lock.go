package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	uid string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.uid = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get("l")
		if err != rpc.OK {
			if err = lk.ck.Put("l", lk.uid, 0); err == rpc.OK {
				break
			} else if err == rpc.ErrMaybe {
				val, ver, err = lk.ck.Get("l")
				if err == rpc.OK && val == lk.uid {
					break
				}
			}
		} else if val == "" {
			if err = lk.ck.Put("l", lk.uid, ver); err == rpc.OK {
				break
			} else if err == rpc.ErrMaybe {
				val, ver, err = lk.ck.Get("l")
				if err == rpc.OK && val == lk.uid {
					break
				}
			}
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get("l")
		if err == rpc.OK && val == lk.uid {
			err := lk.ck.Put("l", "", ver)
			if err == rpc.OK {
				break
			} else if err == rpc.ErrMaybe {
				val, ver, err = lk.ck.Get("l")
				if err == rpc.OK && val == "" {
					break
				}
			}
		}
	}
}
