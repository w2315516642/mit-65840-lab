package lock

import (
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

func RandValue(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck     kvtest.IKVClerk
	name   string
	lockID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:     ck,
		name:   lockname,
		lockID: RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.name)

		// 没锁，尝试创建
		if err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.name, lk.lockID, 0)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrMaybe {
				nvalue, nversion, nerr := lk.ck.Get(lk.name)
				if nerr != rpc.OK {
					continue
				}
				// 创建成功
				if nvalue == lk.lockID && nversion == 1 {
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if err != rpc.OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 锁由别人持有
		if value == lk.lockID {
			return
		} else if value != "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 尝试抢锁
		for {
			err = lk.ck.Put(lk.name, lk.lockID, version)
			if err == rpc.ErrVersion {
				break
			} else if err == rpc.ErrMaybe {
				nvalue, nversion, nerr := lk.ck.Get(lk.name)
				if nerr == rpc.OK && nvalue == lk.lockID && nversion == version+1 {
					return
				}
			} else if err == rpc.OK {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			return
		}
		if value != lk.lockID {
			return
		}

		err = lk.ck.Put(lk.name, "", version)
		switch err {
		case rpc.OK:
			return
		case rpc.ErrVersion:
			{
				time.Sleep(100 * time.Millisecond)
			}
		case rpc.ErrMaybe:
			{
				value, _, err = lk.ck.Get(lk.name)
				if err != rpc.OK {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if value != lk.lockID {
					return
				}
			}
		}
	}
}
