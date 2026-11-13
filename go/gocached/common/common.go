package common

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// #############################################################################
// Current timestamp functionality
var unixTS int64 = 0

func init() {

	go func() {

		var __tmp int64 = 0
		var __tmp2 int64 = 0
		for {

			__tmp = time.Now().Unix()
			if __tmp != __tmp2 {
				__tmp2 = __tmp
				atomic.StoreInt64(&unixTS, __tmp)
			}

			time.Sleep(100 * time.Millisecond)
		}

	}()
}

// get current timestamp in seconds
func TSNow() int {

	__tmp := atomic.LoadInt64(&unixTS)

	// maybe the thread isn't running yet
	if __tmp == 0 {
		__tmp = time.Now().Unix()
	}

	return int(__tmp)
}

func Rand(min, max int) int {

	if min == max {
		return min
	}

	if min > max {
		min, max = max, min
	}

	return rand.Intn(max-min) + min
}

var ch_cas [10]uint32
var mu [10]sync.Mutex

func GetCAS(slab_no int) uint32 {

	x := slab_no % 10
	mu[x].Lock()
	ch_cas[x]++
	mu[x].Unlock()
	return ch_cas[x]
}

// #############################################################################
// Rand String
const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 abcdefghijklmnopqrstuvwxyz" +
	"~!@#$%^&*()-_+={}[]\\|<,>.?/\"';:`"

func RandString(length int) string {
	buf := make([]byte, length)
	for j := 0; j < length; j++ {
		buf[j] = chars[rand.Intn(len(chars))]
	}
	return string(buf)
}

func IsNumber(data []byte) bool {

	if data == nil || len(data) == 0 {
		return false
	}

	for k, v := range data {

		if k == 0 && v == '-' && len(data) > 1 {
			continue
		}

		if v < '0' || v > '9' {
			return false
		}
	}

	return true
}
