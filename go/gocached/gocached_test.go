package main

// this will test slab object for overflows and
//

import (
	"gocached/scheduler"
	"gocached/slab"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
}

// ######################## Scheduler

func TestScheduler(t *testing.T) {

	data := "cc"
	x := scheduler.Schedule(scheduler.Type_Add, "XXXXX", &data)

	t.Log("RECV >>", x)

}

func TestStore(t *testing.T) {
	item := slab.Store([]byte("abc"), 0xffffffff, []byte("defg"))

	t.Log(string(slab.Get([]byte("abc"), *item)))
}

func TestSlabsOverflowRecover(t *testing.T) {

	t.Log("Testing Using ", runtime.NumCPU()*2, "CPUs")

	do_test := func(chunk_size int) {

		slab2 := slab.MakeSlab(chunk_size, 1024*1024*64, nil)

		recovered_correctly := false
		test_overflow := func(i int) {

			tmp := ""
			for ii := 0; ii < i; ii++ {
				tmp += string('0' + ii)
			}

			defer func() {

				if r := recover(); r != nil {

					data_size := slab.CalculateChunkSize([]byte("a"), []byte(tmp))

					if data_size != chunk_size+1 {
						t.Log("ERROR: Max size: ", chunk_size, ", data size:", data_size, " DATA: ", tmp, ">>> Data size isn't appropriate for recovery!")
					} else {
						t.Log("Correct recover: ", chunk_size, ", data size:", data_size)
					}

					recovered_correctly = true
				}

			}()

			slab2.Store([]byte("a"), 0xffffffff, []byte(tmp))
		}

		for i := 1; i < 50; i++ {
			test_overflow(i)
			if recovered_correctly {
				break
			}
		}

		if !recovered_correctly {
			t.Error("Overflow catching isn't working!")
		}
	}

	for i := 0; i < 50; i++ {
		do_test(rand.Intn(30) + 18)
	}

}

func TestMultithreading(t *testing.T) {

	max_key_size := 50
	max_val_size := 100
	chunk_size_required := slab.CalculateChunkSizeI(max_key_size, max_val_size)
	slab2 := slab.MakeSlab(chunk_size_required, 1024*1024*512, nil)

	do_test := func(randomize_sizes bool, thr_id int) {

		ks := make([]string, 0, 1000)
		vs := make([]string, 0, 1000)
		ids := make([]uint16, 0, 1000)

		for i := 0; i < 5000; i++ {

			k := rand_str(max_key_size)
			v := rand_str(max_val_size)
			if randomize_sizes {
				k = rand_str(rand.Intn(max_key_size + 1))
				v = rand_str(rand.Intn(max_val_size + 1))
			}

			chunk_id := slab2.Store([]byte(k), 0xffffffff, []byte(v))

			ks = append(ks, k)
			vs = append(vs, v)
			ids = append(ids, *chunk_id)

			if i%500 == 0 {
				t.Log("Thr: ", thr_id, " chunk id: ", *chunk_id, "items left:", 5000-i)
				runtime.Gosched()
			}

		}

		t.Log("Finished INSERT, testing thr", thr_id)

		ok := 0
		fail := 0

		for _k, id := range ids {

			if _k%500 == 0 {
				runtime.Gosched()
			}

			k := ks[_k]
			v := vs[_k]

			chunk_handle := slab2.GetChunkForRead(id)
			if chunk_handle == nil {
				t.Error("Can't read chunk ID ", id)
				t.Fail()
			}
			if chunk_handle.ReadKey() == nil {
				t.Error("Can't read KEY from chunk ", id)
				t.Fail()
			}

			k2 := string(chunk_handle.ReadKey())
			v2 := string(chunk_handle.ReadValue())

			if k2 != k || v2 != v {
				fail++
			} else {
				ok++
			}

		}

		if fail > 0 {
			t.Error("Testing thread FAILED!", thr_id, "OK: ", ok, "FAILED: ", fail)
			t.Fail()
		} else {
			t.Log("Testing thread OK", thr_id, "OK: ", ok, "FAILED: ", fail)
		}

	}

	wg := sync.WaitGroup{}
	wg.Add(6)
	go func() { do_test(true, 0); wg.Done() }()
	go func() { do_test(true, 1); wg.Done() }()
	time.Sleep(time.Millisecond * 50)
	go func() { do_test(false, 2); wg.Done() }()
	time.Sleep(time.Millisecond * 50)
	go func() { do_test(false, 3); wg.Done() }()
	time.Sleep(time.Millisecond * 50)
	go func() { do_test(false, 4); wg.Done() }()
	time.Sleep(time.Millisecond * 50)
	go func() { do_test(true, 5); wg.Done() }()
	time.Sleep(time.Millisecond * 50)
	go func() { do_test(true, 6); wg.Done() }()
	wg.Wait()
}

func rand_str(chars int) string {

	tmp := ""
	for i := 0; i < chars; i++ {
		tmp += string('A' + (rand.Intn('Z' - 'A')))
	}
	return tmp
}
