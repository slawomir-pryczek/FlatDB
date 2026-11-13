package main

import (
	"fmt"
	"gocached/common"
	"gocached/slab"
	"hash/crc32"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
)

/*type kvitem struct {
	slab_class uint8
	slab_num   uint16
	chunk_id   uint16
}*/

type kvstore struct {
	key_map     map[string]slab.StoredItem
	key_map_old map[string]slab.StoredItem

	mu sync.RWMutex
}

var kvstores []kvstore

func init() {

	kvstores = make([]kvstore, runtime.NumCPU()*4)
	for i := 0; i < len(kvstores); i++ {

		tmp := kvstore{}
		tmp.key_map = make(map[string]slab.StoredItem)
		tmp.key_map_old = make(map[string]slab.StoredItem)

		kvstores[i] = tmp

	}

}

func OpSetKey(key string, data []byte, expires uint32) {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs := &kvstores[int(cs)%len(kvstores)]

	kvs.mu.Lock()

	kvs.key_map[key] = *slab.Store(data, expires)

	if _, exists := kvs.key_map_old[key]; exists {
		delete(kvs.key_map_old, key)
	}

	kvs.mu.Unlock()
}

func OpGetKey(key string) []byte {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs := &kvstores[int(cs)%len(kvstores)]

	ret := []byte{}
	cas := uint32(0)

	kvs.mu.RLock()
	if item, exists := kvs.key_map[key]; exists {
		ret = item.Get()
		cas = item.CAS
	}

	if item, exists := kvs.key_map_old[key]; exists {

		_ret := item.Get()

		if len(ret) == 0 {
			ret = _ret
			cas = item.CAS
		}

		if len(_ret) > 0 && (item.CAS > cas || cas-item.CAS > 0x7FFFFFFF /*maybe CAS rolled over*/) {
			ret = _ret
			cas = item.CAS
		}

	}

	kvs.mu.RUnlock()

	return ret
}

func mainx() {

	num_cpu := runtime.NumCPU() * 4
	runtime.GOMAXPROCS(num_cpu)

	ts := common.NewTimeSpan()

	wg := sync.WaitGroup{}
	total := 0
	i := 0
	test := func() {

		for ; i < 1000000; i++ {

			common.GetCAS(i)

		}
		wg.Done()
		total += i * 3
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go test()
	}
	wg.Wait()

	fmt.Printf("\n%s total ; %fms /10k items", ts.Get(), ts.GetRaw()/(float64(total)/10000))

	fmt.Println(slab.GetSlabInfo())
}

func main() {

	num_cpu := runtime.NumCPU() * 4
	runtime.GOMAXPROCS(num_cpu)

	OpSetKey("abc", []byte{1, 2, 3}, uint32(common.TSNow()+10))
	OpSetKey("abcd", []byte{2, 3}, uint32(common.TSNow()+10))

	fmt.Println(OpGetKey("abc"))
	fmt.Println(OpGetKey("abcd"))

	run_kvs_testing()

	return

	run_slab_testing()
}

func run_kvs_testing() {

	data := []byte(common.RandString(2 * 60))
	data2 := []byte(common.RandString(2 * 2))
	data3 := []byte(common.RandString(2 * 20))

	wg := sync.WaitGroup{}
	ts := common.NewTimeSpan()
	total := 0

	test := func() {
		i := 0
		now := common.TSNow()
		for ; i < 1000000; i++ {
			k := common.RandString(5)
			OpSetKey("1"+k, data, uint32(now+i%3))
			OpSetKey("2"+k, data2, uint32(now+i%3))
			OpSetKey("3"+k, data3, uint32(now+i%3))
		}

		total += i
		wg.Done()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go test()
	}
	wg.Wait()

	fmt.Printf("\n%s total ; %fms /10k items", ts.Get(), ts.GetRaw()/(float64(total)/10000))

}

func run_slab_testing() {

	data2 := []byte(common.RandString(2 * 2))

	data := []byte(common.RandString(2 * 60))
	data3 := []byte(common.RandString(2 * 20))

	if false {
		f, _ := os.Create("profiler4")
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ts := common.NewTimeSpan()

	wg := sync.WaitGroup{}
	total := 0
	test := func() {
		i := 0
		now := common.TSNow()
		for ; i < 1000000; i++ {
			si := slab.Store(data, uint32(now+i%3))

			si2 := slab.Store(data2, uint32(now+i%3))

			si3 := slab.Store(data3, uint32(now+i%3))

			func() {
				si.Get()
				si2.Get()
				si3.Get()
				si2.Delete()
			}()

			if i%10000 == 0 {
				fmt.Println(si)
				now = common.TSNow()
			}
		}
		wg.Done()
		total += i * 3
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go test()
	}
	wg.Wait()

	fmt.Printf("\n%s total ; %fms /10k items", ts.Get(), ts.GetRaw()/(float64(total)/10000))

	fmt.Println(slab.GetSlabInfo())

}
