package ops

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"gocached/slab"
	"handler_socket2/byteslabs"
	"handler_socket2/hscommon"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// data dump structs

var global_op_mutex sync.Mutex

var preserve_mutex sync.Mutex
var is_saving_cas_threshold uint32
var preserved_items map[string]preserved_item

func init() {
	preserved_items = map[string]preserved_item{}
}

type preserved_item struct {
	data    []byte
	expires uint32
}

func preserveItem(key string, item *slab.StoredItem) {

	preserve_mutex.Lock()
	if _, exists := preserved_items[key]; !exists {
		preserved_items[key] = preserved_item{item.Get(), item.Expires}
	}
	preserve_mutex.Unlock()

}

func preserveItemR(key string, data []byte, expires uint32) {

	// if item is empty, delete it by using empty data and expire time in the past!
	if data == nil {
		data = []byte{}
		expires = 1
	}

	preserve_mutex.Lock()
	preserved_items[key] = preserved_item{data, expires}
	preserve_mutex.Unlock()
}

func StoreToFiles() string {

	global_op_mutex.Lock()
	defer global_op_mutex.Unlock()

	ret := ""

	mode := slab.GetNewCAS()
	atomic.StoreUint32(&is_saving_cas_threshold, mode)
	time.Sleep(100 * time.Millisecond)

	//mode := atomic.LoadUint32(&is_saving_cas_threshold)
	now := uint32(hscommon.TSNow())

	for kvstore_num := 0; kvstore_num < len(kvstores); kvstore_num++ {

		saved_items := map[string]bool{}
		all_keys := []string{}

		stat_items_skipped_preprocess := 0
		stat_items_skipped := 0
		stat_items_saved := 0

		kvstores[kvstore_num].mu.RLock()
		kvs := kvstores[kvstore_num]

		for key, v := range kvs.key_map {
			if saved_items[key] == true || v.Expires < now || v.CAS > mode {
				stat_items_skipped++
				continue
			}
			all_keys = append(all_keys, key)
			saved_items[key] = true
		}

		for key, v := range kvs.key_map_old {
			if saved_items[key] == true || v.Expires < now || v.CAS > mode {
				stat_items_skipped++
				continue
			}
			all_keys = append(all_keys, key)
			saved_items[key] = true
		}
		kvstores[kvstore_num].mu.RUnlock()

		// wait some time before we start processing data
		time.Sleep(100 * time.Millisecond)

		kvstores[kvstore_num].mu.RLock()
		kvs = kvstores[kvstore_num]

		to_store := map[string]preserved_item{}
		for _, key := range all_keys {

			v := getStoredItemUnsafe(kvs, key, false)
			if v == nil || v.Expires < now || v.CAS > mode {
				stat_items_skipped_preprocess++
				continue
			}

			// we can't use opGetKey here because it'll cause deadlock!
			//data := OpGetKey(key)
			data := v.Get()
			if data == nil {
				stat_items_skipped++
				continue
			}

			stat_items_saved++
			to_store[key] = preserved_item{data, v.Expires}
		}
		kvstores[kvstore_num].mu.RUnlock()

		dumper := MakeDumper(fmt.Sprintf("fdata.%d.dump", kvstore_num))
		for key, pi := range to_store {
			dumper.Write(key, &pi)
		}
		dumper.Close()

		_tmp := fmt.Sprintf("Hashlist #%d, Skipped (preprocessing):%d, Skipped:%d, Saved:%d\n",
			kvstore_num, stat_items_skipped_preprocess, stat_items_skipped, stat_items_saved)
		fmt.Printf(_tmp)
		ret += _tmp

	}

	// stop saving process, and wait 1s so we can store changed data without
	// waiting in locked state
	atomic.StoreUint32(&is_saving_cas_threshold, 0)
	time.Sleep(1 * time.Second)

	preserve_mutex.Lock()
	dumper := MakeDumper(fmt.Sprintf("fdata.%d.dump", len(kvstores)))
	for key, pi := range preserved_items {
		dumper.Write(key, &pi)
	}
	preserved_items_count := len(preserved_items)
	preserved_items = map[string]preserved_item{}
	dumper.Close()
	preserve_mutex.Unlock()

	_tmp := fmt.Sprintf("Hashlist #%d, Changed items: %d", len(kvstores)+1, preserved_items_count)
	return ret + _tmp
}

type dumper struct {
	f      *os.File
	writer *bufio.Writer
	path   string
}

func MakeDumper(path string) *dumper {
	ret := &dumper{}
	os.Mkdir("dump", 0666)
	ret.f, _ = os.Create("dump/__" + path)
	ret.writer = bufio.NewWriter(ret.f)
	ret.path = path

	return ret
}

func (this *dumper) Write(key string, i *preserved_item) {

	convbuff := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(convbuff[0:4], i.expires)
	binary.LittleEndian.PutUint32(convbuff[4:8], uint32(len(key)))
	binary.LittleEndian.PutUint32(convbuff[8:12], uint32(len(i.data)))

	this.writer.Write(convbuff)
	this.writer.Write([]byte(key))
	this.writer.Write(i.data)
	this.writer.WriteByte('\r')
}

func (this *dumper) Close() {

	this.writer.Flush()
	this.f.Close()

	os.Rename("dump/__"+this.path, "dump/"+this.path)
}

func ReadFromFiles() string {

	stat_read := 0
	stat_skipped := 0

	filenum := 0
	for ; ; filenum++ {

		file, err := os.Open(fmt.Sprintf("dump/fdata.%d.dump", filenum))
		if err != nil {
			break
		}

		fmt.Println(err)
		fmt.Println(fmt.Sprintf("dump/fdata.%d.dump", filenum))
		allocator := byteslabs.MakeAllocator()
		buffer := allocator.Allocate(100000)
		for {
			header := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
			n, err := file.Read(header)
			if n == 0 && err == io.EOF {
				break
			}

			if n != len(header) {
				fmt.Println("ERROR: can't process dump file: ", filenum)
				break
			}

			expires := binary.LittleEndian.Uint32(header[0:4])
			key_size := binary.LittleEndian.Uint32(header[4:8])
			data_size := binary.LittleEndian.Uint32(header[8:12])
			readsize := int(key_size + data_size + 1)

			if cap(buffer) < readsize {
				allocator.Release()
				allocator = byteslabs.MakeAllocator()
				buffer = allocator.Allocate(readsize + 20000)
			}

			n, err = file.Read(buffer[:readsize])
			if n != readsize || (err != nil && err != io.EOF) {
				fmt.Println("ERROR: can't read KEY from dump file: ", filenum)
				break
			}

			_key := buffer[0:key_size]
			_data := buffer[key_size : key_size+data_size]
			_now := uint32(hscommon.TSNow())

			if _now < expires {
				stat_read++
				OpSetKey(string(_key), _data, expires)
			} else {
				stat_skipped++
			}

			if err == io.EOF {
				break
			}
		}
		allocator.Release()
		file.Close()
	}

	return fmt.Sprintf("Flatdb engine initialization\n ** Keys read: %d\n ** Keys skipped: %d", stat_read, stat_skipped)

}
