package replicator

import (
	"encoding/binary"
	"gocached/common"
	"handler_socket2/byteslabs"
	"sort"

	"fmt"
	"sync"
	"time"
)

var mutex_replica sync.Mutex

const SYNC_KEY_DELETE = 100
const SYNC_KEY_SET = 101
const SYNC_KEY_TOUCH = 102
const SYNC_FLUSH = 200

type sync_item struct {
	name      string
	cas       uint32
	operation byte
}
type bycas []sync_item

func (this bycas) Len() int      { return len(this) }
func (this bycas) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this bycas) Less(i, j int) bool {
	if this[i].cas == this[j].cas {
		if this[j].operation == SYNC_FLUSH {
			return false
		}
		return true
	}
	return this[i].cas < this[j].cas
}

func (this *sync_item) DataWrite(expires uint32, data []byte, out_buffer []byte) int {

	pos := 0
	out_buffer[pos] = this.operation
	pos += 1

	binary.LittleEndian.PutUint32(out_buffer[pos:pos+4], expires)
	pos += 4

	binary.LittleEndian.PutUint32(out_buffer[pos:pos+4], this.cas)
	pos += 4

	binary.LittleEndian.PutUint32(out_buffer[pos:pos+4], uint32(len(this.name)))
	pos += 4
	binary.LittleEndian.PutUint32(out_buffer[pos:pos+4], uint32(len(data)))
	pos += 4

	copy(out_buffer[pos:pos+len(this.name)], this.name)
	pos += len(this.name)

	copy(out_buffer[pos:pos+len(data)], data)
	pos += len(data)

	return pos
}
func (this *sync_item) DataBytesNeeded(data []byte) int {
	return len(this.name) + len(data) + 1 + 4 + 4 + 4 + 4
}

type sync_task struct {
	id       int
	started  int64
	finished int64
	status   string

	stat_item_pending int
	stat_key_set      int
	stat_key_touch    int
	stat_key_delete   int
	stat_flush        int

	stat_uncompressed int
	stat_compressed   int

	items            []sync_item
	_getkey_callback func(name string) (uint32, uint32, []byte)

	data_packets [][]byte

	added_to_dispatcher bool
}

var stat_replicator_time int
var stat_replicator_times = make([]int, 0, 5)
var sync_tasks = make([]*sync_task, 0)

func NewSyncTask(task_id int, item_count int, _getkey_callback func(name string) (uint32, uint32, []byte)) *sync_task {
	new_task := &sync_task{id: task_id, started: time.Now().UnixNano(), status: "Gathering Items",
		stat_item_pending: item_count, _getkey_callback: _getkey_callback, data_packets: make([][]byte, 0)}
	mutex_replica.Lock()
	sync_tasks = append(sync_tasks, new_task)
	mutex_replica.Unlock()

	return new_task
}

func (this *sync_task) setStatus(status string) {
	mutex_replica.Lock()
	this.status = status
	mutex_replica.Unlock()
}

func (this *sync_task) AddItem(name string, cas uint32, operation byte) {

	// flush should be always processed first!
	if operation == SYNC_FLUSH {
		cas = 0
	}

	this.items = append(this.items, sync_item{name, cas, operation})
	this.stat_item_pending--

	switch {
	case operation == SYNC_KEY_SET:
		this.stat_key_set++
	case operation == SYNC_KEY_TOUCH:
		this.stat_key_touch++
	case operation == SYNC_KEY_DELETE:
		this.stat_key_delete++
	case operation == SYNC_FLUSH:
		this.stat_flush++
	}
}

func (this *sync_task) AddPacket(buffer []byte) {

	if buffer == nil || len(buffer) <= 0 {
		return
	}

	allocator := byteslabs.MakeAllocator()
	data := byteslabs.Compress(buffer, allocator)

	ret := make([]byte, len(data)+4)
	binary.LittleEndian.PutUint32(ret[0:4], uint32(len(buffer)))
	copy(ret[4:], data)
	allocator.Release()

	mutex_replica.Lock()
	this.stat_uncompressed += len(buffer) + 4
	this.stat_compressed += len(ret)
	this.data_packets = append(this.data_packets, ret)
	mutex_replica.Unlock()
}

func (this *sync_task) Process() {

	this.setStatus("Pre-Sorting By CAS ...")
	this.stat_item_pending = 0
	sort.Sort(bycas(this.items))

	allocator := byteslabs.MakeAllocator()
	buffer := allocator.Allocate(1024 * 256)
	buffer_pos := 0

	now := uint32(common.TSNow())
	for item_num, item := range this.items {

		if item_num%1000 == 0 {
			this.setStatus(fmt.Sprintf("Preparing Data [%d / %d] ...", item_num, len(this.items)))
		}

		expires, cas, data := this._getkey_callback(item.name)
		if now > expires {
			continue
		}

		// update the "fast" cas used for sorting only to CAS currently held in data store
		item.cas = cas
		bytes_needed := item.DataBytesNeeded(data)

		if bytes_needed > cap(buffer) {
			allocator.Release()
			buffer = allocator.Allocate(bytes_needed + 1024*64)
			buffer_pos = 0
			this.AddPacket(buffer)
			buffer = buffer[:0]
		}

		if buffer_pos+bytes_needed > cap(buffer) {
			buffer_pos = 0
			this.AddPacket(buffer)
			buffer = buffer[:0]
		}

		buffer = buffer[0 : buffer_pos+bytes_needed]
		buffer_pos += item.DataWrite(expires, data, buffer)

	}

	this.AddPacket(buffer)
	allocator.Release()

	this.setStatus("Ready To Send")
	mutex_replica.Lock()
	this.finished = time.Now().UnixNano()

	// dispatch tasks in order, and also if there are 0 items, just mark the task as "virtually" dispatched
	can_dispatch := true
	for _, v := range sync_tasks {

		if v.finished == 0 {
			can_dispatch = false
		}

		if can_dispatch && v.finished != 0 && !v.added_to_dispatcher {
			v.added_to_dispatcher = true
			if v.stat_key_delete > 0 || v.stat_key_set > 0 || v.stat_key_touch > 0 || v.stat_flush > 0 {
				DispatchSyncTask(v)
			}
		}
	}

	//limit tasks in history to 10 max
	_tmp := make([]*sync_task, 0, len(sync_tasks))
	for _, v := range sync_tasks {
		if v.stat_key_delete > 0 || v.stat_key_set > 0 || v.stat_key_touch > 0 || v.stat_flush > 0 {
			_tmp = append(_tmp, v)
		}
	}
	_e := len(_tmp)
	_s := _e - 10
	if _s < 0 {
		_s = 0
	}
	sync_tasks = _tmp[_s:_e]
	mutex_replica.Unlock()
}
