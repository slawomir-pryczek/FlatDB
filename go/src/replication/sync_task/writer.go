package sync_task

import (
	"encoding/binary"
	"fmt"
	"gocached/common"
	"handler_socket2/byteslabs"
	"sort"
	"time"
)

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

func (this *sync_item) dataWrite(expires uint32, data []byte, out_buffer []byte) int {

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
func (this *sync_item) dataBytesNeeded(data []byte) int {
	return len(this.name) + len(data) + 1 + 4 + 4 + 4 + 4
}

func (this *Sync_task) addPacket(buffer []byte) {

	if buffer == nil || len(buffer) <= 0 {
		return
	}

	allocator := byteslabs.MakeAllocator()
	data := byteslabs.Compress(buffer, allocator)

	ret := make([]byte, len(data)+4)
	binary.LittleEndian.PutUint32(ret[0:4], uint32(len(buffer)))
	copy(ret[4:], data)
	allocator.Release()

	sync_tasks_mutex.Lock()
	this.stat_uncompressed += len(buffer) + 4
	this.stat_compressed += len(ret)
	this.Data_Packets = append(this.Data_Packets, ret)
	sync_tasks_mutex.Unlock()
}

func (this *Sync_task) prepareData() {

	sync_tasks_mutex.Lock()
	_f := this.finished
	sync_tasks_mutex.Unlock()
	if _f != 0 {
		return
	}

	this.SetStatus("Pre-Sorting By CAS ...")
	this.stat_item_pending = 0
	sort.Sort(bycas(this.items))

	allocator := byteslabs.MakeAllocator()
	buffer := allocator.Allocate(1024 * 256)
	buffer_pos := 0

	now := uint32(common.TSNow())
	for item_num, item := range this.items {

		if item_num%1000 == 0 {
			this.SetStatus(fmt.Sprintf("Preparing Data [%d / %d] ...", item_num, len(this.items)))
		}

		expires := uint32(0)
		cas := uint32(0)
		data := make([]byte, 0)

		switch {
		case item.operation == SYNC_KEY_SET:
			_e, _c, _d := this._getkey_callback(item.name, true)
			expires = _e
			cas = _c
			if now <= expires {
				data = _d
			}

		case item.operation == SYNC_KEY_TOUCH:
			expires, cas, _ = this._getkey_callback(item.name, false)
		}

		// update the "fast" cas used for sorting only, to CAS currently held in data store
		item.cas = cas
		bytes_needed := item.dataBytesNeeded(data)

		if bytes_needed > cap(buffer) {
			this.addPacket(buffer)

			allocator.Release()
			buffer = allocator.Allocate(bytes_needed + 1024*64)
			buffer_pos = 0
			buffer = buffer[:0]
		}

		if buffer_pos+bytes_needed > cap(buffer) {
			this.addPacket(buffer)

			buffer_pos = 0
			buffer = buffer[:0]
		}

		buffer = buffer[0 : buffer_pos+bytes_needed]
		buffer_pos += item.dataWrite(expires, data, buffer[buffer_pos:buffer_pos+bytes_needed])
	}

	this.addPacket(buffer)
	allocator.Release()

	this.SetStatus("Ready To Send")
	sync_tasks_mutex.Lock()
	this.finished = time.Now().UnixNano()
	sync_tasks_mutex.Unlock()
}
