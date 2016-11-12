package slab

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type slab_class struct {
	slab_class_no uint8

	slab_chunk_size  int
	slab_chunk_count int
	slab_margin      int

	slabs []*slab

	mu             sync.RWMutex
	_lastfree_slab int

	stat_store_failed           int
	stat_slab_freed             int
	stat_rebal_slabs_rebalanced int
	stat_rebal_gc_needed        int

	stat_rebal_took_us       uint64
	stat_rebal_slabs_scanned uint64

	stat_rebal_item_moved       uint64
	stat_rebal_item_failed      uint64
	stat_rebal_slab_interrupted uint64
}

type StoredItem struct {
	slab_class_no uint8
	slab_no       uint16
	chunk_no      uint16

	CAS, Expires uint32
}

const min_chunk_size = 32
const max_chunk_size = 1024 * 1024 * 2
const max_slab_size = 1024 * 1024 * 3
const factor = 0.12
const slabs_max = 2000

var slabs_allocated = int32(0)
var slabs_freed = int32(0)
var slab_classes []*slab_class

func init() {

	slab_classes = make([]*slab_class, 0)

	i := 0
	for size := float64(min_chunk_size); int(size) < max_slab_size; {

		_size := int(size)
		_chunks := max_slab_size / _size
		if int(_chunks) > 0xffff {
			fmt.Printf(" !Too many chunks per slab, maximum is uint16: %d/%d\n", _chunks, 0xffff)
			_chunks = 0xffff
		}

		size_adjusted := max_slab_size / _chunks
		margin := max_slab_size - (size_adjusted * _chunks)

		fmt.Printf("Slab class #%d, %d chunks, chunk size=%db (margin %db)\n", i, _chunks, size_adjusted, margin)

		if int(_chunks) == 0 {
			continue
		}

		tmp := slab_class{}
		tmp.slab_class_no = uint8(i)
		tmp.slab_chunk_size = size_adjusted
		tmp.slab_chunk_count = _chunks
		tmp.slab_margin = margin

		tmp.slabs = make([]*slab, 0)
		slab_classes = append(slab_classes, &tmp)

		size += (factor + 1) * size
		i++
	}

	// automatic cleanup function!
	go func() {
		for {
			time.Sleep(1500 * time.Millisecond)

			//elxRun()
			elxRun()
			RunGC()

		}
	}()

}

func RunGC() {

	for _, slab_class := range slab_classes {

		// let's copy all slab addresses to run GC without locking the manager!
		slab_class.mu.RLock()
		_tmp := make([]*slab, len(slab_class.slabs))
		copy(_tmp, slab_class.slabs)
		slab_class.mu.RUnlock()

		for _, slab := range _tmp {
			slab.RunGC()
		}
	}

}

func getSlabClass(slab_class_no int) *slab_class {

	if slab_class_no < 0 || slab_class_no > 0xff || slab_class_no > len(slab_classes) {
		panic("Slab overflow")
	}

	return slab_classes[uint8(slab_class_no)]
}

func getSlabClassFromSize(size_data int) *slab_class {

	requested_chunk_size := CalculateChunkSizeI(size_data)

	slab_class_no := -1
	for i := 0; i < len(slab_classes); i++ {
		if requested_chunk_size < slab_classes[i].slab_chunk_size {
			slab_class_no = i
			break
		}
	}

	if slab_class_no == -1 {
		return nil
	}

	return getSlabClass(slab_class_no)
}

// add slab this function is not safe, you need to acquire lock to call it!
func (this *slab_class) _addSlabUnsafe() *int {

	// we cannot allocate any more
	if atomic.LoadInt32(&slabs_allocated) >= slabs_max {
		return nil
	}
	atomic.AddInt32(&slabs_allocated, 1)

	mem_size := (this.slab_chunk_size * this.slab_chunk_count) + this.slab_margin
	tmp := makeSlab(this.slab_chunk_size, this.slab_chunk_count, make([]byte, mem_size))

	//this.mu.Lock()
	this.slabs = append(this.slabs, tmp)
	ret := len(this.slabs) - 1
	//this.mu.Unlock()

	return &ret
}

var xxxx = 0

func Store(data []byte, expires uint32) *StoredItem {

	xxxx++

	slab_class := getSlabClassFromSize(len(data))
	if slab_class == nil {
		fmt.Println("Item too large: ", len(data), "\n")
		if len(data) > 200 {
			fmt.Println("Item too large: ", data[:200], "\n")
		}
		return nil
	}

	shuffle := slab_class._lastfree_slab
	slab_no := -1
	chunk_no := -1
	cas := uint32(0)

	// Add item in already allocated slab(s), this operation won't write to slab list
	// acquire read lock!
	slab_class.mu.RLock()
	_len := len(slab_class.slabs)

	if _len > 2 {
		_rnd_slab := int(cas_global+expires) % _len
		if slab_class.slabs[_rnd_slab].chunks_free > 0 {

			if ok, _chunk_no, _cas := slab_class.slabs[_rnd_slab].Store(expires, data); ok {
				slab_no = _rnd_slab
				chunk_no = int(_chunk_no)
				cas = _cas

				slab_class.mu.RUnlock()
				return &StoredItem{slab_class.slab_class_no, uint16(slab_no), uint16(chunk_no), cas, expires}
			}
		}
	}

	for i := 0; chunk_no == -1 && i < _len; i++ {

		// don't lock SLAB, operations are thread safe, so we're doing double-check!
		// TODO: Can be unsafe!
		pos := (i + shuffle) % _len
		if slab_class.slabs[pos].chunks_free == 0 {
			continue
		}

		if ok, _chunk_no, _cas := slab_class.slabs[pos].Store(expires, data); ok {
			slab_no = pos
			chunk_no = int(_chunk_no)
			cas = _cas

			if xxxx%10000 == 0 {
				fmt.Print("c", i)
			}
		}
	}
	slab_class.mu.RUnlock()

	for chunk_no == -1 && false {

		pos := slab_class.elxGCWalk()
		if pos < 0 {
			break
		}

		slab_class.mu.RLock()
		if slab_class.slabs[pos].chunks_free > 0 {

			if ok, _chunk_no, _cas := slab_class.slabs[pos].Store(expires, data); ok {
				slab_no = pos
				chunk_no = int(_chunk_no)
				cas = _cas
			}
		}
		slab_class.mu.RUnlock()

	}

	//	ok let's try to allocate a new SLAB, this is write-op, so we'll nee a lock
	if chunk_no == -1 {
		if xxxx%10000 == 0 {
			fmt.Print("x")
		}

		slab_class.mu.Lock()
		for i := 0; chunk_no == -1 && i < len(slab_class.slabs); i++ {

			// this time DO locking, so we're sure we put the value in cache if we still
			// have some memory avail
			if ok, _chunk_no, _cas := slab_class.slabs[i].Store(expires, data); ok {
				slab_no = i
				chunk_no = int(_chunk_no)
				cas = _cas
			}
		}

		if chunk_no == -1 {
			slab_no_ptr := slab_class._addSlabUnsafe()
			if slab_no_ptr != nil {

				if ok, _chunk_no, _cas := slab_class.slabs[*slab_no_ptr].Store(expires, data); ok {
					slab_no = *slab_no_ptr
					chunk_no = int(_chunk_no)
					cas = _cas
				}

			}
		}

		if slab_no >= 0 {
			slab_class._lastfree_slab = slab_no
		}

		slab_class.mu.Unlock()
	}

	if chunk_no > -1 {
		return &StoredItem{slab_class.slab_class_no, uint16(slab_no), uint16(chunk_no), cas, expires}
	}

	slab_class.mu.Lock()
	slab_class.stat_store_failed++
	slab_class.mu.Unlock()

	return nil
}

func (this *StoredItem) GetUsingBuffer(getBuffer func(int) []byte) []byte {

	slab_class := getSlabClass(int(this.slab_class_no))
	slab_class.mu.RLock()
	if int(this.slab_no) >= len(slab_class.slabs) {
		slab_class.mu.RUnlock()
		return nil
	}
	slab := slab_class.slabs[this.slab_no]
	slab_class.mu.RUnlock()

	chunk := slab.GetChunkForReadC(this.chunk_no, this.CAS)
	if chunk == nil {
		return nil
	}

	val := chunk.ReadValue()
	buffer := getBuffer(len(val))
	copy(buffer[0:len(val)], val)
	chunk.Release()

	return buffer[0:len(val)]
}

func (this *StoredItem) Get() []byte {

	slab_class := getSlabClass(int(this.slab_class_no))
	slab_class.mu.RLock()
	if int(this.slab_no) >= len(slab_class.slabs) {
		slab_class.mu.RUnlock()
		return nil
	}
	slab := slab_class.slabs[this.slab_no]
	slab_class.mu.RUnlock()

	chunk := slab.GetChunkForReadC(this.chunk_no, this.CAS)
	if chunk == nil {
		return nil
	}

	val := chunk.ReadValue()
	val_copy := make([]byte, len(val))
	copy(val_copy, val)
	chunk.Release()

	return val_copy
}

func (this *StoredItem) Exists() bool {

	slab_class := getSlabClass(int(this.slab_class_no))
	slab_class.mu.RLock()
	if int(this.slab_no) >= len(slab_class.slabs) {
		slab_class.mu.RUnlock()
		return false
	}
	slab := slab_class.slabs[this.slab_no]
	slab_class.mu.RUnlock()

	if cas, exists := slab.ReadCAS(this.chunk_no); exists {
		if cas == this.CAS {
			return true
		}
	}

	return false
}

func (this *StoredItem) Delete() {

	slab_class := getSlabClass(int(this.slab_class_no))
	slab_class.mu.RLock()
	if int(this.slab_no) >= len(slab_class.slabs) {
		slab_class.mu.RUnlock()
		return
	}
	slab := slab_class.slabs[this.slab_no]
	slab_class.mu.RUnlock()

	slab.DeleteLiveChunk(this.chunk_no, this.CAS)

	/*slab_class := getSlabClass(int(this.slab_class_no))
	slab_class.mu.RLock()
	slab := slab_class.slabs[this.slab_no]
	slab_class.mu.RUnlock()

	slab.DeleteChunk(this.chunk_no)*/

	//slab_classes[this.slab_class_no].slabs[this.slab_no].DeleteChunk(this.chunk_no)

}

func (this *StoredItem) Touch(expires uint32) (bool, uint32) {

	slab_class := getSlabClass(int(this.slab_class_no))
	slab_class.mu.RLock()
	if int(this.slab_no) >= len(slab_class.slabs) {
		slab_class.mu.RUnlock()
		return false, 0
	}
	slab := slab_class.slabs[this.slab_no]
	slab_class.mu.RUnlock()

	return slab.Touch(this.chunk_no, this.CAS, expires)
}

func (this *StoredItem) Increment(by int64, expires uint32) (*StoredItem, []byte) {

	slab_class := getSlabClass(int(this.slab_class_no))
	slab_class.mu.RLock()
	slab := slab_class.slabs[this.slab_no]
	slab_class.mu.RUnlock()

	if value, new_data, new_cas := slab.Increment(this.chunk_no, this.CAS, by, expires); value != nil {

		// new data was allocated, we need to store it
		if new_data != nil {
			slab.DeleteLiveChunk(this.chunk_no, this.CAS)
			return Store(new_data, expires), value
		}

		this.CAS = new_cas
		this.Expires = expires
		return this, value
	}

	return nil, nil
}

func (this *StoredItem) InSlabBetween(slab_class_no uint8, min, max uint16) bool {

	if this.slab_class_no != slab_class_no {
		return false
	}

	return this.slab_no >= min && this.slab_no <= max
}

func (this *StoredItem) GetSlabNo() uint16 {
	return this.slab_no
}

func (this *StoredItem) ReplaceCAS(oldCAS, newCAS uint32) bool {

	if this.CAS != oldCAS {
		return false
	}

	slab_class := getSlabClass(int(this.slab_class_no))
	slab_class.mu.RLock()
	if int(this.slab_no) >= len(slab_class.slabs) {
		slab_class.mu.RUnlock()
		return false
	}
	slab := slab_class.slabs[this.slab_no]
	slab_class.mu.RUnlock()

	return slab.ReplaceCAS(this.chunk_no, oldCAS, newCAS)
}
