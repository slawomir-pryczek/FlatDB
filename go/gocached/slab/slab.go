package slab

import (
	"encoding/binary"
	"gocached/common"
	"strconv"
	"sync"
	"sync/atomic"
)

var cas_global = uint32(1)

const CLEANUP_LIST_ITEM_RATIO int = 4
const CLEANUP_LIST_ITEM_MIN int = 10

// INITIALIZING -> CAN_USE -> SCHEDULED_FOR_DELETION
const STATE_INITIALIZING byte = 0x0
const STATE_CAN_USE byte = 0x5
const STATE_SCHEDULED_FOR_DELETION = 0x10
const STATE_DELETED = 0xff

const CHUNK_POS_STATE int = 0
const CHUNK_POS_READ_LOCKS int = 1
const CHUNK_POS_EXPIRES int = 5
const CHUNK_POS_CAS int = 9
const CHUNK_POS_DATASIZE int = 13
const CHUNK_POS_USERDATA int = 17

func CalculateChunkSize(data []byte) int {
	return CalculateChunkSizeI(len(data))
}

func CalculateChunkSizeI(size_data int) int {
	return CHUNK_POS_USERDATA + size_data
}

type slab struct {
	allow_inserts bool
	chunk_size    int
	chunks_count  int
	chunks_free   int

	stat_op_alloc            int
	stat_alloc_size          int
	stat_op_delete           int
	stat_op_free             int
	stat_op_free_postponed   int
	stat_op_delete_postponed int
	stat_gc_run_count        int
	stat_gc_skip_count       int

	mu sync.Mutex

	chunks_free_list    []uint16
	chunks_cleanup_list []uint16
	storage             []byte

	elx_expire      []uint32
	elx_gc_next_run uint32
	elx_gc_pos      uint16
}

// Creates new slab structure
func makeSlab(chunk_size int, chunks_count int, storage []byte) *slab {

	if chunks_count > 0xffff {
		chunks_count = 0xffff
	}

	chunks_free_list := make([]uint16, chunks_count)
	for i := 0; i < int(chunks_count); i++ {
		chunks_free_list[i] = uint16(chunks_count - i - 1)
	}

	cleanup_list_count := chunks_count / CLEANUP_LIST_ITEM_RATIO
	if cleanup_list_count < CLEANUP_LIST_ITEM_MIN {
		cleanup_list_count = CLEANUP_LIST_ITEM_MIN
	}
	if cleanup_list_count < chunks_count {
		cleanup_list_count = chunks_count
	}

	chunks_cleanup_list := make([]uint16, 0, cleanup_list_count)

	tmp := slab{}
	tmp.allow_inserts = true
	tmp.chunk_size = chunk_size
	tmp.chunks_count = chunks_count
	tmp.chunks_free = chunks_count

	tmp.chunks_free_list = chunks_free_list
	tmp.chunks_cleanup_list = chunks_cleanup_list
	if storage == nil {
		tmp.storage = make([]byte, chunk_size*chunks_count)
	} else {
		tmp.storage = storage
	}
	tmp.elx_expire = make([]uint32, chunks_count)

	return &tmp
}

// returns chunk data
func (this *slab) getChunkMemoryPtr(chunk_id uint16) []byte {

	pos := this.chunk_size * int(chunk_id)
	return this.storage[pos : pos+this.chunk_size]

}

// checks if slab has any free room
func (this *slab) CanStore() bool {

	this.mu.Lock()
	if !this.allow_inserts || this.chunks_free == 0 {
		this.mu.Unlock()
		return false
	}

	this.mu.Unlock()
	return true
}

// This function will store data in a free chunk
// This is a 3 step process
//  A. chunk is removed from free list, expire time and other basic data are set
//		put and state is set to STATE_INITIALIZING
//
//  B. Lock is released and data is copied to chunk's memory
//  C. Lock is applied to change state from STATE_INITIALIZING to STATE_CAN_USE

// Expires is expiration timestamp
// Data is data we put
// New CAS will be generated and returned if successfull
// Structure used is STATE(byte), READ_LOCKS(uint32), EXPIRES(uint32), DATA_SIZE(uint32), USER_DATA
func (this *slab) Store(expires uint32, data []byte) (bool, uint16, uint32) {

	if CalculateChunkSizeI(len(data)) > this.chunk_size {
		panic("Chunk data too long")
	}

	cas := GetNewCAS()

	this.mu.Lock()
	if !this.allow_inserts || this.chunks_free == 0 {
		this.mu.Unlock()
		return false, 0, 0
	}

	_len := len(this.chunks_free_list)
	chunk_id := this.chunks_free_list[_len-1]
	this.chunks_free_list = this.chunks_free_list[:_len-1]
	this.chunks_free--

	this.stat_op_alloc++
	this.stat_alloc_size += len(data)

	// mark this block as initializing!
	memory := this.getChunkMemoryPtr(chunk_id)
	memory[CHUNK_POS_STATE] = STATE_INITIALIZING
	//this.mu.Unlock()

	// Write SLAB data without holding LOCK
	// P=1 - read_locks
	p := CHUNK_POS_READ_LOCKS
	binary.LittleEndian.PutUint32(memory[p:p+4], 0)

	// P=5 - expires
	p = CHUNK_POS_EXPIRES
	binary.LittleEndian.PutUint32(memory[p:p+4], expires)

	// P=9 - CAS
	p = CHUNK_POS_CAS
	binary.LittleEndian.PutUint32(memory[p:p+4], cas)

	// Report expire time to GC
	this.elxGCReportExpire(chunk_id, expires)

	// Write SLAB data without holding LOCK
	// we need to initialize CAS, EXPIRATION, and RLOCKS before!
	this.mu.Unlock()

	// P=13 - datasize
	p = CHUNK_POS_DATASIZE
	binary.LittleEndian.PutUint32(memory[p:p+4], uint32(len(data)))

	// P=17 - key data
	p = CHUNK_POS_USERDATA
	copy(memory[p:p+len(data)], data)
	p += len(data)

	// set item state to initialized, double check because the item could get deleted in the meantime!!
	this.mu.Lock()
	if memory[CHUNK_POS_STATE] == STATE_INITIALIZING {
		memory[CHUNK_POS_STATE] = STATE_CAN_USE
	}
	this.mu.Unlock()

	return true, chunk_id, cas
}

func (this *slab) ReadCAS(chunk_id uint16) (uint32, bool) {

	this.mu.Lock()
	memory := this.getChunkMemoryPtr(chunk_id)

	// we can only process chunk that's in CAN-USE state
	if memory[CHUNK_POS_STATE] != STATE_CAN_USE {
		this.mu.Unlock()
		return 0, false
	}

	// do we hold any read - locks?
	_expires := binary.LittleEndian.Uint32(memory[CHUNK_POS_EXPIRES : CHUNK_POS_EXPIRES+4])
	expired := (uint32(common.TSNow()) > _expires)

	if expired {
		this.mu.Unlock()
		return 0, false
	}

	_cas := binary.LittleEndian.Uint32(memory[CHUNK_POS_CAS : CHUNK_POS_CAS+4])
	this.mu.Unlock()

	return _cas, true
}

func (this *slab) Touch___NoCasUpdate(chunk_id uint16, cas uint32, expires uint32) bool {
	ret, _ := this.__touch_op(chunk_id, cas, expires, false)
	return ret
}

func (this *slab) Touch(chunk_id uint16, cas uint32, expires uint32) (bool, uint32) {
	return this.__touch_op(chunk_id, cas, expires, true)
}

func (this *slab) __touch_op(chunk_id uint16, cas uint32, expires uint32, update_cas bool) (bool, uint32) {

	this.mu.Lock()
	memory := this.getChunkMemoryPtr(chunk_id)

	_state := memory[CHUNK_POS_STATE]
	if _state != STATE_CAN_USE {
		this.mu.Unlock()
		return false, 0
	}

	_tmp := memory[CHUNK_POS_CAS : CHUNK_POS_CAS+4]
	_cas := binary.LittleEndian.Uint32(_tmp)

	if cas != _cas {
		this.mu.Unlock()
		return false, 0
	}

	_expires := binary.LittleEndian.Uint32(memory[CHUNK_POS_EXPIRES : CHUNK_POS_EXPIRES+4])
	expired := (uint32(common.TSNow()) > _expires)
	if expired {
		this.mu.Unlock()
		return false, 0
	}

	cas_new := uint32(0)
	if update_cas {
		cas_new = GetNewCAS()
		binary.LittleEndian.PutUint32(memory[CHUNK_POS_CAS:CHUNK_POS_CAS+4], cas_new)
	}
	binary.LittleEndian.PutUint32(memory[CHUNK_POS_EXPIRES:CHUNK_POS_EXPIRES+4], uint32(expires))

	// Report new expire time to GC, so we can optimize GCs better!
	this.elxGCReportExpire(chunk_id, expires)
	this.mu.Unlock()

	return true, cas_new
}

func (this *slab) ReplaceCAS(chunk_id uint16, oldCAS, newCAS uint32) bool {

	this.mu.Lock()
	memory := this.getChunkMemoryPtr(chunk_id)

	// we can only process chunk that's in CAN-USE state
	if memory[CHUNK_POS_STATE] != STATE_CAN_USE {
		this.mu.Unlock()
		return false
	}

	// do we hold any read - locks?
	_expires := binary.LittleEndian.Uint32(memory[CHUNK_POS_EXPIRES : CHUNK_POS_EXPIRES+4])
	expired := (uint32(common.TSNow()) > _expires)

	if expired {
		this.mu.Unlock()
		return false
	}

	_cas := binary.LittleEndian.Uint32(memory[CHUNK_POS_CAS : CHUNK_POS_CAS+4])
	if _cas != oldCAS {
		this.mu.Unlock()
		return false
	}

	binary.LittleEndian.PutUint32(memory[CHUNK_POS_CAS:CHUNK_POS_CAS+4], newCAS)
	this.mu.Unlock()
	return true
}

// This function will delete chunk that has live data, basically disallowing the value to be read
// It'll set STATE to one of output values and change expire time to 1
// It'll also automatically garbage collect the chunk if possible
// STATE map
// CONSTRAINTS: STATE_CAN_USE || STATE_INITIALIZING; CAS needs to match
//
// OUTPUT: STATE_DELETED if there are no read locks held, the item is also immediatelly garbage-collected
// OUTPUT: STATE_SCHEDULED_FOR_DELETION if there are read locks, so threads are reading currently; item needs to be garbage-collected later
func (this *slab) DeleteLiveChunk(chunk_id uint16, cas uint32) bool {

	this.mu.Lock()
	// this.mu.Unlock() this is slow
	memory := this.getChunkMemoryPtr(chunk_id)

	_state := memory[CHUNK_POS_STATE]
	if _state != STATE_CAN_USE && _state != STATE_INITIALIZING {
		this.mu.Unlock()
		return false
	}

	_tmp := memory[CHUNK_POS_CAS : CHUNK_POS_CAS+4]
	_cas := binary.LittleEndian.Uint32(_tmp)

	if cas != _cas {
		this.mu.Unlock()
		return false
	}

	_tmp = memory[CHUNK_POS_READ_LOCKS : CHUNK_POS_READ_LOCKS+4]
	_read_lck := binary.LittleEndian.Uint32(_tmp)
	if _read_lck > 0 {
		memory[CHUNK_POS_STATE] = STATE_SCHEDULED_FOR_DELETION
		binary.LittleEndian.PutUint32(memory[CHUNK_POS_EXPIRES:CHUNK_POS_EXPIRES+4], uint32(1))
		this.stat_op_delete_postponed++
		this.mu.Unlock()
		return true
	}

	memory[CHUNK_POS_STATE] = STATE_DELETED
	this.elx_expire[chunk_id] = 0
	binary.LittleEndian.PutUint32(memory[CHUNK_POS_EXPIRES:CHUNK_POS_EXPIRES+4], uint32(0))

	this.chunks_free++
	this.chunks_free_list = append(this.chunks_free_list, chunk_id)

	this.stat_op_delete++
	this.mu.Unlock()

	return true
}

// This function will garbage-collect old / deleted chunks that has no live data and is _NOT_ being read
// It'll check expire time and read locks. Item exp time needs to be in the past, and read locks need to be 0
//
// It'll set STATE to one of output values and change expire time to 1
// STATE map
// CONSTRAINTS: STATE_CAN_USE || STATE_SCHEDULED_FOR_DELETION
//
// OUTPUT: STATE_DELETED, item is garbage-collected
// Needs LOCK on SLAB
func (this *slab) CollectChunkUnsafe(chunk_id uint16) bool {

	memory := this.getChunkMemoryPtr(chunk_id)

	_state := memory[CHUNK_POS_STATE]
	if _state != STATE_CAN_USE && _state != STATE_SCHEDULED_FOR_DELETION {
		return false
	}

	// check that the item is expired, if not - we cannot remove it
	_tmp := memory[CHUNK_POS_EXPIRES : CHUNK_POS_EXPIRES+4]
	_exp := binary.LittleEndian.Uint32(_tmp)
	if _exp > uint32(common.TSNow()) {
		return false
	}

	// if we're not reading anything from the chunk, remove it immediately!
	_tmp = memory[CHUNK_POS_READ_LOCKS : CHUNK_POS_READ_LOCKS+4]
	_read_lck := binary.LittleEndian.Uint32(_tmp)
	if _read_lck == 0 {
		this.chunks_free++
		this.chunks_free_list = append(this.chunks_free_list, chunk_id)
		memory[CHUNK_POS_STATE] = STATE_DELETED

		this.elx_expire[chunk_id] = 0
		binary.LittleEndian.PutUint32(memory[CHUNK_POS_EXPIRES:CHUNK_POS_EXPIRES+4], 0)
		this.stat_op_free++
		return true
	} else {
		this.stat_op_free_postponed++
	}

	// we can't remove now - do nothing!
	return false
}

// increment the value IN PLACE, if possible, original value stored needs to be integer
// if we need to re-allocate the SLAB we'll return pointer to new number byte array
// if we cannot proceed, we'll return false
func (this *slab) Increment(chunk_id uint16, cas uint32, by int64, expires uint32) ([]byte, []byte, uint32) {

	this.mu.Lock()
	memory := this.getChunkMemoryPtr(chunk_id)

	curr_int := int64(0)
	is_ok := false
	if curr_val := this.ReadValueUnsafeC(chunk_id, cas); curr_val != nil {

		is_ok = common.IsNumber(curr_val)
		if i, err := strconv.ParseInt(string(curr_val), 10, 64); is_ok && err == nil {
			curr_int = i
		} else {
			is_ok = false
		}

		if is_ok {
			curr_int += by
			new_val := []byte(strconv.FormatInt(curr_int, 10))

			_tmp := memory[CHUNK_POS_READ_LOCKS : CHUNK_POS_READ_LOCKS+4]
			_read_lck := binary.LittleEndian.Uint32(_tmp)

			// we can update the value in-place, just copyit into the slab's memory
			// and return nil so we won't use another SLAB to save it
			// only if data length doesn't change and we're not reading from SLAB currently
			if _read_lck == 0 && len(new_val) == len(curr_val) {
				copy(curr_val, new_val)

				cas_new := GetNewCAS()
				binary.LittleEndian.PutUint32(memory[CHUNK_POS_CAS:CHUNK_POS_CAS+4], cas_new)
				binary.LittleEndian.PutUint32(memory[CHUNK_POS_EXPIRES:CHUNK_POS_EXPIRES+4], expires)

				// Report expire time to GC
				this.elxGCReportExpire(chunk_id, expires)

				this.mu.Unlock()
				return new_val, nil, cas_new
			}

			// item length changed, return item data to store the value again
			this.mu.Unlock()
			return new_val, new_val, 0
		}
	}

	this.mu.Unlock()
	return nil, nil, 0
}

func GetNewCAS() uint32 {
	cas := atomic.AddUint32(&cas_global, uint32(1))
	if cas == 0 {
		cas = atomic.AddUint32(&cas_global, uint32(1))
	}

	return cas
}

// ##################################################################################
// OLD CODE BELOW - NO LONGER USED

// DELETE chunk only if it's in state CAN_USE
// let's not delete UNINITIALIZED variables, it could lead to race condition
// this should be used to do cleanup-delete -> remove expired items
// for deleting live items we should use DeleteLiveChunk
func (this *slab) DeleteChunkPostpone(chunk_id uint16) bool {

	memory := this.getChunkMemoryPtr(chunk_id)

	if !(memory[CHUNK_POS_STATE] == STATE_CAN_USE) {
		return false
	}

	// check that the item is expired, if not - we cannot remove it
	_tmp := memory[CHUNK_POS_EXPIRES : CHUNK_POS_EXPIRES+4]
	_exp := binary.LittleEndian.Uint32(_tmp)
	if _exp > uint32(common.TSNow()) {
		return false
	}

	// if we're not reading anything from the chunk, remove it immediately!
	_tmp = memory[CHUNK_POS_READ_LOCKS : CHUNK_POS_READ_LOCKS+4]
	_read_lck := binary.LittleEndian.Uint32(_tmp)
	if _read_lck == 0 {
		this.chunks_free++
		this.chunks_free_list = append(this.chunks_free_list, chunk_id)
		memory[CHUNK_POS_STATE] = STATE_DELETED

		this.elx_expire[chunk_id] = 0
		return true
	}

	// otherwise if we have room on remove list - append it, and check later
	if len(this.chunks_cleanup_list) < cap(this.chunks_cleanup_list) {

		this.chunks_cleanup_list = append(this.chunks_cleanup_list, chunk_id)
		memory[CHUNK_POS_STATE] = STATE_SCHEDULED_FOR_DELETION

		this.elx_expire[chunk_id] = 0
		return true

	}

	// we can't remove now - do nothing!
	return false
}

func (this *slab) RunGC() {

	this.mu.Lock()
	defer this.mu.Unlock()

	write_pos := 0
	for _, chunk_id := range this.chunks_cleanup_list {

		memory := this.getChunkMemoryPtr(chunk_id)

		// we can remove only "scheduled for deletion"!
		// because we're skipping EXPIRE comparison
		if memory[CHUNK_POS_STATE] != STATE_SCHEDULED_FOR_DELETION {
			this.chunks_cleanup_list[write_pos] = chunk_id
			write_pos++
			continue
		}

		// ensure not read locked, we don't need to check expiration time as the item is already scheduled
		// for deletion!
		_tmp := memory[CHUNK_POS_READ_LOCKS : CHUNK_POS_READ_LOCKS+4]
		_read_lck := binary.LittleEndian.Uint32(_tmp)

		if _read_lck < 0 {
			panic("FATAL! Read lock <0 ...!")
		}

		if _read_lck > 0 {
			this.chunks_cleanup_list[write_pos] = chunk_id
			write_pos++
			continue
		}

		this.chunks_free++
		this.chunks_free_list = append(this.chunks_free_list, chunk_id)
		memory[CHUNK_POS_STATE] = STATE_DELETED

	}

	// rewrite the cleanup list!
	this.chunks_cleanup_list = this.chunks_cleanup_list[0:write_pos]
}
