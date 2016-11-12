package slab

import (
	"encoding/binary"
	"gocached/common"
)

type chunk struct {
	chunk_memory []byte
	slab_ptr     *slab
	read_locked  bool
}

func (this *slab) GetChunkForReadC(chunk_id uint16, with_cas uint32) *chunk {

	chunk := this.GetChunkForRead(chunk_id)
	if chunk == nil {
		return nil
	}

	_mem_cas := chunk.chunk_memory[CHUNK_POS_CAS : CHUNK_POS_CAS+4]
	cas := binary.LittleEndian.Uint32(_mem_cas)

	if with_cas != cas {

		// bad CAS, we need to release read lock IMMEDIATELY, as
		// we won't be reading anything!
		chunk.Release()
		return nil
	}

	return chunk
}

// get chunk
func (this *slab) GetChunkForRead(chunk_id uint16) *chunk {

	this.mu.Lock()
	memory := this.getChunkMemoryPtr(chunk_id)

	// we can only process chunk that's in CAN-USE state
	if memory[CHUNK_POS_STATE] != STATE_CAN_USE {
		this.mu.Unlock()
		return nil
	}

	// do we hold any read - locks?
	_mem_rl := memory[CHUNK_POS_READ_LOCKS : CHUNK_POS_READ_LOCKS+4]
	_expires := binary.LittleEndian.Uint32(memory[CHUNK_POS_EXPIRES : CHUNK_POS_EXPIRES+4])

	rlocks := binary.LittleEndian.Uint32(_mem_rl)
	expired := (uint32(common.TSNow()) > _expires)

	// expired and we're not reading... safe to delete!
	if expired && rlocks == 0 {
		this.CollectChunkUnsafe(chunk_id)
		this.mu.Unlock()
		return nil
	}

	// expired but we're reading - mark as scheduled for deletion!
	if expired && rlocks > 0 {
		memory[CHUNK_POS_STATE] = STATE_SCHEDULED_FOR_DELETION
		this.mu.Unlock()
		return nil
	}

	// ok, we can read this item! Increase read locks held
	binary.LittleEndian.PutUint32(_mem_rl, rlocks+1)
	this.mu.Unlock()
	return &chunk{memory, this, true}
}

func (this *chunk) Release() {

	this.slab_ptr.mu.Lock()

	if !this.read_locked {
		panic("Releasing already released chunk!")
	}
	this.read_locked = false

	memory := this.chunk_memory
	if memory[CHUNK_POS_STATE] == STATE_CAN_USE || memory[CHUNK_POS_STATE] == STATE_SCHEDULED_FOR_DELETION {

		// decrease read lock
		_tmp := memory[CHUNK_POS_READ_LOCKS : CHUNK_POS_READ_LOCKS+4]
		_read_lck := binary.LittleEndian.Uint32(_tmp) - 1
		binary.LittleEndian.PutUint32(_tmp, _read_lck)
	}

	this.slab_ptr.mu.Unlock()
}

func (this *chunk) ReadValue() []byte {

	if !this.read_locked {
		panic("Reading from released chunk")
	}

	datasize := int(binary.LittleEndian.Uint32(this.chunk_memory[CHUNK_POS_DATASIZE : CHUNK_POS_DATASIZE+4]))
	return this.chunk_memory[CHUNK_POS_USERDATA : CHUNK_POS_USERDATA+datasize]
}

func (this *slab) ReadValueUnsafeC(chunk_id uint16, with_cas uint32) []byte {

	memory := this.getChunkMemoryPtr(chunk_id)

	// we can only process chunk that's in CAN-USE state
	if memory[CHUNK_POS_STATE] != STATE_CAN_USE {
		return nil
	}

	_mem_cas := memory[CHUNK_POS_CAS : CHUNK_POS_CAS+4]
	cas := binary.LittleEndian.Uint32(_mem_cas)
	if cas != with_cas {
		return nil
	}

	// do we hold any read - locks?
	_mem_rl := memory[CHUNK_POS_READ_LOCKS : CHUNK_POS_READ_LOCKS+4]
	_expires := binary.LittleEndian.Uint32(memory[CHUNK_POS_EXPIRES : CHUNK_POS_EXPIRES+4])

	rlocks := binary.LittleEndian.Uint32(_mem_rl)
	expired := (uint32(common.TSNow()) > _expires)

	// expired and we're not reading... safe to delete!
	if expired && rlocks == 0 {
		this.CollectChunkUnsafe(chunk_id)
		return nil
	}

	// expired but we're reading - mark as scheduled for deletion!
	if expired && rlocks > 0 {
		memory[CHUNK_POS_STATE] = STATE_SCHEDULED_FOR_DELETION
		return nil
	}

	datasize := int(binary.LittleEndian.Uint32(memory[CHUNK_POS_DATASIZE : CHUNK_POS_DATASIZE+4]))
	return memory[CHUNK_POS_USERDATA : CHUNK_POS_USERDATA+datasize]
}
