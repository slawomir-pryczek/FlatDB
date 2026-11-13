package ops

import (
	"fmt"
	"gocached/common"
	"gocached/slab"
	"hash/crc32"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

type kvstore struct {
	key_map     map[string]slab.StoredItem
	key_map_old map[string]slab.StoredItem
	sync_key    map[string]byte

	stat_gc_passes    int
	stat_gc_kept      int
	stat_gc_collected int

	stat_delete    int
	stat_increment int
	stat_decrement int
	stat_set       int
	stat_add       int
	stat_replace   int

	stat_atomic_get_hit   uint64
	stat_atomic_get_nohit uint64

	mu sync.RWMutex
}

/*
func (this *kvstore) GetCurrMap() map[string]slab.StoredItem {

	if this.curr_km%2 == 0 {
		return this.key_map
	}
	return this.key_map2
}

func (this *kvstore) GetOldMap() map[string]slab.StoredItem {

	if this.curr_km%2 == 0 {
		return this.key_map2
	}
	return this.key_map
}*/

var kvstores []*kvstore

func init() {

	kvstores = make([]*kvstore, runtime.NumCPU()*4)
	for i := 0; i < len(kvstores); i++ {

		tmp := kvstore{}
		tmp.key_map = make(map[string]slab.StoredItem)
		tmp.key_map_old = make(map[string]slab.StoredItem)

		kvstores[i] = &tmp

	}
	fmt.Println(ReadFromFiles())
}

func getStoredItemUnsafe(kvs *kvstore, key string, can_move_item bool) *slab.StoredItem {

	var ret *slab.StoredItem = nil

	if item, exists := kvs.key_map[key]; exists {
		ret = &item
	}

	if item, exists := kvs.key_map_old[key]; exists {

		cas := uint32(0)
		if ret != nil {
			cas = ret.CAS
		}

		if item.CAS > cas || cas-item.CAS > 0x7FFFFFFF /*maybe CAS rolled over*/ {
			ret = &item
		}
	}

	return ret
}

func OpExists(key string) bool {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.RLock()
	kvs := kvstores[kvs_num]
	item := getStoredItemUnsafe(kvs, key, false)

	exists := item.Exists()
	kvstores[kvs_num].mu.RUnlock()

	return exists
}

func OpGetExpires(key string) uint32 {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.RLock()
	kvs := kvstores[kvs_num]
	item := getStoredItemUnsafe(kvs, key, false)

	ret := uint32(0)
	if item != nil && item.Exists() {
		ret = item.Expires
	}
	kvstores[kvs_num].mu.RUnlock()

	return ret
}

func OpGetCAS(key string) uint32 {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.RLock()
	kvs := kvstores[kvs_num]
	item := getStoredItemUnsafe(kvs, key, false)

	ret := uint32(0)
	if item != nil && item.Exists() {
		ret = item.CAS
	}
	kvstores[kvs_num].mu.RUnlock()

	return ret
}

func OpDelete(key string) bool {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.Lock()
	kvs := kvstores[kvs_num]
	item := getStoredItemUnsafe(kvs, key, false)

	if item == nil {
		kvstores[kvs_num].mu.Unlock()
		return false
	}
	existed := item.Exists()

	if item != nil {

		// if we're dumping the datastore - SAVE previous data for ATOMIC dump
		mode := atomic.LoadUint32(&is_saving_cas_threshold)
		if mode > 0 && mode > item.CAS {
			preserveItem(key, item)
		}
		// <<

		if item, exists := kvstores[kvs_num].key_map[key]; exists {
			delete(kvstores[kvs_num].key_map, key)
			item.Delete()
		}
		if item, exists := kvstores[kvs_num].key_map_old[key]; exists {
			//delete(kvstores[kvs_num].key_map_old, key) ... we cannot do any writes to key_map_old
			item.Delete()
		}

		// replication support
		if true || existed {
			_replication_delete(kvs, key)
		}
	}

	kvs.stat_delete++
	kvstores[kvs_num].mu.Unlock()

	return existed
}

func OpSetKey(key string, data []byte, expires uint32) bool {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.Lock()
	kvs := kvstores[kvs_num]

	item := getStoredItemUnsafe(kvs, key, true)
	if item != nil {

		// if we're dumping the datastore - SAVE previous data for ATOMIC dump
		mode := atomic.LoadUint32(&is_saving_cas_threshold)
		if mode > 0 && mode > item.CAS {
			preserveItem(key, item)
		}
		// <<

		// need to delete previous item from SLAB, because if new TTL is lower than OLD
		// then hash map could keep pointing to ip ( because we're using CURRENT-OLD hashmaps)
		item.Delete()
	}

	tmp := slab.Store(data, expires)
	if tmp != nil {
		kvs.key_map[key] = *tmp

		// replication support
		_replication_set(kvs, key)
	}
	kvs.stat_set++

	kvstores[kvs_num].mu.Unlock()

	return tmp != nil
}

func OpGetKey(key string, getBuffer func(int) []byte) []byte {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)
	var ret []byte = nil

	kvstores[kvs_num].mu.RLock()
	kvs := kvstores[kvs_num]

	item := getStoredItemUnsafe(kvs, key, false)
	if item != nil {
		ret = item.GetUsingBuffer(getBuffer)
	}
	kvstores[kvs_num].mu.RUnlock()

	if ret != nil {
		atomic.AddUint64(&kvs.stat_atomic_get_hit, 1)
	} else {
		atomic.AddUint64(&kvs.stat_atomic_get_nohit, 1)
	}

	return ret
}

func OpAddKey(key string, data []byte, expires uint32) bool {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.Lock()
	kvs := kvstores[kvs_num]

	item := getStoredItemUnsafe(kvs, key, true)

	can_add := (item == nil || !item.Exists())
	if can_add {
		tmp := slab.Store(data, expires)
		if tmp != nil {
			kvs.key_map[key] = *tmp

			// replication support
			_replication_set(kvs, key)
		} else {
			can_add = false
		}

		kvs.stat_add++
	}
	kvstores[kvs_num].mu.Unlock()

	return can_add
}

func OpReplaceKey(key string, data []byte, expires uint32) bool {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.Lock()
	kvs := kvstores[kvs_num]

	item := getStoredItemUnsafe(kvs, key, true)

	can_replace := item != nil && item.Exists()
	if can_replace {

		// if we're dumping the datastore - SAVE previous data for ATOMIC dump
		mode := atomic.LoadUint32(&is_saving_cas_threshold)
		if mode > 0 && mode > item.CAS {
			preserveItem(key, item)
		}
		// <<

		tmp := slab.Store(data, expires)
		if tmp != nil {
			kvs.key_map[key] = *tmp

			// replication support
			_replication_set(kvs, key)
		} else {
			can_replace = false
		}

		kvs.stat_replace++
		item.Delete()
	}
	kvstores[kvs_num].mu.Unlock()

	return can_replace
}

func OpArithmetics(key string, data []byte, increment bool, expires *uint32) []byte {

	if !common.IsNumber(data) {
		return nil
	}

	inc_by := int64(0)
	if v, err := strconv.ParseInt(string(data), 10, 64); err == nil {
		inc_by = v
	} else {
		return nil
	}

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.Lock()
	kvs := kvstores[kvs_num]

	item := getStoredItemUnsafe(kvs, key, true)
	if item == nil {
		kvstores[kvs_num].mu.Unlock()
		return nil
	}

	// if we're dumping the datastore - SAVE previous data for ATOMIC dump
	mode := atomic.LoadUint32(&is_saving_cas_threshold)
	if mode > 0 && mode > item.CAS {
		if val := item.Get(); val != nil {
			preserveItemR(key, val, item.Expires)
		}
	}
	// <<

	if !increment {
		inc_by = -inc_by
		kvs.stat_decrement++
	} else {
		kvs.stat_increment++
	}

	_exp := item.Expires
	if expires != nil {
		_exp = *expires
	}

	updated_item, value := item.Increment(inc_by, _exp)
	if updated_item == nil {
		kvstores[kvs_num].mu.Unlock()
		return nil
	}

	kvs.key_map[key] = *updated_item

	// replication support
	_replication_set(kvs, key)

	kvstores[kvs_num].mu.Unlock()
	return value
}

func OpTouch___NoCasUpdate(key string, expires uint32, cas *uint32) bool {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.Lock()
	defer kvstores[kvs_num].mu.Unlock()

	kvs := kvstores[kvs_num]
	item := getStoredItemUnsafe(kvs, key, false)

	if item == nil || (cas != nil && *cas != item.CAS) {
		return false
	}

	if ok := item.Touch___NoCasUpdate(expires); ok {
		item.Expires = expires
		kvs.key_map[key] = *item
		_replication_touch(kvs, key)

		return true
	}

	return false

}

func OpTouch(key string, expires uint32, in_out_cas *uint32) bool {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.Lock()
	defer kvstores[kvs_num].mu.Unlock()

	kvs := kvstores[kvs_num]
	item := getStoredItemUnsafe(kvs, key, false)
	if item == nil || (in_out_cas != nil && *in_out_cas != item.CAS) {
		return false
	}

	// if we're dumping the datastore - SAVE previous data for ATOMIC dump,
	// then read the value and save it again!
	/* mode := atomic.LoadUint32(&is_saving_cas_threshold)

	if mode > 0 && mode > item.CAS {

		data := item.Get()
		if data == nil {
			return false
		}

		// delete old item!
		expires := item.Expires
		preserveItemR(key, data, item.Expires)

		kvs.stat_set++
		tmp := slab.Store(data, expires)
		if tmp != nil {
			item.Delete()
			kvs.key_map[key] = *tmp

			return true
		}

		return false
	} */
	// <<

	if ok, new_cas := item.Touch(expires); ok {

		mode := atomic.LoadUint32(&is_saving_cas_threshold)
		if mode > 0 && mode > item.CAS {
			data := item.Get()
			preserveItemR(key, data, item.Expires)
		}

		item.Expires = expires
		item.CAS = new_cas
		if in_out_cas != nil {
			*in_out_cas = new_cas
		}

		kvs.key_map[key] = *item
		_replication_touch(kvs, key)

		return true
	}

	return false
}
