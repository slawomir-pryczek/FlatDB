package ops

import (
	"gocached/slab"
	"hash/crc32"
	"sync/atomic"
)

func OpAdvancedGetKey(key string, getBuffer func(int) []byte) ([]byte, uint32, uint32) {

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
		return ret, item.CAS, item.Expires
	} else {
		atomic.AddUint64(&kvs.stat_atomic_get_nohit, 1)
	}

	return ret, 0, 0
}

const ADVI_INSERT_CAS_MISMATCH = int8(1)
const ADVI_INSERT_OK = int8(10)
const ADVI_DELETE_OK = int8(20)
const ADVI_INSERT_OUT_OF_MEMORY = int8(2)

// This function will return ADVI_INSERT_OK, CAS, and Expire if the item is inserted correctly
// And
func OpAdvancedInsert(key string, data []byte, expires uint32, cas *uint32, getBuffer func(int) []byte) (int8, *uint32, uint32, []byte) {

	cs := crc32.ChecksumIEEE([]byte(key))
	kvs_num := int(cs) % len(kvstores)

	kvstores[kvs_num].mu.Lock()
	defer kvstores[kvs_num].mu.Unlock()
	kvs := kvstores[kvs_num]

	item := getStoredItemUnsafe(kvs, key, true)

	can_replace := (cas == nil && item == nil) ||
		(cas == nil && !item.Exists()) ||
		(cas != nil && item.CAS == *cas && item.Exists())
	if can_replace {

		// if we're dumping the datastore - SAVE previous data for ATOMIC dump
		mode := atomic.LoadUint32(&is_saving_cas_threshold)
		if item != nil && mode > 0 && mode > item.CAS {
			preserveItem(key, item)
		}
		// <<

		// item delete op, return ADVI_INSERT_OK and NULLs
		if data == nil {
			if item, exists := kvstores[kvs_num].key_map[key]; exists {
				delete(kvstores[kvs_num].key_map, key)
				item.Delete()
			}
			if item, exists := kvstores[kvs_num].key_map_old[key]; exists {
				//delete(kvstores[kvs_num].key_map_old, key) ... we cannot do any writes to key_map_old
				item.Delete()
			}

			return ADVI_DELETE_OK, nil, 0, nil
		}

		//

		// we can store new item, return its expiry time and CAS
		tmp := slab.Store(data, expires)
		if tmp != nil {
			kvs.key_map[key] = *tmp

			kvs.stat_replace++
			if item != nil {
				item.Delete()
			}
			return ADVI_INSERT_OK, &tmp.CAS, tmp.Expires, nil
		}
		//

		// out of memory, we can return item's value!
		if item != nil && getBuffer != nil {
			ret := item.GetUsingBuffer(getBuffer)
			if ret != nil {
				return ADVI_INSERT_OUT_OF_MEMORY, &item.CAS, item.Expires, ret
			}
		}
		//

		// out of memory, no item to return
		return ADVI_INSERT_OUT_OF_MEMORY, nil, 0, nil
	}

	// item cannot be inserted, return item's data, expire, CAS
	if item != nil && getBuffer != nil {
		ret := item.GetUsingBuffer(getBuffer)
		if ret != nil {
			return ADVI_INSERT_CAS_MISMATCH, &item.CAS, item.Expires, ret
		}
	}

	// item cannot be inserted, we have no previous item
	return ADVI_INSERT_CAS_MISMATCH, nil, 0, nil
}
