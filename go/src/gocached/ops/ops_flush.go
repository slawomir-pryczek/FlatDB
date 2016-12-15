package ops

import (
	"fmt"
	"gocached/common"
	"gocached/slab"
	"time"
)

func OpFlush(replicate bool) string {

	global_op_mutex.Lock()
	defer global_op_mutex.Unlock()

	now := uint32(common.TSNow())

	stat_deleted := 0
	stat_ts_started := time.Now().UnixNano() / 1000

	do_cleanup := make([]map[string]slab.StoredItem, 0, len(kvstores)*2)

	// lock all hash maps, so we can atomically get point in time, which will be CAS
	for i := 0; i < len(kvstores); i++ {
		kvstores[i].mu.Lock()
	}

	for i := 0; i < len(kvstores); i++ {

		// we can replace key maps
		do_cleanup = append(do_cleanup, kvstores[i].key_map, kvstores[i].key_map_old)
		kvstores[i].key_map = make(map[string]slab.StoredItem)
		kvstores[i].key_map_old = make(map[string]slab.StoredItem)
		if replicate && kvstores[i].sync_key != nil { // flush - we discard all previous data!

			// add flush op, remove all pending sync tasks!
			if i == 0 {
				kvstores[i].sync_key = map[string]byte{"___387234927423_flush_op_required_3298738971": SYNC_FLUSH}
			} else {
				kvstores[i].sync_key = map[string]byte{}
			}
		}
		kvstores[i].mu.Unlock()
	}

	// we need to remove all items from slab that have not expired yet,
	// so we won't loose ability to re-balance and won't de-sync hash and slab list
	for _, keymap := range do_cleanup {
		for _, v := range keymap {
			if v.Expires >= now {
				v.Delete()
				stat_deleted++
			}
		}
	}

	took := (time.Now().UnixNano() / 1000) - stat_ts_started
	ret := "Flush operation finished<br>"
	ret += fmt.Sprintf("Items Removed: %d - took: %.2fms\n",
		stat_deleted, float64(took)/1000.0)

	return ret
}
