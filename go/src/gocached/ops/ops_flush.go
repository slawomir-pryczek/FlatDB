package ops

import (
	"fmt"
	"gocached/common"
	"gocached/slab"
	"time"
)

func OpFlush() string {

	global_op_mutex.Lock()
	defer global_op_mutex.Unlock()

	now := uint32(common.TSNow())
	_to_delete := make([]slab.StoredItem, 0, 1000)

	stat_deleted := 0
	stat_ts_started := time.Now().UnixNano() / 1000

	for i := 0; i < len(kvstores); i++ {
		kvstores[i].mu.Lock()

		// we can replace key maps
		_si_delete1 := kvstores[i].key_map
		_si_delete2 := kvstores[i].key_map_old
		kvstores[i].key_map = make(map[string]slab.StoredItem)
		kvstores[i].key_map_old = make(map[string]slab.StoredItem)
		kvstores[i].mu.Unlock()

		// we need to remove all items from slab that have not expired yet,
		// so we won't loose ability to re-balance and won't de-sync hash and slab list
		for _, v := range _si_delete1 {
			if v.Expires >= now {
				_to_delete = append(_to_delete, v)
			}
		}
		for _, v := range _si_delete2 {
			if v.Expires >= now {
				_to_delete = append(_to_delete, v)
			}
		}
	}

	for _, v := range _to_delete {
		v.Delete()
		stat_deleted++
	}

	took := (time.Now().UnixNano() / 1000) - stat_ts_started
	ret := "Flush operation finished<br>"
	ret += fmt.Sprintf("Items Removed: %d - took: %.2fms\n",
		stat_deleted, float64(took)/1000.0)

	return ret
}
