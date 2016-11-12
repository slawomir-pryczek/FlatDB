package ops

import (
	"gocached/common"
	"gocached/slab"
	"time"
)

func init() {

	i := 0
	go func() {

		for {

			time.Sleep(1 * time.Second)
			runGC(i)

			i++
			if i >= len(kvstores) {
				i = 0
			}
		}

	}()

}

func runGC(kvstore_num int) {

	global_op_mutex.Lock()
	defer global_op_mutex.Unlock()

	kvstores[kvstore_num].mu.Lock()
	kvs := kvstores[kvstore_num]
	kvs.key_map, kvs.key_map_old = kvs.key_map_old, kvs.key_map
	kvstores[kvstore_num].mu.Unlock()

	// we can only read from key_map_old
	truncated_map := make(map[string]slab.StoredItem)

	now := uint32(common.TSNow())
	gc_kept := 0
	gc_collected := 0
	for k, v := range kvs.key_map_old {

		if v.Expires >= now {
			truncated_map[k] = v
			gc_kept++
		} else {
			gc_collected++
		}

	}

	kvs.mu.Lock()
	kvs.stat_gc_kept += gc_kept
	kvs.stat_gc_collected += gc_collected
	kvs.stat_gc_passes++
	kvs.key_map_old = truncated_map
	kvs.mu.Unlock()
}
