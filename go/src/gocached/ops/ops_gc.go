package ops

import (
	"fmt"
	"gocached/common"
	"gocached/slab"
	"replication"
	"time"
)

func init() {
	go func() {
		for {
			RunHashGC(1000)
		}
	}()
}

func RunHashGC(sleep_ms time.Duration) string {

	ret := ""
	for i := 0; i < len(kvstores); i++ {
		if sleep_ms > 0 {
			time.Sleep(sleep_ms * time.Millisecond)
		}
		ts := time.Now().UnixNano()
		gc_kept, gc_collected := runHashGCPartial(i)

		took := int((time.Now().UnixNano() - ts) / 1000)
		ret += fmt.Sprintf("Hash #%d, rebalance took %.2fms ... kept:%d collected:%d\n",
			i, float64(took)/1000, gc_kept, gc_collected)
	}

	return ret
}

func runHashGCPartial(kvstore_num int) (int, int) {

	replication_should_sync := replication.HashMapShouldSynchronize(kvstore_num)

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

			// send all items, if replica is freshly added
			if replication_should_sync {

				if v.Exists() {
					kvstores[kvstore_num].mu.Lock()
					_replication_set(kvs, k)
					kvstores[kvstore_num].mu.Unlock()
				}

			}

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

	return gc_kept, gc_collected
}
