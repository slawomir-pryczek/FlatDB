package ops

import (
	"fmt"
	"handler_socket2"
	"handler_socket2/byteslabs"
	"hash/crc32"
	"os"
	"replication"
	"strconv"
	"strings"
	"time"
)

func HashMapForceResync(host string, port int) int {
	return replication.HashMapForceResync(host, port)
}

// Create new replica on list, also create sync_key map so
// items can be synchronized if there's at least one
func AddReplica(host string, port int) {

	if replication.AddReplica(host, port) != 1 {
		return
	}

	for i := 0; i < len(kvstores); i++ {
		kvstores[i].mu.Lock()
		kvs := kvstores[i]

		// create sync map
		if kvs.sync_key == nil {
			kvs.sync_key = make(map[string]byte)
		}
		kvstores[i].mu.Unlock()
	}
}

// Remove replica from list, also set sync_key to nil if we have no
// replicas running
func RemoveReplica(host string, port int) {

	if replication.RemoveReplica(host, port) == 0 {
		return
	}

	for i := 0; i < len(kvstores); i++ {
		kvstores[i].mu.Lock()
		kvstores[i].sync_key = nil
		kvstores[i].mu.Unlock()
	}
}

func do_sync_task(task_id int) {

	_buffer_size := 1024 * 64
	_allocator := byteslabs.MakeAllocator()
	_buffer := _allocator.Allocate(_buffer_size)
	_getbuffer := func(size int) []byte {

		if size > _buffer_size {
			_buffer_size = size
			_allocator.Release()
			_buffer = _allocator.Allocate(size + 1024*32)
		}
		return _buffer[:0]
	}

	_getkey := func(key string, get_data bool) (uint32, uint32, []byte) {

		cs := crc32.ChecksumIEEE([]byte(key))
		kvs_num := int(cs) % len(kvstores)

		kvstores[kvs_num].mu.RLock()
		kvs := kvstores[kvs_num]

		item := getStoredItemUnsafe(kvs, key, false)
		if item == nil {
			kvstores[kvs_num].mu.RUnlock()
			return 0, 0, nil
		}

		if !get_data {
			_ex := item.Exists()
			kvstores[kvs_num].mu.RUnlock()
			if _ex {
				return item.Expires, item.CAS, nil
			}
			return 0, 0, nil
		}

		ret := item.GetUsingBuffer(_getbuffer)
		kvstores[kvs_num].mu.RUnlock()

		if ret != nil {
			return item.Expires, item.CAS, ret
		}
		return 0, 0, nil
	}

	// get all items, and add them to sync task
	type sync_item struct {
		key string
		op  byte
	}

	// get sync structure for single point in time (as much as possible)
	__kvs := make([]map[string]byte, 0)
	for i := 0; i < len(kvstores); i++ {
		kvstores[i].mu.Lock()
		__kvs = append(__kvs, kvstores[i].sync_key)
		if kvstores[i].sync_key != nil {
			kvstores[i].sync_key = make(map[string]byte, 0)
		}
		kvstores[i].mu.Unlock()
	}

	// get each item (KEY AND OPERATION)
	// item_count = number of items to process initially
	// _tmp structure with items
	// _tmp_pos processing position for round robin processing
	item_count := 0
	_tmp := make([][]sync_item, 0)
	for i := 0; i < len(__kvs); i++ {

		if __kvs[i] == nil {
			_tmp = append(_tmp, make([]sync_item, 0))
			continue
		}

		_tmp_slab := make([]sync_item, 0, len(__kvs[i]))
		for key, op := range __kvs[i] {
			_tmp_slab = append(_tmp_slab, sync_item{key, op})
		}
		_tmp = append(_tmp, _tmp_slab)
		item_count += len(_tmp_slab)
	}
	_tmp_pos := make([]int, len(_tmp))

	// Begin the task!
	// Include CAS, only for sorting (locking op, do that separately!)
	sync_task := replication.NewReplicationTask(task_id, item_count, _getkey)

	added := true
	for added {

		added = false
		for hash_bucket_no := 0; hash_bucket_no < len(_tmp_pos); hash_bucket_no++ {

			// get info of 100 items at once, and go round robin so we're not locking individual slab classes for too long
			_t := _tmp[hash_bucket_no]
			if _t == nil {
				continue
			}

			kvstores[hash_bucket_no].mu.RLock()
			kvs := kvstores[hash_bucket_no]

			i := _tmp_pos[hash_bucket_no]
			for ; i < len(_t); i++ {

				added = true
				switch {
				case _t[i].op == replication.SYNC_KEY_DELETE:
					sync_task.AddItem(_t[i].key, 1, _t[i].op)
				case _t[i].op == replication.SYNC_FLUSH:
					sync_task.AddItem(_t[i].key, 0, _t[i].op)
				default:
					item := getStoredItemUnsafe(kvs, _t[i].key, false)
					if item != nil {
						sync_task.AddItem(_t[i].key, item.CAS, _t[i].op)
					}
				}

				if i%100 == 99 {
					break
				}
			}
			_tmp_pos[hash_bucket_no] = i + 1 // if we break then we start from next item

			kvstores[hash_bucket_no].mu.RUnlock()

		}

	}

	sync_task.Process()
	_allocator.Release()

}

const SYNC_KEY_DELETE = 100
const SYNC_KEY_SET = 101
const SYNC_KEY_TOUCH = 102
const SYNC_FLUSH = 200

const REPLICATION_MODE_SAFE = 2
const REPLICATION_MODE_OPTIMISTIC = 200

var replication_mode = REPLICATION_MODE_OPTIMISTIC

// replication support for touch, only touch if no operation is already there, cases:
// A. if item is previously deleted touch would have no effect
// B. and if item is set previously, we want to leave SET operator, so
// we can set item value and expiry time using single operation
func _replication_touch(kvs *kvstore, key string) {

	if kvs.sync_key == nil {
		return
	}

	curr_op, exists := kvs.sync_key[key]
	if curr_op == SYNC_KEY_DELETE {
		return
	}

	if replication_mode == REPLICATION_MODE_SAFE {

		// touch-by-set
		kvs.sync_key[key] = SYNC_KEY_SET

	} else {

		// touch only if we don't have any OP in current time span,
		// eg. if there's SET already, use SET!
		if !exists || curr_op == SYNC_KEY_TOUCH {
			kvs.sync_key[key] = SYNC_KEY_TOUCH
		} else {
			kvs.sync_key[key] = SYNC_KEY_SET
		}
	}

}

// replication support for delete, process normally
func _replication_delete(kvs *kvstore, key string) {

	if kvs.sync_key == nil {
		return
	}
	kvs.sync_key[key] = SYNC_KEY_DELETE
}

// replication support for set, process normally
func _replication_set(kvs *kvstore, key string) {

	if kvs.sync_key == nil {
		return
	}
	kvs.sync_key[key] = SYNC_KEY_SET
}

func init() {

	_replicas_config := handler_socket2.Config.Get("SLAVE", "")
	_rmode := handler_socket2.Config.Get("REPLICATION_MODE", "")
	switch {
	case _rmode == "OPTIMISTIC":
		replication_mode = REPLICATION_MODE_OPTIMISTIC
	case _rmode == "SAFE":
		replication_mode = REPLICATION_MODE_SAFE
	default:
		fmt.Fprintf(os.Stderr, "WARNING: Incorrect replication mode, reverting to OPTIMISTIC (default)")
		_rmode = "OPTIMISTIC"
	}

	if len(_replicas_config) > 0 {
		for _, replica := range strings.Split(_replicas_config, ",") {
			replica = strings.Trim(replica, "\r\t\n ")

			_host_port := strings.Split(replica, ":")
			_host_port[0] = strings.Trim(_host_port[0], "\r\t\n ")

			if len(_host_port) != 2 {

				if len(_host_port) == 1 && _host_port[0] == "/dev/null" {
					AddReplica("/dev/null", -1)
					continue
				}

				fmt.Fprintf(os.Stderr, "WARNING: Failed to add replica (host:port) %s\n", replica)
				continue
			}

			port, err := strconv.Atoi(strings.Trim(_host_port[1], "\r\t\n "))
			if err == nil {
				AddReplica(_host_port[0], port)
			}
		}
	}

	go func() {
		for i := 0; ; i++ {
			time.Sleep(100 * time.Millisecond)
			do_sync_task(i)
		}
	}()

	handler_socket2.StatusPluginRegister(func() (string, string) {
		return "Replication Plugin", replication.GetStatus(_rmode)
	})
}
