package ops

import (
	"fmt"
	"gocached/slab"
	"handler_socket2"
	"time"
)

func OpRebalance(use_settings string) string {

	ret := "---Rebalance:\n"

	global_op_mutex.Lock()
	defer global_op_mutex.Unlock()

	tomove := slab.MoveSlabsStart(use_settings)

	for slab_class_no, chunks := range tomove {
		if len(chunks) == 0 {
			ret += fmt.Sprintf("Slab Class #%.2d: - \n", slab_class_no)
			continue
		}

		ts := time.Now().UnixNano() / 1000
		i_moved, i_failed := ___domove(slab_class_no, chunks)
		took := (time.Now().UnixNano() / 1000) - ts

		slab.MoveSlabsStat(uint16(slab_class_no), len(chunks), uint64(took), i_moved, i_failed)

		_tmp := fmt.Sprintf("Slab Class #%.2d: Moving %d chunks (items moved: ok %d, failed %d) - took: %.2fms\n",
			slab_class_no, len(chunks), i_moved, i_failed, float64(took)/1000.0)
		ret += _tmp
		fmt.Print(_tmp)
	}

	slab.MoveSlabsFinish(tomove)

	_, comment := slab.GetRebalanceSettings(use_settings)
	ret += "\n\n---Config:\n" + comment

	return "<pre>" + ret + "</pre>"
}

func init() {

	go func() {

		time.Sleep(1 * time.Second)
		rebalance_time := handler_socket2.Config.GetI("REBALANCE_TIME", 60)

		if rebalance_time == 0 {
			fmt.Println("Auto rebalancer is disabled")
			return
		}
		fmt.Println("Rebalance time: ", rebalance_time, "seconds")

		for {
			OpRebalance("")
			time.Sleep(time.Duration(rebalance_time) * time.Second)
		}

	}()

}

func ___domove(slab_class_no int, chunks []int) (int, int) {

	ret, ret2 := 0, 0

	for kvstore_num := 0; kvstore_num < len(kvstores); kvstore_num++ {

		all_keys := make(map[string]uint16, 0)

		slab_move_min := uint16(chunks[len(chunks)-1])
		slab_move_max := uint16(chunks[0])

		kvstores[kvstore_num].mu.RLock()
		kvs := kvstores[kvstore_num]
		for k, v := range kvs.key_map {
			if v.InSlabBetween(uint8(slab_class_no), slab_move_min, slab_move_max) {
				all_keys[k] = v.GetSlabNo()
			}
		}

		for k, v := range kvs.key_map_old {
			if v.InSlabBetween(uint8(slab_class_no), slab_move_min, slab_move_max) {

				if slab_no, exists := all_keys[k]; !exists || v.GetSlabNo() > slab_no {
					all_keys[k] = v.GetSlabNo()
				}
			}
		}
		//fmt.Println("X")
		kvstores[kvstore_num].mu.RUnlock()
		moved, failed := ___domove_keys(all_keys, kvstore_num)
		ret += moved
		ret2 += failed
	}

	/*cs := crc32.ChecksumIEEE([]byte(key))
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
	}*/

	return ret, ret2
}

func ___domove_keys(all_keys map[string]uint16, kvs_num int) (int, int) {

	ret, ret2 := 0, 0

	kvstores[kvs_num].mu.Lock()
	kvs := kvstores[kvs_num]
	for key, _ := range all_keys {

		item := getStoredItemUnsafe(kvs, key, false)
		if item == nil {

			if i, exists := kvs.key_map[key]; exists {
				i.Delete()
			}
			if i, exists := kvs.key_map_old[key]; exists {
				i.Delete()
			}
			continue
		}

		data := item.Get()
		if i, exists := kvs.key_map[key]; exists {
			i.Delete()
		}
		if i, exists := kvs.key_map_old[key]; exists {
			i.Delete()
		}

		if data != nil {

			tmp := slab.Store(data, item.Expires)

			//fmt.Print("FROM: ", item.GetSlabNo())

			if tmp != nil {
				if !tmp.ReplaceCAS(tmp.CAS, item.CAS) {

					// cannot re-set the CAS, so this item expired!
					// just delete references from the hash map!
					delete(kvs.key_map, key)
					delete(kvs.key_map_old, key)
					continue

				}
			}

			//fmt.Println(">>", tmp.GetSlabNo())
			// we need to preserve CAS
			if tmp != nil {
				tmp.CAS = item.CAS
				kvs.key_map[key] = *tmp

				ret++
			} else {
				ret2++
				fmt.Println("Error moving item!")
			}
			delete(kvs.key_map_old, key)
		}

	}

	kvstores[kvs_num].mu.Unlock()

	return ret, ret2
}
