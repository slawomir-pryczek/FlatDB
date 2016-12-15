package replica

import (
	"fmt"
	"net"
	"replication/sync_task"
	"sync"
	"time"
)

var mutex_replica sync.Mutex

type replica struct {
	host string
	port int

	mu                sync.Mutex
	sync_tasks        []*sync_task.Sync_task
	sync_task_sending *sync_task.Sync_task

	stat_key_set            int
	stat_key_touch          int
	stat_key_delete         int
	stat_flush              int
	stat_skipped_key_set    int
	stat_skipped_key_touch  int
	stat_skipped_key_delete int
	stat_skipped_flush      int
	stat_failed_key_set     int
	stat_failed_key_touch   int
	stat_failed_key_delete  int
	stat_failed_flush       int

	stat_sends_since_last_recync   int
	stat_retries_since_last_resync int
	stat_errors_since_last_resync  int
	stat_skips_since_last_resync   int

	stat_sends        int
	stat_skips        int
	stat_send_fail    int
	stat_send_retried int

	stat_bytes_sent    int
	stat_bytes_pending int
	stat_bytes_skipped int
	stat_bytes_failed  int

	stat_bytes_sent_uncompressed int64
	last_send_skipped_timestamp  int64
	last_send_failed_timestamp   int64

	conn                 net.Conn
	last_send_ts         int
	stat_conn_number     int
	stat_no_data_to_sent int

	hashmap_synchronization_progress map[int]int
}

// list of all replicas
var replicas []*replica = make([]*replica, 0)

// add replica and start task to dispatch replica's data
func AddReplica(host string, port int) int {

	fmt.Println("Replica added... ", host, " port: ", port)

	mutex_replica.Lock()
	new_replica := &replica{host: host, port: port, sync_tasks: make([]*sync_task.Sync_task, 0), conn: nil,
		hashmap_synchronization_progress: make(map[int]int, 0)}
	replicas = append(replicas, new_replica)
	ret := len(replicas)
	mutex_replica.Unlock()

	go func(new_replica *replica) {

		for {
			time.Sleep(20 * time.Millisecond)

			new_replica.mu.Lock()
			if new_replica.sync_tasks == nil {

				new_replica._connect(false) // if we're removing replica, close connection
				new_replica.mu.Unlock()
				break
			}
			if len(new_replica.sync_tasks) <= 0 {

				new_replica.stat_no_data_to_sent++
				new_replica._connect_clean() // if there's nothing to do, try closing old connection
				new_replica.mu.Unlock()
				continue
			}

			// send first task, and remove it from the list
			_tmp := make([]*sync_task.Sync_task, 0, len(new_replica.sync_tasks)-1)
			var _tosend *sync_task.Sync_task = nil
			for k, v := range new_replica.sync_tasks {
				if k == 0 {
					_tosend = v
				} else {
					_tmp = append(_tmp, v)
				}
			}
			new_replica.sync_tasks = _tmp
			new_replica.sync_task_sending = _tosend
			new_replica.mu.Unlock()

			// send all ppackets
			var sent_ok bool
			for i := 0; i < 10; i++ {

				sent_ok = true
				for _, data_packet := range _tosend.Data_Packets {

					if new_replica.port == -1 {
						continue
					}

					new_replica._connect(true)
					sent_ok = sent_ok && sendData(new_replica.conn, data_packet)
					if !sent_ok {
						break
					}
				}

				if sent_ok {
					new_replica.last_send_ts = int(time.Now().Unix())
					break
				}

				new_replica.mu.Lock()
				new_replica.stat_send_retried++
				new_replica.stat_retries_since_last_resync++
				new_replica._connect(false) // if something went wrong - disconnect so we can reconnect later
				new_replica.mu.Unlock()

				time.Sleep(time.Millisecond * 500)
			}

			_, _s_ks, _s_kt, _s_kd, _s_f, _s_uncomp, _s_comp := _tosend.GetStats()

			new_replica.mu.Lock()
			if sent_ok {
				new_replica.stat_key_set += _s_ks
				new_replica.stat_key_touch += _s_kt
				new_replica.stat_key_delete += _s_kd
				new_replica.stat_flush += _s_f
				new_replica.stat_sends++
				new_replica.stat_bytes_sent += _s_comp
				new_replica.stat_bytes_pending -= _s_comp
				new_replica.stat_bytes_sent_uncompressed += int64(_s_uncomp)

				new_replica.stat_sends_since_last_recync++

			} else {
				new_replica.stat_failed_key_set += _s_ks
				new_replica.stat_failed_key_touch += _s_kt
				new_replica.stat_failed_key_delete += _s_kd
				new_replica.stat_failed_flush += _s_f
				new_replica.stat_send_fail++
				new_replica.stat_bytes_failed += _s_comp
				new_replica.last_send_failed_timestamp = time.Now().Unix()

				new_replica.stat_errors_since_last_resync++
			}

			new_replica.sync_task_sending = nil
			new_replica.mu.Unlock()

		}

	}(new_replica)

	return ret
}

// remove replica from list, and stop the processing thread by
// setting sync_tasks to null
func RemoveReplica(host string, port int) int {

	// delete that replica from list
	mutex_replica.Lock()
	tmp := make([]*replica, 0, len(replicas))
	for _, v := range replicas {
		if host == v.host && port == v.port {
			fmt.Println("[!] Replica removed... ", host, " port: ", port)

			v.mu.Lock()
			v.sync_tasks = nil
			v.mu.Unlock()
			continue
		}
		tmp = append(tmp, v)
	}
	replicas = tmp
	mutex_replica.Unlock()

	ret := len(replicas)
	return ret
}

// add sync task to all replicas
func AddSyncTask(st *sync_task.Sync_task) {

	mutex_replica.Lock()
	for _, v := range replicas {

		_, _s_ks, _s_kt, _s_kd, _s_f, _, _s_comp := st.GetStats()
		v.mu.Lock()
		if v.sync_tasks != nil {
			if len(v.sync_tasks) < 120 {
				v.sync_tasks = append(v.sync_tasks, st)
				v.stat_bytes_pending += _s_comp
			} else {
				v.stat_skipped_key_set += _s_ks
				v.stat_skipped_key_touch += _s_kt
				v.stat_skipped_key_delete += _s_kd
				v.stat_skipped_flush += _s_f
				v.stat_skips++
				v.stat_bytes_skipped += _s_comp
				v.last_send_skipped_timestamp = time.Now().Unix()

				v.stat_skips_since_last_resync++
			}
		}
		v.mu.Unlock()

	}
	mutex_replica.Unlock()
}

// returns true if hash map needs to be synchronized with newly added replica
func HashMapShouldSynchronize(kvstore_num int) bool {

	should_sync := false

	mutex_replica.Lock()
	for _, v := range replicas {
		if v.hashmap_synchronization_progress[kvstore_num] < 2 {
			should_sync = true
			v.hashmap_synchronization_progress[kvstore_num]++
		}
	}
	mutex_replica.Unlock()

	return should_sync
}

func HashMapForceResync(host string, port int) int {

	resynced := 0
	mutex_replica.Lock()
	for _, v := range replicas {
		if host != "" && v.host != host {
			continue
		}

		if port != 0 && v.port != port {
			continue
		}

		v.stat_sends_since_last_recync = 0
		v.stat_retries_since_last_resync = 0
		v.stat_errors_since_last_resync = 0
		v.stat_skips_since_last_resync = 0

		v.hashmap_synchronization_progress = make(map[int]int, 0)
	}
	mutex_replica.Unlock()

	return resynced
}
