package replicator

import (
	"fmt"
	"sync"
	"time"
)

type replica struct {
	host string
	port int

	mu                sync.Mutex
	sync_tasks        []*sync_task
	sync_task_sending *sync_task

	stat_key_set            int
	stat_key_touch          int
	stat_key_delete         int
	stat_flush              int
	stat_skipped_key_set    int
	stat_skipped_key_touch  int
	stat_skipped_key_delete int
	stat_skipped_flush      int

	stat_sends         int
	stat_skips         int
	stat_bytes_sent    int
	stat_bytes_pending int
	stat_bytes_skipped int

	stat_bytes_sent_uncompressed int64
	last_send_failed_timestamp   int64
}

var replicas []*replica = make([]*replica, 0)

func AddReplica(host string, port int) int {

	fmt.Println("Replica added... ", host, " port: ", port)

	mutex_replica.Lock()
	new_replica := &replica{host: host, port: port, sync_tasks: make([]*sync_task, 0)}
	replicas = append(replicas, new_replica)
	ret := len(replicas)
	mutex_replica.Unlock()

	go func(new_replica *replica) {

		for {
			time.Sleep(20 * time.Millisecond)

			new_replica.mu.Lock()
			if new_replica.sync_tasks == nil {
				new_replica.mu.Unlock()
				break
			}
			if len(new_replica.sync_tasks) <= 0 {
				new_replica.mu.Unlock()
				continue
			}

			// send first task, and remove it
			_tmp := make([]*sync_task, 0, len(new_replica.sync_tasks)-1)
			var _tosend *sync_task = nil
			for k, v := range new_replica.sync_tasks {
				if k == 0 {
					_tosend = v
				} else {
					_tmp = append(_tmp, v)
				}
			}
			new_replica.sync_tasks = _tmp
			new_replica.mu.Unlock()

			mutex_replica.Lock()
			new_replica.sync_task_sending = _tosend
			mutex_replica.Unlock()

			for _, data_packet := range _tosend.data_packets {

				if new_replica.port != -1 {
					sendData(new_replica.host, new_replica.port, data_packet)
				}

			}

			mutex_replica.Lock()
			new_replica.stat_key_set += _tosend.stat_key_set
			new_replica.stat_key_touch += _tosend.stat_key_touch
			new_replica.stat_key_delete += _tosend.stat_key_delete
			new_replica.stat_flush += _tosend.stat_flush
			new_replica.stat_sends++
			new_replica.stat_bytes_sent += _tosend.stat_compressed
			new_replica.stat_bytes_pending -= _tosend.stat_compressed
			new_replica.stat_bytes_sent_uncompressed += int64(_tosend.stat_uncompressed)

			new_replica.sync_task_sending = nil
			mutex_replica.Unlock()
		}

	}(new_replica)

	return ret
}

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

// this should be only called when we have lock on mutex_replica!!
func DispatchSyncTask(st *sync_task) {

	//mutex_replica.Lock()
	for _, v := range replicas {

		v.mu.Lock()
		if v.sync_tasks != nil {
			if len(v.sync_tasks) < 50 {
				v.sync_tasks = append(v.sync_tasks, st)
				v.stat_bytes_pending += st.stat_compressed
			} else {
				v.stat_skipped_key_set += st.stat_key_set
				v.stat_skipped_key_touch += st.stat_key_touch
				v.stat_skipped_key_delete += st.stat_key_delete
				v.stat_skipped_flush += st.stat_flush
				v.stat_skips++
				v.stat_bytes_skipped += st.stat_compressed
				v.last_send_failed_timestamp = time.Now().Unix()
			}
		}
		v.mu.Unlock()

	}
	//mutex_replica.Unlock()
}
